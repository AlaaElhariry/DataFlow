"""
Phase 4: The Live Data Buffer – Queues
========================================
BI Context: During White Friday (NileMart's peak sales event), thousands of
Point-of-Sale (POS) transactions fire every second. The database cannot write
them fast enough. Without a buffer, transactions are LOST.

A Queue (First In, First Out) solves this by acting as a waiting room:
  - POS terminals ENQUEUE rows at the back.
  - The database worker DEQUEUES from the front in batches.
  - No transaction is lost; they just wait their turn.

This is exactly how Apache Kafka, Azure Event Hubs, and AWS Kinesis work —
they are distributed, scalable queues. We're building the core concept here.

Performance Evolution:
  1. List Queue   → O(n) dequeue  ← TERRIBLE for high-throughput
  2. Linked List  → O(1) dequeue  ← Correct, but manual memory management
  3. deque        → O(1) both     ← Production standard, C-optimized

Author: DataFlow Pro – NileMart ETL Engine
"""

from collections import deque
import time


# ─────────────────────────────────────────────
# VERSION 1 – NAIVE LIST QUEUE  (O(n) dequeue – DON'T USE)
# ─────────────────────────────────────────────

class ListQueue:
    """
    Queue backed by a Python list using index 0 as the front.

    CRITICAL FLAW: list.pop(0) is O(n) because Python must shift every
    remaining element one position to the left after removal.

    BI Impact: During White Friday with 50,000 transactions/minute, this
    means 50,000 element-shifts PER dequeue. The buffer will lag, crash,
    or cause data loss. This version exists to demonstrate why it's wrong.
    """

    def __init__(self):
        self._data: list = []

    def enqueue(self, item: dict) -> None:
        """Add to back — O(1)."""
        self._data.append(item)

    def dequeue(self) -> dict | None:
        """
        Remove from front — O(n) ← THE PROBLEM.
        Python shifts all remaining elements left by one index.
        At 10,000 items, this is 10,000 memory moves per dequeue.
        """
        if not self._data:
            return None
        return self._data.pop(0)   # ← O(n) culprit

    def peek(self) -> dict | None:
        return self._data[0] if self._data else None

    def size(self) -> int:
        return len(self._data)

    def is_empty(self) -> bool:
        return len(self._data) == 0


# ─────────────────────────────────────────────
# VERSION 2 – LINKED LIST QUEUE  (O(1) both ends)
# ─────────────────────────────────────────────

class _QueueNode:
    def __init__(self, data: dict):
        self.data = data
        self.next: "_QueueNode | None" = None


class LinkedListQueue:
    """
    Queue backed by a Singly Linked List with head and tail pointers.

    - enqueue() → O(1): append new node at tail, update tail pointer.
    - dequeue() → O(1): remove head node, update head pointer.

    No shifting, no copying — just pointer updates.

    BI Impact: Handles any transaction volume in constant time per operation.
    This is the theoretical foundation of Kafka's message log segments.
    """

    def __init__(self):
        self._head: _QueueNode | None = None
        self._tail: _QueueNode | None = None
        self._size = 0

    def enqueue(self, item: dict) -> None:
        """Add to back — O(1)."""
        node = _QueueNode(item)
        if self._tail is None:
            self._head = self._tail = node
        else:
            self._tail.next = node
            self._tail = node
        self._size += 1

    def dequeue(self) -> dict | None:
        """Remove from front — O(1)."""
        if self._head is None:
            return None
        data = self._head.data
        self._head = self._head.next
        if self._head is None:
            self._tail = None     # Queue is now empty
        self._size -= 1
        return data

    def peek(self) -> dict | None:
        return self._head.data if self._head else None

    def size(self) -> int:
        return self._size

    def is_empty(self) -> bool:
        return self._size == 0


# ─────────────────────────────────────────────
# VERSION 3 – DEQUE QUEUE  (Production Standard)
# ─────────────────────────────────────────────

class LiveIngestionQueue:
    """
    Production-grade Queue using collections.deque.

    collections.deque is implemented in C as a doubly-ended linked list.
    - appendright() → O(1)   (enqueue at back)
    - popleft()     → O(1)   (dequeue from front)
    - Thread-safe for single producer/consumer scenarios.

    BI Context: Azure Data Factory's self-hosted integration runtime uses
    a similar in-memory buffer when pulling data from on-premise SQL Server
    before pushing to Azure Synapse. The deque is the right abstraction.

    Extra Features added for production realism:
      - max_size: drops oldest data if buffer is full (backpressure handling)
      - process_batch(): pulls N rows at once for bulk INSERT efficiency
      - stats(): monitoring hook (mimics Azure Monitor metrics)
    """

    def __init__(self, max_size: int = 10_000):
        self.buffer = deque(maxlen=max_size)   # Automatically drops oldest on overflow
        self.max_size = max_size
        self._total_enqueued = 0
        self._total_processed = 0

    def enqueue_row(self, row_data: dict) -> None:
        """
        Adds a new transaction row to the back of the buffer — O(1).
        If max_size is reached, the oldest entry is automatically dropped
        (deque with maxlen). In production, you'd log that drop as an alert.
        """
        if len(self.buffer) == self.max_size:
            print(f"  ⚠ Buffer FULL ({self.max_size} rows) — oldest row dropped (backpressure)")
        self.buffer.append(row_data)
        self._total_enqueued += 1
        print(f"  ▶ Enqueued TXN #{row_data.get('txn', '?')} from {row_data.get('branch', '?')} — {row_data.get('amt_egp', 0):,.0f} EGP")

    def process_batch(self, batch_size: int) -> list:
        """
        Dequeues up to batch_size rows from the front — O(batch_size).

        BI Context: Instead of writing one row per INSERT (very slow),
        we collect a batch and do a single bulk INSERT to SQL Server.
        This is the ETL pattern behind SSIS's "OLE DB Destination" with
        fast-load mode enabled. Typical batch sizes: 500–5000 rows.
        """
        processed = []
        for _ in range(batch_size):
            if not self.buffer:
                break
            processed.append(self.buffer.popleft())   # O(1) per pop

        self._total_processed += len(processed)
        print(f"\n  ✔ Batch of {len(processed)} rows processed → Pushing to Power BI Dataset")
        print(f"  📊 Buffer status: {len(self.buffer)} rows waiting | Total processed: {self._total_processed}")
        return processed

    def peek_front(self) -> dict | None:
        """Returns front item without dequeuing — for monitoring."""
        return self.buffer[0] if self.buffer else None

    def is_empty(self) -> bool:
        return len(self.buffer) == 0

    def size(self) -> int:
        return len(self.buffer)

    def stats(self) -> dict:
        """Returns queue health metrics — like an Azure Monitor dashboard widget."""
        return {
            "queued":      len(self.buffer),
            "max_size":    self.max_size,
            "utilization": f"{len(self.buffer)/self.max_size*100:.1f}%",
            "total_in":    self._total_enqueued,
            "total_out":   self._total_processed,
        }


# ─────────────────────────────────────────────
# BENCHMARK: LIST vs DEQUE
# ─────────────────────────────────────────────

def benchmark_queues(n: int = 5_000) -> None:
    """
    Proves the O(n) vs O(1) dequeue difference with real timing.
    """
    print(f"\n{'═'*55}")
    print(f"  PHASE 4 — QUEUE BENCHMARK  (n = {n:,} rows)")
    print(f"{'═'*55}")

    sample_row = {"txn": 9999, "branch": "Maadi", "amt_egp": 850.00}

    # List Queue
    lq = ListQueue()
    for _ in range(n):
        lq.enqueue(sample_row)
    start = time.perf_counter()
    while not lq.is_empty():
        lq.dequeue()
    list_time = time.perf_counter() - start
    print(f"  ListQueue  dequeue {n:,}×   {list_time:.4f}s  [O(n) per op]")

    # Deque
    dq = deque()
    for _ in range(n):
        dq.append(sample_row)
    start = time.perf_counter()
    while dq:
        dq.popleft()
    deque_time = time.perf_counter() - start
    print(f"  deque      dequeue {n:,}×   {deque_time:.4f}s  [O(1) per op]")

    if deque_time > 0:
        print(f"\n  ✔ deque is {list_time/deque_time:.1f}× faster than list-based queue")
    print(f"{'═'*55}")
