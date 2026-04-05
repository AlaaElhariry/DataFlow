# DataFlow Pro — Performance Report & Big-O Analysis
**NileMart ETL Engine | BI Engineering Team**

---

## Phase 1 — Sorting & Searching

### Why did Quick Sort beat Bubble Sort?

| Algorithm      | Time Complexity | 10,000 rows (approx. operations) |
|---------------|----------------|----------------------------------|
| Bubble Sort    | O(n²)          | **100,000,000** comparisons      |
| Insertion Sort | O(n²)          | **100,000,000** comparisons      |
| Selection Sort | O(n²)          | **100,000,000** comparisons      |
| Merge Sort     | O(n log n)     | **133,000** comparisons          |
| Quick Sort     | O(n log n)     | **~100,000** comparisons (avg)   |
| Timsort        | O(n log n)     | Fastest — detects sorted runs    |

Bubble Sort performs n×(n-1)/2 comparisons regardless of data. On 10,000 rows
this is ~50 million swaps. Quick Sort partitions the array around a pivot,
recursively sorting smaller sub-arrays — roughly n × log₂(n) operations.

**BI Impact**: A Power BI dataset with 10M rows sorted via Bubble Sort would take
hours. Timsort (used by pandas and Python's built-in sort) does it in seconds
because it detects already-sorted "runs" in real-world data — which is common
in time-series fact tables (dates are often partially sorted already).

### Why Binary Search vs Linear Search?

| n rows       | Linear Search (unsorted) | Binary Search (sorted) |
|-------------|--------------------------|------------------------|
| 10,000       | Up to 10,000 checks      | Up to 14 checks        |
| 1,000,000    | Up to 1,000,000 checks   | Up to 20 checks        |
| 10,000,000   | Up to 10,000,000 checks  | Up to 23 checks        |

This is why SQL Server's clustered index (a B+ Tree, which extends BST ideas)
makes a LOOKUPVALUE() call near-instant even on 100M-row fact tables.

---

## Phase 2 — Linked List vs Array for Applied Steps

| Operation        | Array (list)  | Singly Linked List | Doubly Linked List |
|-----------------|--------------|--------------------|--------------------|
| Append new step  | O(1) amort.  | O(1) with tail ptr | O(1) with tail ptr |
| Undo (remove end)| O(1)         | O(n) — no prev ptr | **O(1)** ✔         |
| Redo (move fwd)  | O(1)         | O(n)               | **O(1)** ✔         |

The Doubly Linked List is essential for Undo/Redo because each node stores a
`prev` pointer. Without it, to "go back" you'd need to re-traverse from the
head — O(n) — which becomes unusable when an analyst has 50+ transformation steps.

---

## Phase 3 — Stack for DAX Parsing

The Stack is the only data structure that naturally enforces operator precedence
because of its LIFO (Last In, First Out) property:

- When we push operands and see an operator, we POP the most recently pushed
  operands — which are exactly the ones that belong to that operator.
- Parentheses create implicit "sub-stacks" via the Shunting Yard algorithm.

**Why not a Queue (FIFO)?**
A queue would give us the *first* operand pushed — wrong for math expressions.
Multiplication must act on its two immediate neighbors, not the first numbers seen.

---

## Phase 4 — Why deque instead of list for the Buffer?

| Operation      | list (pop(0)) | collections.deque (popleft) |
|---------------|---------------|-----------------------------|
| Enqueue (back) | O(1)          | O(1)                        |
| Dequeue (front)| **O(n)** ✘    | **O(1)** ✔                  |

`list.pop(0)` removes the first element and shifts ALL remaining elements
one position to the left in memory. At 50,000 transactions/minute during
White Friday, this is 50,000 memory-shift operations per dequeue — the buffer
would lag behind the POS stream and eventually crash or drop data.

`collections.deque` is a doubly-linked list under the hood (implemented in C).
Removing from the front just updates a pointer — no shifting, O(1) always.

**Real-world parallel**: Apache Kafka's partitions are essentially persistent,
distributed deques. The same O(1) guarantee at massive scale.

---

## Phase 5 — Trees for Hierarchical Analytics

### BST vs Flat Array for Dimension Lookup

| n customers   | Linear scan (unsorted list) | BST search   |
|--------------|-----------------------------|--------------|
| 1,000         | 500 avg comparisons         | 10 max       |
| 1,000,000     | 500,000 avg                 | 20 max       |
| 10,000,000    | 5,000,000 avg               | 23 max       |

### Why Recursive Roll-Up for the Org Chart?

The tree has an inherently recursive structure: every subtree is itself a tree.
A recursive function elegantly expresses "my total = my sales + sum of my children's totals"
without needing to know the tree's depth or width in advance.

**BI Equivalent**:
```dax
VP Cairo Total Sales =
CALCULATE(
    SUM(Sales[Amount]),
    DESCENDANTS('Employee'[VP_Cairo], 'Employee', INCLUDE_ALL_LEAVES)
)
```
This DAX function does the same recursive traversal internally.

---

## Summary Table

| Phase | Structure        | Core Operation    | Complexity |
|-------|-----------------|-------------------|------------|
| 1     | Sorted Array     | Binary Search     | O(log n)   |
| 1     | -               | Merge/Quick Sort  | O(n log n) |
| 2     | Doubly LL        | Undo / Redo       | O(1)       |
| 3     | Stack (deque)    | Postfix Eval      | O(n)       |
| 4     | deque            | Enqueue/Dequeue   | O(1)       |
| 5     | BST              | Insert / Search   | O(log n)   |
| 5     | N-ary Tree       | Roll-Up Traversal | O(n)       |
