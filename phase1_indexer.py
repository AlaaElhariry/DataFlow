"""
Phase 1: The Query Optimizer – Sorting & Searching
====================================================
BI Context: Power BI's DirectQuery and Import modes both suffer when the
underlying data has no order. When analysts filter by date or transaction ID,
the engine must scan every row (full table scan). Sorting + indexing is how
databases like SQL Server solve this — we're replicating that logic in Python.

Author: DataFlow Pro – NileMart ETL Engine
"""

import random
import time
import bisect


# ─────────────────────────────────────────────
# DATA GENERATOR
# ─────────────────────────────────────────────

def generate_transactions(n: int = 10_000) -> list[dict]:
    """
    Simulates NileMart's raw, unordered transaction fact table.
    In Power BI terms, this is your messy source before any Power Query steps.
    """
    branches = ["Maadi", "Zayed", "Smouha", "Mansoura", "Heliopolis", "Nasr City"]
    records = []
    for _ in range(n):
        records.append({
            "txn_id":   random.randint(100_000, 999_999),
            "branch":   random.choice(branches),
            "amt_egp":  round(random.uniform(50, 15_000), 2),
            "date_key": random.randint(20240101, 20241231),  # YYYYMMDD int for easy sorting
        })
    return records


# ─────────────────────────────────────────────
# PART 1 – SLOW SORTS  (Why O(n²) kills BI pipelines)
# ─────────────────────────────────────────────

def bubble_sort(data: list[dict], key: str) -> list[dict]:
    """
    Bubble Sort – O(n²) time, O(1) space.

    BI Impact: On 10 000 rows this is ~100 million comparisons.
    Power BI's engine would time-out waiting for sorted data.
    Educational value only — never use in production ETL.
    """
    arr = data.copy()
    n = len(arr)
    for i in range(n):
        for j in range(0, n - i - 1):
            if arr[j][key] > arr[j + 1][key]:
                arr[j], arr[j + 1] = arr[j + 1], arr[j]
    return arr


def insertion_sort(data: list[dict], key: str) -> list[dict]:
    """
    Insertion Sort – O(n²) worst, O(n) best (nearly-sorted data).

    BI Impact: If analysts add new daily transactions to an already-sorted
    dataset, Insertion Sort is actually fast. Power Query does something
    similar when it appends new rows incrementally.
    """
    arr = data.copy()
    for i in range(1, len(arr)):
        current = arr[i]
        j = i - 1
        while j >= 0 and arr[j][key] > current[key]:
            arr[j + 1] = arr[j]
            j -= 1
        arr[j + 1] = current
    return arr


def selection_sort(data: list[dict], key: str) -> list[dict]:
    """
    Selection Sort – O(n²) always, O(1) space.

    BI Impact: Worst of the three for any data pattern. No adaptive behavior.
    The only advantage is it does at most n-1 swaps, useful in write-heavy
    storage scenarios (not relevant for in-memory ETL).
    """
    arr = data.copy()
    n = len(arr)
    for i in range(n):
        min_idx = i
        for j in range(i + 1, n):
            if arr[j][key] < arr[min_idx][key]:
                min_idx = j
        arr[i], arr[min_idx] = arr[min_idx], arr[i]
    return arr


# ─────────────────────────────────────────────
# PART 2 – FAST SORTS  (Production ETL grade)
# ─────────────────────────────────────────────

def merge_sort(data: list[dict], key: str) -> list[dict]:
    """
    Merge Sort – O(n log n) guaranteed, O(n) space.

    BI Impact: This is the algorithm behind ORDER BY in SQL.
    Stable sort — equal keys keep their original order, critical for
    time-series fact tables where two transactions on the same date_key
    must stay deterministic.

    How it works: Divide the array in half recursively until single elements,
    then merge pairs back together in sorted order.
    """
    if len(data) <= 1:
        return data

    mid = len(data) // 2
    left  = merge_sort(data[:mid], key)
    right = merge_sort(data[mid:], key)

    # Merge phase – O(n) per level, log(n) levels = O(n log n) total
    merged, i, j = [], 0, 0
    while i < len(left) and j < len(right):
        if left[i][key] <= right[j][key]:
            merged.append(left[i]); i += 1
        else:
            merged.append(right[j]); j += 1

    merged.extend(left[i:])
    merged.extend(right[j:])
    return merged


def quick_sort(data: list[dict], key: str) -> list[dict]:
    """
    Quick Sort – O(n log n) average, O(n²) worst, O(log n) space.

    BI Impact: Fastest in practice for random data — the average case is
    ~2-3× faster than Merge Sort due to better cache locality.
    Python's Timsort uses a hybrid approach inspired by both.

    Pivot choice: We use the median-of-three strategy to avoid worst-case
    O(n²) on already-sorted data (a real risk in ETL incremental loads).
    """
    if len(data) <= 1:
        return data

    # Median-of-three pivot — avoids sorted-data worst case
    mid = len(data) // 2
    candidates = [data[0][key], data[mid][key], data[-1][key]]
    candidates.sort()
    pivot_val = candidates[1]

    left   = [x for x in data if x[key] <  pivot_val]
    middle = [x for x in data if x[key] == pivot_val]
    right  = [x for x in data if x[key] >  pivot_val]

    return quick_sort(left, key) + middle + quick_sort(right, key)


# ─────────────────────────────────────────────
# PART 3 – TIMSORT BENCHMARK
# ─────────────────────────────────────────────

def timsort_builtin(data: list[dict], key: str) -> list[dict]:
    """
    Python's built-in sort — Timsort, O(n log n) guaranteed.

    BI Impact: This is what pandas, Power Query, and SQL Server all use
    internally. Timsort detects already-sorted "runs" in real data and
    merges them — making it faster than any manual implementation on
    realistic (partially-ordered) datasets.
    """
    return sorted(data, key=lambda x: x[key])


def benchmark_all_sorts(data: list[dict], key: str = "txn_id") -> dict:
    """
    Runs and times all sorting algorithms.
    Returns a dict of {algorithm_name: elapsed_seconds}.
    """
    algorithms = {
        "Bubble Sort   O(n²)":     bubble_sort,
        "Insertion Sort O(n²)":    insertion_sort,
        "Selection Sort O(n²)":    selection_sort,
        "Merge Sort    O(n log n)": merge_sort,
        "Quick Sort    O(n log n)": quick_sort,
        "Timsort       O(n log n)": timsort_builtin,
    }
    results = {}
    print("\n" + "═"*55)
    print("  PHASE 1 — SORT BENCHMARK  (n = {:,})".format(len(data)))
    print("═"*55)

    for name, fn in algorithms.items():
        # Skip O(n²) for large n to avoid 10-minute wait
        if len(data) > 5_000 and "O(n²)" in name:
            print(f"  {name:<30} ⏭  SKIPPED (too slow at scale)")
            results[name] = float("inf")
            continue

        start = time.perf_counter()
        fn(data, key)
        elapsed = time.perf_counter() - start
        results[name] = elapsed
        print(f"  {name:<30} {elapsed:.4f}s")

    print("═"*55)
    return results


# ─────────────────────────────────────────────
# PART 4 – SEARCH ENGINE
# ─────────────────────────────────────────────

def linear_search(data: list[dict], target_id: int) -> dict | None:
    """
    Linear Search – O(n).

    BI Impact: This is what Power BI does on an unindexed column.
    Every LOOKUPVALUE or RELATED call without a proper relationship
    degrades to this. On 10M rows, it reads every row.
    """
    for row in data:
        if row["txn_id"] == target_id:
            return row
    return None


def binary_search(sorted_data: list[dict], target_id: int) -> dict | None:
    """
    Binary Search – O(log n).  Requires sorted data.

    BI Impact: This is how SQL Server's clustered index works.
    For 10 000 rows, binary search needs at most log₂(10000) ≈ 14 comparisons
    vs linear search's 10 000. At 10M rows: 23 comparisons vs 10 million.

    Prerequisite: data must be sorted by txn_id (run merge_sort or timsort first).
    """
    lo, hi = 0, len(sorted_data) - 1
    while lo <= hi:
        mid = (lo + hi) // 2
        if sorted_data[mid]["txn_id"] == target_id:
            return sorted_data[mid]
        elif sorted_data[mid]["txn_id"] < target_id:
            lo = mid + 1
        else:
            hi = mid - 1
    return None


def bisect_date_range(sorted_data: list[dict], start_date: int, end_date: int) -> list[dict]:
    """
    Bisect (binary search on sorted list) – O(log n) to find boundaries,
    then O(k) to slice k matching records.

    BI Impact: This is exactly how Power BI slices a date table for a slicer.
    Instead of filtering every row, it jumps directly to the start and end
    positions of the date range — like a SQL BETWEEN on an indexed column.

    Example: Extract Q3 2024 = date_key BETWEEN 20240701 AND 20240930
    """
    # Build a flat list of date keys for bisect (O(n) — done once, like building an index)
    keys = [row["date_key"] for row in sorted_data]

    lo = bisect.bisect_left(keys, start_date)   # First position ≥ start_date
    hi = bisect.bisect_right(keys, end_date)    # First position > end_date

    return sorted_data[lo:hi]


def benchmark_search(sorted_data: list[dict], target_id: int) -> None:
    """Compares linear vs binary search speed."""
    print("\n" + "═"*55)
    print("  PHASE 1 — SEARCH BENCHMARK")
    print("═"*55)

    # Linear search on unsorted copy
    unsorted = sorted_data.copy()
    random.shuffle(unsorted)

    start = time.perf_counter()
    result = linear_search(unsorted, target_id)
    lin_time = time.perf_counter() - start
    print(f"  Linear Search (unsorted)   {lin_time:.6f}s  → {'Found' if result else 'Not Found'}")

    start = time.perf_counter()
    result = binary_search(sorted_data, target_id)
    bin_time = time.perf_counter() - start
    print(f"  Binary Search (sorted)     {bin_time:.6f}s  → {'Found' if result else 'Not Found'}")

    # Q3 slice demo
    q3 = bisect_date_range(sorted_data, 20240701, 20240930)
    print(f"\n  Bisect Q3 2024 slice       {len(q3):,} transactions extracted in O(log n)")
    print("═"*55)
