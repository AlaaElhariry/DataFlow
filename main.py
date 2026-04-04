"""
DataFlow Pro – NileMart ETL Engine
====================================
Main application loop. Run this file to launch the CLI.

Usage:
  cd dataflow_pro/src
  python main.py

Author: DataFlow Pro – NileMart ETL Engine
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from phase1_indexer   import generate_transactions, benchmark_all_sorts, benchmark_search, timsort_builtin, bisect_date_range
from phase2_tracker   import AppliedStepsTracker, demo_pipeline
from phase3_parser    import DAXEvaluator, SAMPLE_FORMULAS, validate_parentheses
from phase4_buffer    import LiveIngestionQueue, benchmark_queues
from phase5_trees     import DimensionIndex, OrgChartAnalyzer


BANNER = """
╔══════════════════════════════════════════════════════════╗
║      DataFlow Pro  –  NileMart ETL & Analytics Engine   ║
║              Smart Village HQ  |  Power BI Team         ║
╚══════════════════════════════════════════════════════════╝
"""

MENU = """
  ┌─ Main Menu ──────────────────────────────────────────┐
  │  1.  Phase 1 – Sort & Search Benchmark               │
  │  2.  Phase 2 – Applied Steps Tracker (Linked List)   │
  │  3.  Phase 3 – DAX Formula Evaluator (Stack)         │
  │  4.  Phase 4 – Live Data Buffer (Queue)              │
  │  5.  Phase 5 – Org Chart & BST (Trees)               │
  │  6.  Run ALL Phases (Full Demo)                      │
  │  0.  Exit                                            │
  └──────────────────────────────────────────────────────┘
"""


def run_phase1():
    print("\n" + "━"*60)
    print("  PHASE 1 – QUERY OPTIMIZER (Sorting & Searching)")
    print("━"*60)
    data = generate_transactions(5_000)
    print(f"  Generated {len(data):,} raw transaction records.")
    benchmark_all_sorts(data, key="txn_id")

    # Sort with Timsort for search phase
    sorted_data = timsort_builtin(data, "txn_id")

    # Pick a real txn_id to search for
    target = sorted_data[len(sorted_data)//2]["txn_id"]
    print(f"\n  Searching for TXN ID: {target}")
    benchmark_search(sorted_data, target)

    # Bisect date slice
    q3 = bisect_date_range(sorted_data, 20240701, 20240930)
    print(f"\n  Bisect Q3 2024 slice: {len(q3):,} transactions extracted")


def run_phase2():
    print("\n" + "━"*60)
    print("  PHASE 2 – APPLIED STEPS TRACKER (Linked List)")
    print("━"*60)
    tracker = demo_pipeline()
    tracker.display()

    print("  → Undoing last 2 steps...")
    tracker.undo_step()
    tracker.undo_step()
    tracker.display()

    print("  → Adding a new step (replaces undone steps)...")
    tracker.add_step("Renamed Columns", "= Table.RenameColumns(#\"Removed Nulls\", {{\"amt_egp\", \"Revenue EGP\"}})")
    tracker.display()

    print("  → Trying to Undo to empty state...")
    for _ in range(10):
        tracker.undo_step()


def run_phase3():
    print("\n" + "━"*60)
    print("  PHASE 3 – DAX FORMULA PARSER (Stack)")
    print("━"*60)
    engine = DAXEvaluator()

    # Parenthesis validation
    test_exprs = [
        "(Revenue - Cost) * Tax_Rate",
        "((Revenue + Bonus) * 0.05",
        "SUM(Sales[Amount])",
    ]
    print("\n  Parenthesis Validator:")
    for expr in test_exprs:
        valid = validate_parentheses(expr)
        icon = "✔" if valid else "✘"
        print(f"  {icon} '{expr}'")

    # Evaluate preset formulas
    print("\n  DAX Expression Evaluator (Postfix):")
    for f in SAMPLE_FORMULAS:
        print(f"\n  ── {f['name']} ({f['note']}) ──")
        result = engine.evaluate_postfix(f["postfix"])
        print(f"  ✔ Result: {result:,.2f}")

    # Infix conversion demo
    print("\n  ── Full Infix → Postfix → Evaluate pipeline ──")
    result = engine.evaluate_infix("(85000 - 62000) / 85000")
    print(f"  ✔ Net Margin: {result*100:.1f}%")


def run_phase4():
    print("\n" + "━"*60)
    print("  PHASE 4 – LIVE DATA BUFFER (Queue)")
    print("━"*60)

    # Demo the production queue
    buffer = LiveIngestionQueue(max_size=100)
    print("\n  Simulating White Friday POS stream (8 transactions):")
    transactions = [
        {"txn": 1045, "branch": "Maadi",     "amt_egp": 850},
        {"txn": 1046, "branch": "Smouha",    "amt_egp": 3200},
        {"txn": 1047, "branch": "Zayed",     "amt_egp": 1750},
        {"txn": 1048, "branch": "Mansoura",  "amt_egp": 420},
        {"txn": 1049, "branch": "Heliopolis","amt_egp": 6800},
        {"txn": 1050, "branch": "Nasr City", "amt_egp": 990},
        {"txn": 1051, "branch": "Maadi",     "amt_egp": 250},
        {"txn": 1052, "branch": "Smouha",    "amt_egp": 4100},
    ]
    for t in transactions:
        buffer.enqueue_row(t)

    print(f"\n  Buffer Stats: {buffer.stats()}")
    print(f"\n  Processing batch of 5 rows...")
    buffer.process_batch(5)
    print(f"\n  Buffer Stats after batch: {buffer.stats()}")

    # Performance benchmark
    benchmark_queues(n=5_000)


def run_phase5():
    print("\n" + "━"*60)
    print("  PHASE 5 – TREES (BST + Org Chart)")
    print("━"*60)

    # BST Dimension Index
    print("\n  ── Binary Search Tree: Customer Dimension Index ──")
    idx = DimensionIndex()
    customers = [
        (29012345678901, "Ahmed Hassan"),
        (29512345678902, "Mona Ibrahim"),
        (30012345678903, "Khaled Mahmoud"),
        (28512345678904, "Sara Youssef"),
        (30512345678905, "Omar Fathy"),
        (29812345678906, "Nadia Samir"),
    ]
    for nid, name in customers:
        idx.insert(nid, name)

    idx.display()
    print("  Searching:")
    print(f"  {idx.search(29512345678902)}")
    print(f"  {idx.search(11111111111111)}")

    # Org Chart
    print("\n  ── N-ary Tree: Organizational Chart ──")
    org = OrgChartAnalyzer()
    org.display_chart()
    org.print_roll_up_report()

    # Specific VP drill-down
    cairo_total = org.roll_up_sales(org.vp_cairo)
    alex_total  = org.roll_up_sales(org.vp_alex)
    print(f"  VP Cairo & Giza total:  {cairo_total:>12,.0f} EGP")
    print(f"  VP Alex & Delta total:  {alex_total:>12,.0f} EGP")


def main():
    print(BANNER)

    while True:
        print(MENU)
        choice = input("  Select option: ").strip()

        if choice == "1":
            run_phase1()
        elif choice == "2":
            run_phase2()
        elif choice == "3":
            run_phase3()
        elif choice == "4":
            run_phase4()
        elif choice == "5":
            run_phase5()
        elif choice == "6":
            run_phase1()
            run_phase2()
            run_phase3()
            run_phase4()
            run_phase5()
            print("\n  ✔ Full DataFlow Pro demo complete.")
        elif choice == "0":
            print("\n  Shutting down DataFlow Pro. Masalama! 🌙\n")
            sys.exit(0)
        else:
            print("  ⚠ Invalid option — please enter 0-6.")


if __name__ == "__main__":
    main()
