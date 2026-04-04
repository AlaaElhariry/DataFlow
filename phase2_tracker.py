"""
Phase 2: The "Applied Steps" Tracker – Linked Lists
=====================================================
BI Context: Every Power Query transformation you apply (Remove Nulls,
Change Type, Rename Column...) is stored as an "Applied Step" — an ordered
list of operations that replay from top to bottom when data refreshes.

A Linked List is the perfect data structure for this because:
  - Inserting a new step at the end is O(1) — just update the last pointer.
  - Deleting the last step (Undo) is O(1) with a Doubly Linked List.
  - Unlike an array, we never need to shift 100 steps because we added one.

Author: DataFlow Pro – NileMart ETL Engine
"""


# ─────────────────────────────────────────────
# NODE DEFINITION
# ─────────────────────────────────────────────

class StepNode:
    """
    Represents a single Power Query transformation step.

    Each node holds:
      - step_name: human-readable label (e.g., "Removed Nulls")
      - m_code:    the actual M language expression Power Query would generate
      - next:      pointer to the next step (forward direction)
      - prev:      pointer to the previous step (backward direction, for Undo)
    """
    def __init__(self, step_name: str, m_code: str = ""):
        self.step_name = step_name
        self.m_code    = m_code      # M language snippet for BI realism
        self.next: "StepNode | None" = None
        self.prev: "StepNode | None" = None

    def __repr__(self):
        return f"[{self.step_name}]"


# ─────────────────────────────────────────────
# PART 1 – SINGLY LINKED LIST  (Forward-only history)
# ─────────────────────────────────────────────

class SinglyStepsTracker:
    """
    A Singly Linked List of ETL transformation steps.

    Why Singly? Good enough for an audit log — we can read the full
    pipeline from start to finish. But we can't Undo without re-traversing
    from the head, which is O(n). See DoublyStepsTracker for the fix.
    """

    def __init__(self):
        self.head: StepNode | None = None
        self.tail: StepNode | None = None   # Keep a tail pointer → O(1) append
        self.size = 0

    def add_step(self, step_name: str, m_code: str = "") -> None:
        """
        Appends a new step at the end of the pipeline.
        O(1) because we maintain a tail pointer — no need to traverse.

        BI Analogy: Clicking "Close & Apply" in Power Query saves a new step.
        """
        node = StepNode(step_name, m_code)
        if self.tail is None:
            self.head = self.tail = node
        else:
            self.tail.next = node
            self.tail = node
        self.size += 1
        print(f"  ✔ Step added: {node}")

    def display(self) -> None:
        """Prints the full pipeline — like Power Query's Applied Steps panel."""
        print("\n  ┌─ Applied Steps (Power Query) ───────────────")
        current = self.head
        step_num = 1
        while current:
            print(f"  │  {step_num}. {current.step_name}")
            if current.m_code:
                print(f"  │     M: {current.m_code}")
            current = current.next
            step_num += 1
        print(f"  └─ Total: {self.size} steps\n")


# ─────────────────────────────────────────────
# PART 2 – DOUBLY LINKED LIST  (Undo/Redo Engine)
# ─────────────────────────────────────────────

class AppliedStepsTracker:
    """
    A Doubly Linked List that adds full Undo/Redo capability.

    Key insight: every node now has BOTH next and prev pointers.
    - add_step()  → O(1): append at tail, update prev/next
    - undo_step() → O(1): move tail pointer back, no traversal
    - redo_step() → O(1): move tail pointer forward, no traversal

    BI Analogy: This is how VS Code / Excel implement Ctrl+Z without
    re-reading the whole file. Power BI's Query Editor works the same way
    under the hood.
    """

    def __init__(self):
        self.head: StepNode | None = None
        self.current: StepNode | None = None   # "cursor" — the active step
        self.size = 0

    def add_step(self, step_name: str, m_code: str = "") -> None:
        """
        Adds a step AFTER the current cursor position.

        Important: If you Undo to step 3 then add a new step, steps 4+ are
        discarded — exactly like Power Query's behavior. You can't have a
        branch in a linear transformation pipeline.
        """
        node = StepNode(step_name, m_code)

        if self.head is None:
            # Empty list — first step
            self.head = node
            self.current = node
        else:
            # Discard any future steps (steps after current cursor)
            # They become unreachable — garbage collected by Python
            self.current.next = node
            node.prev = self.current
            self.current = node

        self.size += 1
        print(f"  ✔ Step added: {node}")

    def undo_step(self) -> str | None:
        """
        Moves the cursor one step backward — O(1).

        BI Analogy: Right-clicking a step in Power Query and choosing
        "Delete" or pressing Ctrl+Z to revert the last transformation.
        The data logically reverts to the previous state without recomputing
        everything from scratch — only the cursor moves.
        """
        if self.current is None:
            print("  ⚠ Nothing to undo — pipeline is at the beginning.")
            return None

        undone = self.current.step_name
        self.current = self.current.prev   # O(1) — just update pointer
        if self.current:
            print(f"  ↩ Undone: '{undone}' | Now at: {self.current}")
        else:
            print(f"  ↩ Undone: '{undone}' | Pipeline is now empty.")
        return undone

    def redo_step(self) -> str | None:
        """
        Moves the cursor one step forward — O(1).

        BI Analogy: Ctrl+Y in Power Query to re-apply a step you just undid.
        """
        next_node = self.current.next if self.current else self.head
        if next_node is None:
            print("  ⚠ Nothing to redo — already at the latest step.")
            return None

        self.current = next_node
        print(f"  ↪ Redone: {self.current}")
        return self.current.step_name

    def display(self) -> None:
        """
        Prints the full step history, marking the current active position.
        Steps after the cursor are grayed out (undone but still in memory).
        """
        print("\n  ┌─ Applied Steps (Doubly Linked – Undo/Redo) ─────────")
        current_node = self.head
        step_num = 1
        while current_node:
            cursor = " ◄ CURRENT" if current_node is self.current else ""
            faded  = " (undone)" if (
                self.current and
                self._is_after_current(current_node)
            ) else ""
            print(f"  │  {step_num}. {current_node.step_name}{cursor}{faded}")
            current_node = current_node.next
            step_num += 1
        print(f"  └─────────────────────────────────────────────────────\n")

    def _is_after_current(self, node: StepNode) -> bool:
        """Helper: checks if a node comes after the current cursor."""
        if self.current is None:
            return True
        check = self.current.next
        while check:
            if check is node:
                return True
            check = check.next
        return False


# ─────────────────────────────────────────────
# DEMO PRELOADED PIPELINE
# ─────────────────────────────────────────────

def demo_pipeline() -> AppliedStepsTracker:
    """
    Pre-loads a realistic NileMart Power Query pipeline for demo purposes.
    These are real M language snippets so the output looks authentic.
    """
    tracker = AppliedStepsTracker()
    steps = [
        ("Source",           "= Csv.Document(File.Contents(\"transactions.csv\"))"),
        ("Promoted Headers", "= Table.PromoteHeaders(Source)"),
        ("Changed Type",     "= Table.TransformColumnTypes(#\"Promoted Headers\", {{\"txn_id\", Int64.Type}})"),
        ("Removed Nulls",    "= Table.SelectRows(#\"Changed Type\", each [txn_id] <> null)"),
        ("Filtered Q3",      "= Table.SelectRows(#\"Removed Nulls\", each [date_key] >= 20240701 and [date_key] <= 20240930)"),
    ]
    for name, m_code in steps:
        tracker.add_step(name, m_code)
    return tracker
