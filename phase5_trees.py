"""
Phase 5: The Hierarchical Matrix Builder – Trees
=================================================
BI Context: Power BI's Matrix visual and Drill-Down features are built on
tree structures. The "Region → City → Branch → Product" hierarchy IS a tree.

Two structures here:
  1. Binary Search Tree (BST): For the Customer Dimension table.
     Power BI compresses dimension tables and uses B-Tree indexes for fast
     RELATED() and LOOKUPVALUE() operations. Our BST mimics this.

  2. N-ary Tree (Org Chart): For the Employee hierarchy.
     The Roll-Up aggregation (sum sales up the org tree) is exactly what
     Power BI's SUMMARIZE + ROLLUP DAX functions do internally.

Author: DataFlow Pro – NileMart ETL Engine
"""

from anytree import Node, RenderTree, PreOrderIter


# ─────────────────────────────────────────────
# PART 1 – BINARY SEARCH TREE (Customer Dimension Index)
# ─────────────────────────────────────────────

class BSTNode:
    """
    A single customer record node in the BST.

    national_id: the sort key (like a clustered index key in SQL Server)
    name:        the dimension attribute

    Left child  → smaller national_id
    Right child → larger national_id

    This invariant is what makes search O(log n) instead of O(n).
    """
    def __init__(self, national_id: int, name: str):
        self.national_id = national_id
        self.name = name
        self.left:  "BSTNode | None" = None
        self.right: "BSTNode | None" = None


class DimensionIndex:
    """
    A Binary Search Tree storing unique Customer IDs (National IDs).

    BI Context: Power BI's VertiPaq engine stores dimension tables sorted by
    key column. Every RELATED() call navigates a similar tree structure —
    at each node, it goes left (smaller) or right (larger) until it finds
    the match. At 1M customers, that's at most log₂(1M) ≈ 20 comparisons.

    Operations:
      insert() → O(log n) average, O(n) worst (degenerate/sorted input)
      search() → O(log n) average
      in_order_traversal() → O(n)  — produces sorted output (like ORDER BY)
    """

    def __init__(self):
        self.root: BSTNode | None = None

    # ── INSERT ──────────────────────────────

    def insert(self, national_id: int, name: str) -> None:
        """
        Inserts a customer into the BST — O(log n) average.
        Duplicates are ignored (dimension tables have unique keys).
        """
        self.root = self._insert_recursive(self.root, national_id, name)

    def _insert_recursive(self, node: BSTNode | None, national_id: int, name: str) -> BSTNode:
        """
        Recursive insertion — navigates left/right until finding an empty slot.

        Pattern: This mirrors how SQL Server's B-Tree index insertion works,
        except SQL Server also handles page splits and balancing. Our BST
        is simpler (no auto-balancing — see note on AVL/Red-Black trees below).
        """
        if node is None:
            return BSTNode(national_id, name)   # Base case: empty slot found

        if national_id < node.national_id:
            node.left  = self._insert_recursive(node.left,  national_id, name)
        elif national_id > node.national_id:
            node.right = self._insert_recursive(node.right, national_id, name)
        # If equal: duplicate — skip

        return node

    # ── SEARCH ──────────────────────────────

    def search(self, national_id: int) -> str:
        """
        Finds a customer by National ID — O(log n).
        Returns name if found, error message otherwise.

        BI Analogy: Every RELATED('Customer'[Name]) call in a DAX measure
        performs this exact lookup on the compressed dimension table.
        """
        result = self._search_recursive(self.root, national_id)
        if result:
            return f"✔ Found: {result.name} (ID: {result.national_id})"
        return f"✘ Customer ID {national_id} not found in dimension index"

    def _search_recursive(self, node: BSTNode | None, national_id: int) -> BSTNode | None:
        if node is None:
            return None                        # Not found
        if national_id == node.national_id:
            return node                        # Found!
        elif national_id < node.national_id:
            return self._search_recursive(node.left, national_id)
        else:
            return self._search_recursive(node.right, national_id)

    # ── TRAVERSALS ──────────────────────────

    def in_order_traversal(self) -> list[tuple]:
        """
        Visits nodes in ascending national_id order — O(n).
        Left → Current → Right

        BI Analogy: SELECT national_id, name FROM Customer ORDER BY national_id
        The BST's structure guarantees sorted output without an extra sort pass.
        """
        result = []
        self._in_order(self.root, result)
        return result

    def _in_order(self, node: BSTNode | None, result: list) -> None:
        if node is None:
            return
        self._in_order(node.left, result)
        result.append((node.national_id, node.name))
        self._in_order(node.right, result)

    def display(self) -> None:
        """Prints the BST as a sorted dimension table."""
        records = self.in_order_traversal()
        print(f"\n  ┌─ Customer Dimension Index (sorted by National ID) ─")
        for nid, name in records:
            print(f"  │  {nid}  →  {name}")
        print(f"  └─ {len(records)} unique customers indexed\n")

    # ── IMPORTANT NOTE ON BST BALANCING ──────────────────────────────────────
    # A plain BST degrades to O(n) if data is inserted in sorted order
    # (the tree becomes a straight line). Production databases use:
    #   - AVL Trees:       self-balancing, strict height control
    #   - Red-Black Trees: used by Java TreeMap, C++ std::map
    #   - B+ Trees:        used by SQL Server, MySQL, PostgreSQL indexes
    # For this project, plain BST is sufficient. Just know the limitation.


# ─────────────────────────────────────────────
# PART 2 – N-ARY TREE (Organizational Chart)
# ─────────────────────────────────────────────

class OrgChartAnalyzer:
    """
    Models NileMart's corporate hierarchy as an N-ary tree using anytree.

    BI Context: This is the data model behind Power BI's Matrix visual with
    Row Hierarchy: CEO → VP → Store Manager → Sales Rep. The drill-down
    feature traverses exactly this tree structure.

    The Roll-Up aggregation (sum bottom-up) is equivalent to:
      DAX: CALCULATE(SUM(Sales[Amount]), DESCENDANTS(Manager[VP_Cairo]))
    We implement this recursively to understand the underlying mechanism.
    """

    def __init__(self):
        """
        Build the NileMart corporate tree.
        Each node has a 'sales' attribute = direct revenue generated.
        Manager nodes have sales=0 (they don't sell directly).
        """
        # Root
        self.ceo = Node("Omar (Global CEO)", sales=0)

        # Regional VPs — level 2
        self.vp_cairo = Node("Tarek (VP Cairo & Giza)", parent=self.ceo,     sales=0)
        self.vp_alex  = Node("Salma (VP Alex & Delta)", parent=self.ceo,     sales=0)

        # Store Managers — level 3
        mgr_maadi   = Node("Hana (Maadi Manager)",     parent=self.vp_cairo, sales=0)
        mgr_zayed   = Node("Bassem (Zayed Manager)",   parent=self.vp_cairo, sales=0)
        mgr_smouha  = Node("Rania (Smouha Manager)",   parent=self.vp_alex,  sales=0)
        mgr_mans    = Node("Fares (Mansoura Manager)", parent=self.vp_alex,  sales=0)

        # Sales Reps (leaf nodes) — level 4  → actual revenue in EGP
        Node("Aya (Maadi Rep 1)",     parent=mgr_maadi,  sales=150_000)
        Node("Sherif (Maadi Rep 2)",  parent=mgr_maadi,  sales=95_000)
        Node("Mahmoud (Zayed Rep)",   parent=mgr_zayed,  sales=270_000)
        Node("Dina (Zayed Rep 2)",    parent=mgr_zayed,  sales=180_000)
        Node("Kareem (Smouha Rep)",   parent=mgr_smouha, sales=180_000)
        Node("Layla (Smouha Rep 2)",  parent=mgr_smouha, sales=220_000)
        Node("Nour (Mansoura Rep)",   parent=mgr_mans,   sales=120_000)
        Node("Adel (Mansoura Rep 2)", parent=mgr_mans,   sales=95_000)

    def display_chart(self) -> None:
        """
        Prints the full visual hierarchy using anytree's RenderTree.

        BI Analogy: This is what you see when you expand all levels in a
        Power BI Matrix visual with row hierarchy enabled.
        """
        print("\n  ┌─ NileMart Organizational Hierarchy ─────────────────")
        for pre, _, node in RenderTree(self.ceo):
            sales_str = f"  [{node.sales:>10,.0f} EGP direct]" if node.sales > 0 else ""
            print(f"  │ {pre}{node.name}{sales_str}")
        print("  └──────────────────────────────────────────────────────\n")

    def roll_up_sales(self, node: Node) -> int:
        """
        Recursively sums sales from bottom of tree up to this node.
        O(n) where n = number of descendants under this node.

        Algorithm:
          total = this_node.direct_sales + sum(roll_up(child) for each child)

        Base case: leaf node (no children) → returns its own sales figure.

        BI Context: This is what Power BI calculates when you drag a manager
        into the rows of a Matrix — it shows the subtotal for all reps below.
        Equivalent DAX: CALCULATE(SUM(Sales[Amount]), TREATAS(...))

        Example for vp_cairo:
          roll_up(vp_cairo)
            = 0 + roll_up(mgr_maadi) + roll_up(mgr_zayed)
            = 0 + (0 + 150000 + 95000) + (0 + 270000 + 180000)
            = 695,000 EGP
        """
        if node is None:
            return 0

        # Base case: leaf node — no children
        if not node.children:
            return node.sales

        # Recursive case: own sales + sum of all children's roll-ups
        total = node.sales + sum(self.roll_up_sales(child) for child in node.children)
        return total

    def print_roll_up_report(self) -> None:
        """Prints a full sales summary at every management level."""
        print("\n  ┌─ Roll-Up Sales Report ───────────────────────────────")
        print(f"  │  {'Node':<35} {'Roll-Up Total':>15}")
        print(f"  │  {'─'*35} {'─'*15}")

        for node in PreOrderIter(self.ceo):
            total = self.roll_up_sales(node)
            indent = "  │  " + "    " * node.depth
            print(f"{indent}{node.name:<31} {total:>15,.0f} EGP")

        grand_total = self.roll_up_sales(self.ceo)
        print(f"  │")
        print(f"  │  {'GRAND TOTAL (All Egypt)':<35} {grand_total:>15,.0f} EGP")
        print(f"  └──────────────────────────────────────────────────────\n")
