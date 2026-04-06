"""
Phase 3: The DAX Formula Parser – Stacks
=========================================
BI Context: DAX (Data Analysis Expressions) is the formula language of Power BI.
Analysts write formulas like: (Revenue - Cost) * Tax_Rate

Computers can't evaluate infix notation (human math) directly — they need
a linear, unambiguous form called Postfix (Reverse Polish Notation).

Example:
  Infix:   (15000 - 5000) * 2
  Postfix: 15000 5000 - 2 *    ← no parentheses needed; order is explicit

A Stack (Last In, First Out) is the perfect tool because:
  - We read tokens left to right.
  - Numbers go ON the stack.
  - When we see an operator, we POP the last two numbers, compute, PUSH result.
  - At the end, one number remains — the answer.

This is exactly how Python's own interpreter evaluates expressions internally.

Author: DataFlow Pro – NileMart ETL Engine
"""


# ─────────────────────────────────────────────
# PART 1 – STACK USING ARRAY (Python list)
# ─────────────────────────────────────────────

class ArrayStack:
    """
    Stack backed by a Python list.

    - push()  → O(1) amortized  (list append)
    - pop()   → O(1)            (list pop from end)
    - peek()  → O(1)

    BI Analogy: Think of a stack of printed reports on your desk.
    You always add to the top and take from the top. The bottom (first item)
    is hardest to reach — that's the Last In, First Out principle.
    """

    def __init__(self):
        self._data: list = []

    def push(self, item) -> None:
        """Add item to top of stack — O(1) amortized."""
        self._data.append(item)

    def pop(self):
        """Remove and return top item — O(1). Raises if empty."""
        if self.is_empty():
            raise IndexError("Stack underflow — popping from empty stack")
        return self._data.pop()

    def peek(self):
        """Return top item without removing — O(1)."""
        if self.is_empty():
            raise IndexError("Stack is empty")
        return self._data[-1]

    def is_empty(self) -> bool:
        return len(self._data) == 0

    def size(self) -> int:
        return len(self._data)

    def __repr__(self):
        return f"Stack(bottom→top): {self._data}"


# ─────────────────────────────────────────────
# PART 2 – STACK USING LINKED LIST
# ─────────────────────────────────────────────

class _StackNode:
    def __init__(self, value):
        self.value = value
        self.next: "_StackNode | None" = None


class LinkedListStack:
    """
    Stack backed by a Linked List.

    - push()  → O(1): prepend a new head node
    - pop()   → O(1): remove head node
    - No resizing overhead (unlike a list that occasionally doubles in memory)

    BI Analogy: Like a singly linked list of transformation steps where you
    only ever add/remove from the front. Useful in memory-constrained ETL
    containers (Docker pods with fixed RAM limits).
    """

    def __init__(self):
        self._head: _StackNode | None = None
        self._size = 0

    def push(self, item) -> None:
        node = _StackNode(item)
        node.next = self._head
        self._head = node
        self._size += 1

    def pop(self):
        if self.is_empty():
            raise IndexError("Stack underflow")
        value = self._head.value
        self._head = self._head.next
        self._size -= 1
        return value

    def peek(self):
        if self.is_empty():
            raise IndexError("Stack is empty")
        return self._head.value

    def is_empty(self) -> bool:
        return self._size == 0

    def size(self) -> int:
        return self._size


# ─────────────────────────────────────────────
# PART 3 – PARENTHESIS VALIDATOR
# ─────────────────────────────────────────────

def validate_parentheses(expression: str) -> bool:
    """
    Checks that every opening bracket has a matching closing bracket.
    O(n) time, O(n) space.

    BI Context: Before Power BI evaluates a DAX formula, it validates syntax.
    An unmatched parenthesis causes a parse error and the dashboard fails to load.
    This function replicates that first-pass validation step.

    Examples:
      "(Revenue - Cost) * Tax"  →  True
      "((Revenue - Cost) * Tax" →  False  (missing closing paren)
      ")Revenue("               →  False  (wrong order)
    """
    stack = ArrayStack()
    pairs = {')': '(', ']': '[', '}': '{'}

    for char in expression:
        if char in '([{':
            stack.push(char)
        elif char in ')]}':
            if stack.is_empty() or stack.pop() != pairs[char]:
                return False

    return stack.is_empty()   # True only if all brackets were matched


# ─────────────────────────────────────────────
# PART 4 – POSTFIX EVALUATOR  (The DAX Engine)
# ─────────────────────────────────────────────

class DAXEvaluator:
    """
    Evaluates Postfix (Reverse Polish Notation) DAX expressions.

    Algorithm (O(n)):
      1. Tokenize the expression string.
      2. For each token:
         - If number → push onto stack.
         - If operator → pop two numbers, apply operator, push result.
      3. Final stack contains exactly one value: the answer.

    Supported operators: + - * / ^ (exponentiation) % (modulo)

    BI Context: Analysts type infix DAX; the Power BI engine converts it to
    postfix internally before evaluating. We skip the conversion and accept
    postfix directly to focus on the evaluation algorithm.

    Example:
      Input:  "15000 5000 - 2 *"
      Means:  (15000 - 5000) * 2
      Result: 20000
    """

    OPERATORS = {
        '+': lambda a, b: a + b,
        '-': lambda a, b: a - b,
        '*': lambda a, b: a * b,
        '/': lambda a, b: a / b if b != 0 else (_ for _ in ()).throw(ZeroDivisionError("Division by zero in DAX formula")),
        '^': lambda a, b: a ** b,
        '%': lambda a, b: a % b,
    }

    def evaluate_postfix(self, expression: str) -> float:
        """
        Evaluates a postfix expression string.

        Args:
            expression: space-separated postfix tokens, e.g. "15000 5000 - 2 *"

        Returns:
            float result of the expression.

        Raises:
            ValueError: for malformed expressions.
            ZeroDivisionError: for division by zero.
        """
        stack = ArrayStack()
        tokens = expression.strip().split()

        if not tokens:
            raise ValueError("Empty DAX expression")

        for token in tokens:
            if token in self.OPERATORS:
                # Need exactly two operands
                if stack.size() < 2:
                    raise ValueError(f"Malformed expression — not enough operands before '{token}'")

                b = stack.pop()   # Second operand (right side)
                a = stack.pop()   # First operand (left side)
                result = self.OPERATORS[token](a, b)
                stack.push(result)

                # Trace for educational output
                print(f"    {a} {token} {b} = {result}  (stack depth: {stack.size()})")

            else:
                try:
                    stack.push(float(token))
                except ValueError:
                    raise ValueError(f"Unknown token '{token}' — not a number or operator")

        if stack.size() != 1:
            raise ValueError("Malformed expression — too many operands remaining")

        return stack.pop()

    def evaluate_infix(self, expression: str) -> float:
        """
        Evaluates standard infix notation (the math humans write).
        Internally converts to postfix first, then evaluates.
        O(n) time, O(n) space.

        BI Context: This is the full pipeline that Power BI DAX engine runs —
        parse infix → convert to postfix → evaluate postfix.

        Supports: + - * /  and parentheses ( )
        Does NOT support: variables like Revenue, Tax_Rate (those need lookup tables)
        """
        postfix = self._infix_to_postfix(expression)
        print(f"    Infix:   {expression}")
        print(f"    Postfix: {postfix}")
        return self.evaluate_postfix(postfix)

    def _infix_to_postfix(self, expression: str) -> str:
        """
        Shunting Yard Algorithm (Dijkstra, 1961) — O(n).
        Converts infix to postfix using operator precedence rules.
        """
        precedence = {'+': 1, '-': 1, '*': 2, '/': 2, '^': 3}
        output  = []
        op_stack = ArrayStack()

        tokens = expression.replace('(', ' ( ').replace(')', ' ) ').split()

        for token in tokens:
            if token not in precedence and token not in '()':
                output.append(token)   # Number → goes straight to output

            elif token == '(':
                op_stack.push(token)

            elif token == ')':
                # Pop until matching '('
                while not op_stack.is_empty() and op_stack.peek() != '(':
                    output.append(op_stack.pop())
                op_stack.pop()         # Remove '(' itself

            else:
                # Operator: pop higher-or-equal precedence operators first
                while (not op_stack.is_empty() and
                       op_stack.peek() != '(' and
                       precedence.get(op_stack.peek(), 0) >= precedence[token]):
                    output.append(op_stack.pop())
                op_stack.push(token)

        while not op_stack.is_empty():
            output.append(op_stack.pop())

        return ' '.join(output)


# ─────────────────────────────────────────────
# PRESET DAX EXAMPLES
# ─────────────────────────────────────────────

SAMPLE_FORMULAS = [
    {
        "name":    "Gross Profit",
        "infix":   "(15000 - 5000) * 2",
        "postfix": "15000 5000 - 2 *",
        "note":    "Gross Profit × 2 branches",
    },
    {
        "name":    "Net Margin %",
        "infix":   "(85000 - 62000) / 85000",
        "postfix": "85000 62000 - 85000 /",
        "note":    "Net Margin = (Revenue - Cost) / Revenue",
    },
    {
        "name":    "Bonus Calculation",
        "infix":   "(270000 + 150000) * 0.05",
        "postfix": "270000 150000 + 0.05 *",
        "note":    "5% bonus on combined Cairo branch revenue",
    },
]
