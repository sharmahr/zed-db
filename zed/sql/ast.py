"""
SQL AST (Abstract Syntax Tree) Nodes

Defines the node types for the SQL parser's AST representation.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


# =============================================================================
# Base Classes
# =============================================================================

@dataclass
class ASTNode:
    """Base class for all AST nodes."""
    pass


@dataclass
class Statement(ASTNode):
    """Base class for all SQL statements."""
    pass


# =============================================================================
# Expression Nodes
# =============================================================================

@dataclass
class Expression(ASTNode):
    """Base class for expressions."""
    pass


@dataclass
class ColumnRef(Expression):
    """Reference to a column (optionally with table qualifier)."""
    name: str
    table: Optional[str] = None  # For qualified references like table.column
    
    def __repr__(self):
        if self.table:
            return f"ColumnRef({self.table}.{self.name})"
        return f"ColumnRef({self.name})"


@dataclass
class TableRef(ASTNode):
    """Reference to a table (optionally with alias)."""
    name: str
    alias: Optional[str] = None
    
    def __repr__(self):
        if self.alias:
            return f"TableRef({self.name} AS {self.alias})"
        return f"TableRef({self.name})"


@dataclass
class Literal(Expression):
    """A literal value (string, number, null, boolean)."""
    value: Any
    type: str = "auto"  # "int", "real", "text", "bool", "null", "auto"
    
    def __repr__(self):
        if self.value is None:
            return "Literal(NULL)"
        return f"Literal({self.value!r})"


@dataclass
class BinaryOp(Expression):
    """Binary operation (e.g., a + b, x > 5)."""
    op: str  # Operator: +, -, *, /, %, =, !=, <, <=, >, >=, AND, OR, LIKE
    left: Expression
    right: Expression
    
    def __repr__(self):
        return f"BinaryOp({self.left} {self.op} {self.right})"


@dataclass
class UnaryOp(Expression):
    """Unary operation (e.g., NOT x, -5)."""
    op: str  # Operator: NOT, -, +
    expr: Expression
    
    def __repr__(self):
        return f"UnaryOp({self.op} {self.expr})"


@dataclass
class FunctionCall(Expression):
    """Function call (e.g., COUNT(*), SUM(age))."""
    name: str
    args: List[Expression] = field(default_factory=list)
    distinct: bool = False
    
    def __repr__(self):
        args_str = ", ".join(repr(a) for a in self.args)
        return f"FunctionCall({self.name}({args_str}))"


# =============================================================================
# CREATE TABLE Statement
# =============================================================================

@dataclass
class ColumnDef(ASTNode):
    """Column definition in CREATE TABLE."""
    name: str
    dtype: str  # "INT", "TEXT", "REAL", "BOOL"
    nullable: bool = True
    primary_key: bool = False
    default: Optional[Any] = None
    
    def __repr__(self):
        parts = [self.name, self.dtype]
        if self.primary_key:
            parts.append("PRIMARY KEY")
        if not self.nullable:
            parts.append("NOT NULL")
        if self.default is not None:
            parts.append(f"DEFAULT {self.default}")
        return f"ColumnDef({' '.join(parts)})"


@dataclass
class CreateTableStatement(Statement):
    """CREATE TABLE statement."""
    table_name: str
    columns: List[ColumnDef]
    
    def __repr__(self):
        cols = "\n    ".join(repr(c) for c in self.columns)
        return f"CreateTableStatement({self.table_name})\n    {cols}"


# =============================================================================
# INSERT Statement
# =============================================================================

@dataclass
class InsertStatement(Statement):
    """INSERT INTO statement."""
    table_name: str
    columns: Optional[List[str]] = None  # None means all columns
    values: List[List[Expression]] = field(default_factory=list)
    
    def __repr__(self):
        cols = f" ({', '.join(self.columns)})" if self.columns else ""
        vals = ",\n    ".join(
            "(" + ", ".join(repr(v) for v in row) + ")"
            for row in self.values
        )
        return f"InsertStatement({self.table_name}{cols})\n    {vals}"


# =============================================================================
# DELETE Statement
# =============================================================================

@dataclass
class DeleteStatement(Statement):
    """DELETE FROM statement."""
    table_name: str
    where: Optional[Expression] = None  # None means delete all rows
    
    def __repr__(self):
        where_str = f" WHERE {self.where}" if self.where else ""
        return f"DeleteStatement({self.table_name}{where_str})"


# =============================================================================
# UPDATE Statement
# =============================================================================

@dataclass
class UpdateStatement(Statement):
    """UPDATE statement."""
    table_name: str
    set_values: Dict[str, Expression]  # column -> new value expression
    where: Optional[Expression] = None  # None means update all rows
    
    def __repr__(self):
        sets = ", ".join(f"{k}={v}" for k, v in self.set_values.items())
        where_str = f" WHERE {self.where}" if self.where else ""
        return f"UpdateStatement({self.table_name} SET {sets}{where_str})"


# =============================================================================
# SELECT Statement
# =============================================================================

@dataclass
class SelectStatement(Statement):
    """SELECT statement."""
    columns: List[Expression]  # ColumnRef, Literal, FunctionCall, or * (as ColumnRef("*"))
    tables: List[TableRef]
    where: Optional[Expression] = None
    joins: List["JoinClause"] = field(default_factory=list)
    group_by: List[Expression] = field(default_factory=list)
    having: Optional[Expression] = None
    order_by: List[tuple] = field(default_factory=list)  # [(expr, "ASC"|"DESC")]
    distinct: bool = False
    limit: Optional[int] = None
    offset: Optional[int] = None
    
    def __repr__(self):
        cols = ", ".join(repr(c) for c in self.columns)
        tables = ", ".join(repr(t) for t in self.tables)
        result = f"SelectStatement(\n  columns: [{cols}]\n  tables: [{tables}]"
        if self.where:
            result += f"\n  where: {self.where}"
        if self.joins:
            result += f"\n  joins: {self.joins}"
        if self.group_by:
            result += f"\n  group_by: {self.group_by}"
        if self.order_by:
            result += f"\n  order_by: {self.order_by}"
        if self.limit:
            result += f"\n  limit: {self.limit}"
        result += "\n)"
        return result


@dataclass
class JoinClause(ASTNode):
    """JOIN clause in SELECT."""
    join_type: str  # "INNER", "LEFT", "RIGHT", "CROSS"
    table: TableRef
    on: Optional[Expression] = None
    
    def __repr__(self):
        if self.on:
            return f"JoinClause({self.join_type} {self.table} ON {self.on})"
        return f"JoinClause({self.join_type} {self.table})"


# =============================================================================
# Transaction Statements
# =============================================================================

@dataclass
class BeginStatement(Statement):
    """BEGIN transaction statement."""
    def __repr__(self):
        return "BeginStatement()"


@dataclass
class CommitStatement(Statement):
    """COMMIT transaction statement."""
    def __repr__(self):
        return "CommitStatement()"


@dataclass
class RollbackStatement(Statement):
    """ROLLBACK transaction statement."""
    def __repr__(self):
        return "RollbackStatement()"
