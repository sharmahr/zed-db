"""
Zed Database - Query Planner

Analyzes SQL AST and generates optimized execution plans.
Implements semantic analysis, optimization, and plan generation.
"""

from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field

from zed.sql.ast import (
    SelectStatement,
    ColumnRef,
    BinaryOp,
    FunctionCall,
    Literal,
)


@dataclass
class PlanNode:
    """Base class for plan nodes."""
    node_type: str
    children: List["PlanNode"] = field(default_factory=list)
    
    def to_dict(self) -> Dict:
        """Convert plan to dictionary for inspection."""
        return {
            "type": self.node_type,
            "children": [c.to_dict() for c in self.children]
        }


@dataclass
class SeqScanNode(PlanNode):
    """Sequential scan of a table."""
    table_name: str = ""
    alias: Optional[str] = None
    filter_expr: Any = None  # WHERE condition
    
    def __init__(self, table_name: str, alias: Optional[str] = None, 
                 filter_expr: Any = None):
        super().__init__(node_type="SeqScan")
        self.table_name = table_name
        self.alias = alias
        self.filter_expr = filter_expr
    
    def to_dict(self) -> Dict:
        return {
            "type": self.node_type,
            "table": self.table_name,
            "alias": self.alias,
            "filter": str(self.filter_expr) if self.filter_expr else None
        }


@dataclass
class IndexScanNode(PlanNode):
    """Index scan using an index."""
    table_name: str = ""
    index_column: str = ""
    filter_expr: Any = None
    index_type: str = "hash"  # hash or btree
    
    def __init__(self, table_name: str, index_column: str, 
                 filter_expr: Any = None, index_type: str = "hash"):
        super().__init__(node_type="IndexScan")
        self.table_name = table_name
        self.index_column = index_column
        self.filter_expr = filter_expr
        self.index_type = index_type
    
    def to_dict(self) -> Dict:
        return {
            "type": self.node_type,
            "table": self.table_name,
            "index_column": self.index_column,
            "index_type": self.index_type,
            "filter": str(self.filter_expr) if self.filter_expr else None
        }


@dataclass
class FilterNode(PlanNode):
    """Filter operator (WHERE clause)."""
    condition: Any = None
    
    def __init__(self, condition: Any):
        super().__init__(node_type="Filter")
        self.condition = condition
    
    def to_dict(self) -> Dict:
        return {
            "type": self.node_type,
            "condition": str(self.condition)
        }


@dataclass
class ProjectNode(PlanNode):
    """Projection operator (SELECT columns)."""
    columns: List[Any] = field(default_factory=list)
    
    def __init__(self, columns: List[Any]):
        super().__init__(node_type="Project")
        self.columns = columns
    
    def to_dict(self) -> Dict:
        col_names = []
        for c in self.columns:
            if isinstance(c, ColumnRef):
                col_names.append(c.name)
            elif isinstance(c, FunctionCall):
                col_names.append(c.name)
            else:
                col_names.append(str(c))
        return {
            "type": self.node_type,
            "columns": col_names
        }


@dataclass
class AggregateNode(PlanNode):
    """Aggregation operator (GROUP BY, aggregates)."""
    group_by: List[Any] = field(default_factory=list)
    aggregates: List[Any] = field(default_factory=list)
    
    def __init__(self, group_by: List[Any] = None, aggregates: List[Any] = None):
        super().__init__(node_type="Aggregate")
        self.group_by = group_by or []
        self.aggregates = aggregates or []
    
    def to_dict(self) -> Dict:
        return {
            "type": self.node_type,
            "group_by": [g.name if isinstance(g, ColumnRef) else str(g) for g in self.group_by],
            "aggregates": [a.name if isinstance(a, FunctionCall) else str(a) for a in self.aggregates]
        }


@dataclass
class JoinNode(PlanNode):
    """Join operator."""
    join_type: str = "INNER"  # INNER, LEFT, RIGHT, CROSS
    on_condition: Any = None
    left_table: str = ""
    right_table: str = ""
    
    def __init__(self, join_type: str, on_condition: Any = None,
                 left_table: str = "", right_table: str = ""):
        super().__init__(node_type=f"{join_type}Join")
        self.join_type = join_type
        self.on_condition = on_condition
        self.left_table = left_table
        self.right_table = right_table
    
    def to_dict(self) -> Dict:
        return {
            "type": self.node_type,
            "left": self.left_table,
            "right": self.right_table,
            "on": str(self.on_condition) if self.on_condition else None
        }


class QueryPlan:
    """Represents a complete query execution plan."""
    
    def __init__(self, root: PlanNode):
        self.root = root
        self.scan_method = "seq_scan"  # Default
    
    def to_dict(self) -> Dict:
        return {
            "scan_method": self.scan_method,
            "plan": self.root.to_dict()
        }
    
    def __repr__(self):
        return f"QueryPlan(scan_method={self.scan_method})"


class QueryPlanner:
    """
    Query Planner: Analyzes SQL AST and generates optimized execution plans.
    
    Implements:
    - Semantic analysis (basic validation)
    - Optimization (index selection, predicate pushdown)
    - Plan generation (tree of operators)
    """
    
    def __init__(self, schema=None, indexes: Dict[str, Any] = None):
        """
        Initialize planner.
        
        Args:
            schema: Database schema for validation
            indexes: Dict of table.column -> index for index selection
        """
        self.schema = schema
        self.indexes = indexes or {}
    
    def plan(self, statement: SelectStatement) -> QueryPlan:
        """
        Generate execution plan for a SELECT statement.
        
        Returns optimized QueryPlan.
        """
        # Step 1: Semantic Analysis
        self._semantic_analysis(statement)
        
        # Step 2: Build initial plan
        plan = self._build_plan(statement)
        
        # Step 3: Optimize
        plan = self._optimize(plan, statement)
        
        return plan
    
    def _semantic_analysis(self, stmt: SelectStatement):
        """Validate the statement (basic checks)."""
        # Check tables exist (if schema available)
        if self.schema:
            for table_ref in stmt.tables:
                table = self.schema.get_table(table_ref.name)
                if table is None:
                    # Could raise error, but for now just skip
                    pass
    
    def _build_plan(self, stmt: SelectStatement) -> QueryPlan:
        """Build initial execution plan from AST."""
        # Get base table
        if not stmt.tables:
            # Empty plan
            return QueryPlan(root=SeqScanNode(table_name=""))
        
        base_table = stmt.tables[0]
        table_name = base_table.name
        alias = base_table.alias
        
        # Check if we can use index scan
        scan_node = self._choose_scan(stmt, table_name)
        
        # Build plan tree
        current = scan_node
        
        # Add joins
        for join in stmt.joins:
            current = JoinNode(
                join_type=join.join_type,
                on_condition=join.on,
                left_table=table_name,
                right_table=join.table.name
            )
            current.children = [scan_node]
            # Recreate scan for right table
            right_scan = SeqScanNode(
                table_name=join.table.name,
                alias=join.table.alias
            )
            current.children.append(right_scan)
            scan_node = current  # Update root
        
        # Add filter (WHERE) if not already in scan
        if stmt.where and not isinstance(scan_node, IndexScanNode):
            filter_node = FilterNode(condition=stmt.where)
            filter_node.children = [current]
            current = filter_node
        
        # Add aggregate if needed
        has_aggregates = any(isinstance(c, FunctionCall) for c in stmt.columns)
        if has_aggregates or stmt.group_by:
            agg_node = AggregateNode(
                group_by=stmt.group_by,
                aggregates=[c for c in stmt.columns if isinstance(c, FunctionCall)]
            )
            agg_node.children = [current]
            current = agg_node
        
        # Add projection
        project_node = ProjectNode(columns=stmt.columns)
        project_node.children = [current]
        current = project_node
        
        return QueryPlan(root=current)
    
    def _choose_scan(self, stmt: SelectStatement, table_name: str) -> PlanNode:
        """
        Choose between SeqScan and IndexScan based on query.
        
        Optimization: If WHERE has equality on indexed column, use IndexScan.
        """
        where = stmt.where
        
        # Check for simple equality: column = value
        if where and isinstance(where, BinaryOp) and where.op == "=":
            if isinstance(where.left, ColumnRef):
                col_name = where.left.name
                
                # Check if index exists
                index_key = f"{table_name}.{col_name}"
                if index_key in self.indexes:
                    return IndexScanNode(
                        table_name=table_name,
                        index_column=col_name,
                        filter_expr=where,
                        index_type="hash"
                    )
        
        # Default: sequential scan
        return SeqScanNode(
            table_name=table_name,
            alias=stmt.tables[0].alias if stmt.tables else None,
            filter_expr=where
        )
    
    def _optimize(self, plan: QueryPlan, stmt: SelectStatement) -> QueryPlan:
        """
        Apply optimizations to the plan.
        
        Optimizations:
        - Index selection (already done in _choose_scan)
        - Predicate pushdown (WHERE into scan)
        - Join reordering (future)
        - Projection pushdown (future)
        """
        # Set scan method based on root
        if isinstance(plan.root, IndexScanNode):
            plan.scan_method = "index_scan"
        elif isinstance(plan.root, SeqScanNode):
            plan.scan_method = "seq_scan"
        
        return plan
    
    def explain(self, statement: SelectStatement) -> str:
        """Generate EXPLAIN output for a query."""
        plan = self.plan(statement)
        
        lines = ["Query Plan:", "=" * 40]
        lines.append(f"Scan Method: {plan.scan_method}")
        lines.append("")
        lines.append("Plan Tree:")
        lines.extend(self._format_plan(plan.root, indent=0))
        
        return "\n".join(lines)
    
    def _format_plan(self, node: PlanNode, indent: int) -> List[str]:
        """Format plan tree as text."""
        prefix = "  " * indent
        lines = [f"{prefix}- {node.node_type}"]
        
        if isinstance(node, SeqScanNode):
            lines[-1] += f" (table: {node.table_name})"
        elif isinstance(node, IndexScanNode):
            lines[-1] += f" (table: {node.table_name}, col: {node.index_column})"
        
        for child in node.children:
            lines.extend(self._format_plan(child, indent + 1))
        
        return lines
