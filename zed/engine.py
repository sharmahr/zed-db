"""
Zed Database - Execution Engine

Simple execution engine following KISS and Single Responsibility.
- Engine: Executes parsed statements against schema and storage
"""

from typing import Any, Dict, List, Optional

from zed.schema import Schema, Table, Column
from zed.storage import BTree
from zed.sql.ast import (
    Statement,
    CreateTableStatement,
    InsertStatement,
    SelectStatement,
    DeleteStatement,
    UpdateStatement,
    ColumnDef,
    Literal,
    ColumnRef,
    BinaryOp,
    JoinClause,
    FunctionCall,
    BeginStatement,
    CommitStatement,
    RollbackStatement,
)
from zed.wal import WriteAheadLog, WALRecordType
from zed.query_planner import QueryPlanner, QueryPlan


# =============================================================================
# Engine - Single Responsibility: Execute statements
# =============================================================================

class Engine:
    """
    Execution engine: runs SQL statements against the database.
    
    Uses:
    - Schema: Table definitions and row storage (simple in-memory)
    - B-Tree: Indexed storage (optional, per-table)
    - WAL: Write-Ahead Log for durability and transactions
    """
    
    def __init__(self, wal_path: str = "zed.wal"):
        self.schema = Schema()
        # Optional: per-table B-Trees for indexed access
        self._trees: Dict[str, BTree] = {}
        # Write-Ahead Log for durability and transactions
        self.wal = WriteAheadLog(wal_path)
        # Track if we're in a transaction
        self._in_transaction = False
        # Transaction snapshots: saved table state at BEGIN for rollback
        self._txn_snapshots: Dict[str, List[Dict]] = {}  # table_name -> list of rows (deep copy)
        # Query planner for optimization
        self.planner = QueryPlanner(schema=self.schema)
    
    def execute(self, statement: Statement) -> Any:
        """Execute a statement and return result."""
        if isinstance(statement, CreateTableStatement):
            return self._execute_create_table(statement)
        elif isinstance(statement, InsertStatement):
            return self._execute_insert(statement)
        elif isinstance(statement, DeleteStatement):
            return self._execute_delete(statement)
        elif isinstance(statement, UpdateStatement):
            return self._execute_update(statement)
        elif isinstance(statement, SelectStatement):
            return self._execute_select(statement)
        elif isinstance(statement, BeginStatement):
            return self._execute_begin()
        elif isinstance(statement, CommitStatement):
            return self._execute_commit()
        elif isinstance(statement, RollbackStatement):
            return self._execute_rollback()
        else:
            return {"error": f"Unknown statement type: {type(statement)}"}
    
    # =========================================================================
    # CREATE TABLE
    # =========================================================================
    
    def _execute_create_table(self, stmt: CreateTableStatement) -> Dict:
        """Create a new table."""
        # Build columns from AST
        columns = []
        for col_def in stmt.columns:
            columns.append(Column(
                name=col_def.name,
                dtype=col_def.dtype,
                nullable=col_def.nullable,
                primary_key=col_def.primary_key
            ))
        
        # Create table
        table = Table(name=stmt.table_name, columns=columns)
        
        if self.schema.create_table(table):
            # Create B-Tree for this table (indexed by row id)
            self._trees[stmt.table_name] = BTree()
            
            # Log to WAL
            schema_dict = {
                "columns": [{"name": c.name, "dtype": c.dtype} for c in columns]
            }
            self.wal.log_create_table(stmt.table_name, schema_dict)
            
            return {"status": "ok", "message": f"Table '{stmt.table_name}' created"}
        else:
            return {"status": "error", "message": f"Table '{stmt.table_name}' already exists"}
    
    # =========================================================================
    # INSERT
    # =========================================================================
    
    def _execute_insert(self, stmt: InsertStatement) -> Dict:
        """Insert rows into a table with type coercion."""
        table = self.schema.get_table(stmt.table_name)
        if table is None:
            return {"status": "error", "message": f"Table '{stmt.table_name}' not found"}
        
        # Determine columns to insert into
        if stmt.columns:
            insert_cols = stmt.columns
        else:
            insert_cols = table.column_names()
        
        # Validate column count
        for row_values in stmt.values:
            if len(row_values) != len(insert_cols):
                return {"status": "error", "message": "Column count mismatch"}
        
        # Insert each row with type coercion
        inserted = 0
        for row_values in stmt.values:
            row = {}
            for i, col_name in enumerate(insert_cols):
                val = self._eval_expr(row_values[i])
                # Coerce value to column type
                val = self._coerce_type(val, col_name, table)
                row[col_name] = val
            
            # Add row to table
            table.rows.append(row)
            row_index = len(table.rows) - 1
            
            # Log to WAL
            self.wal.log_insert(stmt.table_name, row, row_index)
            
            # Also insert into B-Tree (use row index as key)
            tree = self._trees.get(stmt.table_name)
            if tree:
                tree.insert(row_index, row)
            
            inserted += 1
        
        return {"status": "ok", "message": f"Inserted {inserted} row(s)"}
    
    # =========================================================================
    # DELETE
    # =========================================================================
    
    def _execute_delete(self, stmt: DeleteStatement) -> Dict:
        """Execute DELETE FROM statement."""
        table = self.schema.get_table(stmt.table_name)
        if table is None:
            return {"status": "error", "message": f"Table '{stmt.table_name}' not found"}
        
        # Log to WAL and delete
        deleted = 0
        rows_to_keep = []
        for i, row in enumerate(table.rows):
            if stmt.where is None or self._eval_condition(stmt.where, row):
                # This row will be deleted - log it
                self.wal.log_delete(stmt.table_name, i, row)
                deleted += 1
            else:
                rows_to_keep.append(row)
        
        table.rows.clear()
        table.rows.extend(rows_to_keep)
        
        return {"status": "ok", "message": f"Deleted {deleted} row(s)"}
    
    # =========================================================================
    # UPDATE
    # =========================================================================
    
    def _execute_update(self, stmt: UpdateStatement) -> Dict:
        """Execute UPDATE statement."""
        table = self.schema.get_table(stmt.table_name)
        if table is None:
            return {"status": "error", "message": f"Table '{stmt.table_name}' not found"}
        
        updated = 0
        for row in table.rows:
            if stmt.where is None or self._eval_condition(stmt.where, row):
                # Update columns with type coercion
                for col_name, expr in stmt.set_values.items():
                    val = self._eval_expr(expr, row)
                    val = self._coerce_type(val, col_name, table)
                    row[col_name] = val
                updated += 1
        
        return {"status": "ok", "message": f"Updated {updated} row(s)"}
    
    # =========================================================================
    # TRANSACTIONS
    # =========================================================================
    
    def _execute_begin(self) -> Dict:
        """Execute BEGIN statement - start a transaction."""
        if self.wal.in_transaction():
            return {"status": "error", "message": "Already in a transaction"}
        
        txn_id = self.wal.begin_transaction()
        self._in_transaction = True
        
        # Save snapshots of all current table states for potential rollback
        self._txn_snapshots.clear()
        for table_name in self.schema.list_tables():
            table = self.schema.get_table(table_name)
            if table:
                # Deep copy of rows for rollback safety
                self._txn_snapshots[table_name] = [dict(r) for r in table.rows]
        
        return {"status": "ok", "message": f"Transaction {txn_id} started"}
    
    def _execute_commit(self) -> Dict:
        """Execute COMMIT statement - commit transaction."""
        if not self.wal.in_transaction():
            return {"status": "error", "message": "Not in a transaction"}
        
        txn_id = self.wal.get_current_txn_id()
        self.wal.commit()
        self._in_transaction = False
        
        # Clear snapshots - changes are committed
        self._txn_snapshots.clear()
        
        return {"status": "ok", "message": f"Transaction {txn_id} committed"}
    
    def _execute_rollback(self) -> Dict:
        """Execute ROLLBACK statement - rollback transaction (revert in-memory data)."""
        if not self.wal.in_transaction():
            return {"status": "error", "message": "Not in a transaction"}
        
        txn_id = self.wal.get_current_txn_id()
        self.wal.rollback()
        self._in_transaction = False
        
        # Restore table states from snapshots (revert all changes)
        for table_name, saved_rows in self._txn_snapshots.items():
            table = self.schema.get_table(table_name)
            if table:
                # Restore to saved state
                table.rows.clear()
                table.rows.extend([dict(r) for r in saved_rows])
        
        # Clear snapshots
        self._txn_snapshots.clear()
        
        return {"status": "ok", "message": f"Transaction {txn_id} rolled back"}
    
    # =========================================================================
    # SELECT
    # =========================================================================
    
    def _execute_select(self, stmt: SelectStatement) -> Dict:
        """Execute SELECT query with JOIN support, index tracking, and query planner."""
        # Get base table
        if not stmt.tables:
            return {"status": "error", "message": "No table specified"}
        
        base_table = stmt.tables[0]
        table = self.schema.get_table(base_table.name)
        if table is None:
            return {"status": "error", "message": f"Table '{base_table.name}' not found"}
        
        # Use query planner to generate optimized plan
        plan = self.planner.plan(stmt)
        scan_method = plan.scan_method  # Use planner's recommendation
        
        # Start with base table rows (add table prefix for JOINs)
        rows = [dict(r) for r in table.rows]
        
        # Apply JOINs
        for join in stmt.joins:
            rows = self._apply_join(rows, join, base_table.alias or base_table.name)
        
        # Apply WHERE filter (could use index here)
        if stmt.where:
            # Check if we can use index (simple equality on single column)
            index_used = self._try_index_scan(stmt.where, table)
            if index_used:
                scan_method = "index_scan"
            rows = [r for r in rows if self._eval_condition(stmt.where, r)]
        
        # Apply aggregations if any (BEFORE projection to preserve column names)
        has_aggregates = any(isinstance(c, FunctionCall) for c in stmt.columns)
        
        if has_aggregates or stmt.group_by:
            rows = self._apply_aggregations(rows, stmt.columns, stmt.group_by)
        else:
            # Apply column projection only if no aggregates
            if stmt.columns:
                if len(stmt.columns) == 1 and isinstance(stmt.columns[0], ColumnRef) and stmt.columns[0].name == "*":
                    result_cols = list(rows[0].keys()) if rows else []
                else:
                    result_cols = []
                    for col_expr in stmt.columns:
                        if isinstance(col_expr, ColumnRef):
                            result_cols.append(col_expr.name if not col_expr.table else f"{col_expr.table}.{col_expr.name}")
                        else:
                            result_cols.append(str(col_expr))
                
                projected = []
                for row in rows:
                    new_row = {}
                    for i, col_expr in enumerate(stmt.columns):
                        if isinstance(col_expr, ColumnRef) and col_expr.name == "*":
                            new_row = row.copy()
                        elif isinstance(col_expr, ColumnRef):
                            col_name = col_expr.name
                            if col_expr.table:
                                # Qualified column: table.col
                                qualified = f"{col_expr.table}.{col_name}"
                                new_row[qualified] = row.get(qualified, row.get(col_name))
                            else:
                                new_row[col_name] = row.get(col_name)
                        else:
                            val = self._eval_expr(col_expr, row)
                            new_row[result_cols[i]] = val
                    projected.append(new_row)
                rows = projected
        
        return {
            "status": "ok",
            "columns": list(rows[0].keys()) if rows else [],
            "rows": rows,
            "count": len(rows),
            "scan_method": scan_method  # "seq_scan" or "index_scan"
        }
    
    def _apply_aggregations(self, rows: List[Dict], columns: List[Any], 
                           group_by: List[Any]) -> List[Dict]:
        """Apply aggregate functions to rows."""
        # Determine grouping columns
        if group_by:
            group_keys = []
            for gb in group_by:
                if isinstance(gb, ColumnRef):
                    group_keys.append(gb.name)
        else:
            group_keys = []
        
        # Handle empty table: still return aggregate result (COUNT=0, etc.)
        if not rows:
            if not group_keys:
                # Single aggregate result for empty table
                row_result = {}
                for col_expr in columns:
                    if isinstance(col_expr, FunctionCall):
                        if col_expr.name.upper() == "COUNT":
                            row_result[col_expr.name] = 0
                        else:
                            row_result[col_expr.name] = None
                    elif isinstance(col_expr, ColumnRef):
                        if col_expr.name != "*":
                            row_result[col_expr.name] = None
                return [row_result] if row_result else []
            else:
                return []  # GROUP BY on empty table returns nothing
        
        # Group rows
        if group_keys:
            groups: Dict[tuple, List[Dict]] = {}
            for row in rows:
                key = tuple(row.get(k) for k in group_keys)
                if key not in groups:
                    groups[key] = []
                groups[key].append(row)
        else:
            # Single group with all rows
            groups = {(): rows}
        
        # Apply aggregates per group
        result = []
        for key, group_rows in groups.items():
            row_result = {}
            
            # Add group by columns
            if group_keys:
                for i, k in enumerate(group_keys):
                    row_result[k] = key[i]
            
            # Apply each column expression
            for col_expr in columns:
                if isinstance(col_expr, FunctionCall):
                    result_val = self._apply_aggregate(col_expr, group_rows)
                    row_result[col_expr.name] = result_val
                elif isinstance(col_expr, ColumnRef):
                    if col_expr.name == "*":
                        # COUNT(*) handled specially
                        pass
                    else:
                        # Take first value (works for GROUP BY columns)
                        if group_rows:
                            row_result[col_expr.name] = group_rows[0].get(col_expr.name)
            
            result.append(row_result)
        
        return result
    
    def _apply_aggregate(self, func: FunctionCall, rows: List[Dict]) -> Any:
        """Apply a single aggregate function."""
        name = func.name.upper()
        args = func.args
        
        if name == "COUNT":
            if args and isinstance(args[0], ColumnRef) and args[0].name == "*":
                return len(rows)
            elif args:
                # COUNT(column) - count non-null values
                col = args[0]
                if isinstance(col, ColumnRef):
                    return sum(1 for r in rows if r.get(col.name) is not None)
            return len(rows)
        
        elif name == "SUM":
            if args and isinstance(args[0], ColumnRef):
                col_name = args[0].name
                values = [r.get(col_name) for r in rows if r.get(col_name) is not None]
                if values:
                    return sum(values)
            return 0
        
        elif name == "AVG":
            if args and isinstance(args[0], ColumnRef):
                col_name = args[0].name
                values = [r.get(col_name) for r in rows if r.get(col_name) is not None]
                if values:
                    return sum(values) / len(values)
            return None
        
        elif name == "MIN":
            if args and isinstance(args[0], ColumnRef):
                col_name = args[0].name
                values = [r.get(col_name) for r in rows if r.get(col_name) is not None]
                if values:
                    return min(values)
            return None
        
        elif name == "MAX":
            if args and isinstance(args[0], ColumnRef):
                col_name = args[0].name
                values = [r.get(col_name) for r in rows if r.get(col_name) is not None]
                if values:
                    return max(values)
            return None
        
        return None
    
    def _try_index_scan(self, where_expr: Any, table: Table) -> bool:
        """
        Check if an index can be used for WHERE clause.
        
        Returns True only if:
        1. WHERE is equality on a column (column = value)
        2. An index exists for that column
        
        Currently, per-column indexes are not fully integrated,
        so this returns False unless explicitly registered.
        """
        # Check for equality pattern: column = value
        if not (isinstance(where_expr, BinaryOp) and where_expr.op == "="):
            return False
        
        if not isinstance(where_expr.left, ColumnRef):
            return False
        
        col_name = where_expr.left.name
        
        # Check if we have an index for this column
        # Indexes can be registered via _indexes dict
        index_key = f"{table.name}.{col_name}"
        if hasattr(self, '_indexes') and index_key in self._indexes:
            return True
        
        # Check if the column has an index stored in table
        # For now, no per-column indexes are auto-created
        # Return False to indicate seq_scan (honest reporting)
        return False
    
    def register_index(self, table_name: str, column: str, index):
        """Register an index for a table column."""
        if not hasattr(self, '_indexes'):
            self._indexes = {}
        self._indexes[f"{table_name}.{column}"] = index
    
    def _apply_join(self, left_rows: List[Dict], join: JoinClause, left_table_name: str) -> List[Dict]:
        """Apply a JOIN clause to rows."""
        right_table = self.schema.get_table(join.table.name)
        if right_table is None:
            return left_rows  # Table not found, skip join
        
        right_name = join.table.alias or join.table.name
        join_type = join.join_type
        on_expr = join.on
        
        result = []
        
        if join_type == "CROSS":
            # Cartesian product
            for left in left_rows:
                for right in right_table.rows:
                    row = dict(left)
                    for k, v in right.items():
                        row[f"{right_name}.{k}"] = v
                    result.append(row)
        
        elif join_type in ("INNER", "LEFT"):
            for left in left_rows:
                matched = False
                for right in right_table.rows:
                    row = dict(left)
                    for k, v in right.items():
                        row[f"{right_name}.{k}"] = v
                    
                    if on_expr is None or self._eval_condition(on_expr, row):
                        result.append(row)
                        matched = True
                
                # LEFT JOIN: include unmatched left rows
                if join_type == "LEFT" and not matched:
                    result.append(dict(left))
        
        elif join_type == "RIGHT":
            for right in right_table.rows:
                matched = False
                for left in left_rows:
                    row = dict(left)
                    for k, v in right.items():
                        row[f"{right_name}.{k}"] = v
                    
                    if on_expr is None or self._eval_condition(on_expr, row):
                        result.append(row)
                        matched = True
                
                # RIGHT JOIN: include unmatched right rows
                if not matched:
                    row = {}
                    for k, v in right.items():
                        row[f"{right_name}.{k}"] = v
                    result.append(row)
        
        return result
    
    # =========================================================================
    # Type Coercion
    # =========================================================================
    
    def _coerce_type(self, value: Any, col_name: str, table: Table) -> Any:
        """Coerce value to match column's declared type."""
        if value is None:
            return None
        
        # Find column definition
        col = table.get_column(col_name)
        if col is None:
            return value
        
        dtype = col.dtype.upper()
        
        try:
            if dtype == "INT":
                return int(value)
            elif dtype == "REAL":
                return float(value)
            elif dtype == "TEXT":
                return str(value)
            elif dtype == "BOOL":
                if isinstance(value, bool):
                    return value
                if isinstance(value, str):
                    return value.lower() in ('true', '1', 'yes')
                return bool(value)
            else:
                return value
        except (ValueError, TypeError):
            # If coercion fails, return original value
            return value
    
    # =========================================================================
    # Expression Evaluation
    # =========================================================================
    
    def _eval_expr(self, expr: Any, row: Optional[Dict] = None) -> Any:
        """Evaluate an expression to a value."""
        if isinstance(expr, Literal):
            return expr.value
        elif isinstance(expr, ColumnRef):
            if row is not None:
                # Try qualified name first (table.column)
                if expr.table:
                    qualified = f"{expr.table}.{expr.name}"
                    if qualified in row:
                        return row[qualified]
                # Try unqualified name
                return row.get(expr.name)
            return None
        elif isinstance(expr, BinaryOp):
            left = self._eval_expr(expr.left, row)
            right = self._eval_expr(expr.right, row)
            return self._eval_binary(expr.op, left, right)
        else:
            return None
    
    def _eval_binary(self, op: str, left: Any, right: Any) -> Any:
        """Evaluate binary operator."""
        if op == "+":
            return left + right
        elif op == "-":
            return left - right
        elif op == "*":
            return left * right
        elif op == "/":
            return left / right if right != 0 else None
        elif op == "=":
            return left == right
        elif op == "!=":
            return left != right
        elif op == "<":
            return left < right if left is not None and right is not None else False
        elif op == "<=":
            return left <= right if left is not None and right is not None else False
        elif op == ">":
            return left > right if left is not None and right is not None else False
        elif op == ">=":
            return left >= right if left is not None and right is not None else False
        elif op == "AND":
            return bool(left) and bool(right)
        elif op == "OR":
            return bool(left) or bool(right)
        else:
            return None
    
    def _eval_condition(self, expr: Any, row: Dict) -> bool:
        """Evaluate a WHERE condition (must return bool)."""
        result = self._eval_expr(expr, row)
        return bool(result) if result is not None else False
    
    # =========================================================================
    # Utility
    # =========================================================================
    
    def __repr__(self):
        return f"Engine({len(self.schema.list_tables())} tables)"
