"""
Zed Database - Schema Layer

Simple schema management following KISS and Single Responsibility.
Each class has one job:
- Column: Defines a column's name and type
- Table: Holds column definitions and rows
- Schema: Maps table names to Table objects
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


# =============================================================================
# Column - Single Responsibility: Define one column
# =============================================================================

@dataclass
class Column:
    """Defines a single column in a table."""
    name: str
    dtype: str  # "INT", "TEXT", "REAL", "BOOL"
    nullable: bool = True
    primary_key: bool = False
    
    def __repr__(self):
        parts = [self.name, self.dtype]
        if self.primary_key:
            parts.append("PK")
        if not self.nullable:
            parts.append("NOT NULL")
        return f"Column({' '.join(parts)})"


# =============================================================================
# Table - Single Responsibility: Hold table definition and data
# =============================================================================

@dataclass
class Table:
    """A table definition with columns and rows."""
    name: str
    columns: List[Column]
    rows: List[Dict[str, Any]] = field(default_factory=list)
    
    def get_column(self, name: str) -> Optional[Column]:
        """Get column by name."""
        for col in self.columns:
            if col.name == name:
                return col
        return None
    
    def column_names(self) -> List[str]:
        """Get list of column names."""
        return [c.name for c in self.columns]
    
    def __repr__(self):
        return f"Table({self.name}, {len(self.columns)} cols, {len(self.rows)} rows)"


# =============================================================================
# Schema - Single Responsibility: Manage all tables in database
# =============================================================================

class Schema:
    """Database schema: maps table names to Table objects."""
    
    def __init__(self):
        self._tables: Dict[str, Table] = {}
    
    def create_table(self, table: Table) -> bool:
        """Create a new table. Returns True if created, False if exists."""
        if table.name in self._tables:
            return False
        self._tables[table.name] = table
        return True
    
    def get_table(self, name: str) -> Optional[Table]:
        """Get table by name."""
        return self._tables.get(name)
    
    def drop_table(self, name: str) -> bool:
        """Drop a table. Returns True if dropped, False if not found."""
        if name in self._tables:
            del self._tables[name]
            return True
        return False
    
    def list_tables(self) -> List[str]:
        """List all table names."""
        return list(self._tables.keys())
    
    def __repr__(self):
        return f"Schema({len(self._tables)} tables: {', '.join(self._tables.keys())})"
