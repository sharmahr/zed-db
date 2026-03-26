"""
Tests for Schema management layer.
"""

import pytest
from zed.schema import Column, Table, Schema


class TestColumn:
    """Tests for Column class."""
    
    def test_column_basic(self):
        """Basic column creation."""
        col = Column(name="id", dtype="INT")
        assert col.name == "id"
        assert col.dtype == "INT"
        assert col.nullable is True
        assert col.primary_key is False
    
    def test_column_full(self):
        """Column with all options."""
        col = Column(name="id", dtype="INT", nullable=False, primary_key=True)
        assert col.primary_key is True
        assert col.nullable is False
    
    def test_column_repr(self):
        """Column string representation."""
        col = Column(name="id", dtype="INT", primary_key=True)
        assert "id" in repr(col)
        assert "INT" in repr(col)
        assert "PK" in repr(col)


class TestTable:
    """Tests for Table class."""
    
    def test_table_creation(self):
        """Create a table."""
        cols = [Column("id", "INT", primary_key=True), Column("name", "TEXT")]
        table = Table(name="users", columns=cols)
        
        assert table.name == "users"
        assert len(table.columns) == 2
        assert len(table.rows) == 0
    
    def test_get_column(self):
        """Get column by name."""
        cols = [Column("id", "INT"), Column("name", "TEXT")]
        table = Table(name="users", columns=cols)
        
        assert table.get_column("id").name == "id"
        assert table.get_column("missing") is None
    
    def test_column_names(self):
        """Get list of column names."""
        cols = [Column("id", "INT"), Column("name", "TEXT")]
        table = Table(name="users", columns=cols)
        
        assert table.column_names() == ["id", "name"]
    
    def test_add_rows(self):
        """Add rows to table."""
        cols = [Column("id", "INT"), Column("name", "TEXT")]
        table = Table(name="users", columns=cols)
        
        table.rows.append({"id": 1, "name": "Alice"})
        table.rows.append({"id": 2, "name": "Bob"})
        
        assert len(table.rows) == 2


class TestSchema:
    """Tests for Schema class."""
    
    def test_schema_creation(self):
        """Create empty schema."""
        schema = Schema()
        assert len(schema.list_tables()) == 0
    
    def test_create_table(self):
        """Create a table in schema."""
        schema = Schema()
        cols = [Column("id", "INT")]
        table = Table("users", cols)
        
        assert schema.create_table(table) is True
        assert "users" in schema.list_tables()
    
    def test_create_duplicate_table(self):
        """Cannot create duplicate table."""
        schema = Schema()
        cols = [Column("id", "INT")]
        table = Table("users", cols)
        
        assert schema.create_table(table) is True
        assert schema.create_table(table) is False  # Duplicate
    
    def test_get_table(self):
        """Get table from schema."""
        schema = Schema()
        cols = [Column("id", "INT")]
        table = Table("users", cols)
        schema.create_table(table)
        
        got = schema.get_table("users")
        assert got.name == "users"
        assert schema.get_table("missing") is None
    
    def test_drop_table(self):
        """Drop table from schema."""
        schema = Schema()
        cols = [Column("id", "INT")]
        table = Table("users", cols)
        schema.create_table(table)
        
        assert schema.drop_table("users") is True
        assert schema.get_table("users") is None
        assert schema.drop_table("missing") is False
    
    def test_list_tables(self):
        """List all tables."""
        schema = Schema()
        schema.create_table(Table("a", [Column("x", "INT")]))
        schema.create_table(Table("b", [Column("y", "INT")]))
        
        tables = schema.list_tables()
        assert set(tables) == {"a", "b"}
