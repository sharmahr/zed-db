"""
Tests for Index layer (HashIndex and BTreeIndex).
"""

import pytest
from zed.index import HashIndex, BTreeIndex


class TestHashIndex:
    """Tests for HashIndex."""
    
    def test_insert_and_lookup(self):
        """Basic insert and lookup."""
        idx = HashIndex("id")
        idx.insert(1, 0)
        idx.insert(1, 1)
        idx.insert(2, 2)
        
        assert idx.lookup(1) == [0, 1]
        assert idx.lookup(2) == [2]
        assert idx.lookup(3) == []
    
    def test_rebuild(self):
        """Rebuild index from rows."""
        idx = HashIndex("name")
        rows = [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}, {"id": 3, "name": "A"}]
        
        idx.rebuild(rows, "name")
        assert set(idx.lookup("A")) == {0, 2}
        assert idx.lookup("B") == [1]
    
    def test_delete(self):
        """Delete entry."""
        idx = HashIndex("x")
        idx.insert(1, 0)
        idx.insert(1, 1)
        idx.delete(1, 0)
        
        assert idx.lookup(1) == [1]
    
    def test_len(self):
        """Length is number of unique keys."""
        idx = HashIndex("x")
        idx.insert(1, 0)
        idx.insert(1, 1)
        idx.insert(2, 2)
        
        assert len(idx) == 2


class TestBTreeIndex:
    """Tests for BTreeIndex."""
    
    def test_insert_and_lookup(self):
        """Basic insert and lookup."""
        idx = BTreeIndex("id")
        idx.insert(1, 0)
        idx.insert(2, 1)
        idx.insert(3, 2)
        
        assert idx.lookup(2) == [1]
    
    def test_insert_duplicates(self):
        """Insert with duplicate keys - stores all indices."""
        idx = BTreeIndex("col")
        idx.insert(1, 0)
        idx.insert(1, 1)  # Duplicate key
        idx.insert(1, 2)  # Another duplicate
        idx.insert(2, 3)
        
        # All three indices for key 1
        assert idx.lookup(1) == [0, 1, 2]
        assert idx.lookup(2) == [3]
    
    def test_range_query(self):
        """Range query."""
        idx = BTreeIndex("age")
        idx.insert(20, 0)
        idx.insert(25, 1)
        idx.insert(30, 2)
        idx.insert(35, 3)
        
        # Range 20-30
        indices = idx.range_query(start=20, end=30)
        assert set(indices) == {0, 1, 2}
    
    def test_range_query_open(self):
        """Open-ended range query."""
        idx = BTreeIndex("x")
        idx.insert(1, 0)
        idx.insert(2, 1)
        idx.insert(3, 2)
        
        # All >= 2
        indices = idx.range_query(start=2)
        assert set(indices) == {1, 2}
    
    def test_range_query_with_duplicates(self):
        """Range query includes all indices for matching keys."""
        idx = BTreeIndex("age")
        idx.insert(20, 0)
        idx.insert(20, 1)  # Duplicate key 20
        idx.insert(30, 2)
        
        indices = idx.range_query(start=20, end=20)
        assert set(indices) == {0, 1}  # Both indices for key 20
    
    def test_rebuild(self):
        """Rebuild from rows."""
        idx = BTreeIndex("age")
        rows = [{"id": 1, "age": 20}, {"id": 2, "age": 30}]
        
        idx.rebuild(rows, "age")
        assert len(idx) == 2
    
    def test_rebuild_with_duplicates(self):
        """Rebuild handles duplicate values."""
        idx = BTreeIndex("name")
        rows = [
            {"id": 1, "name": "A"},
            {"id": 2, "name": "A"},  # Duplicate
            {"id": 3, "name": "B"},
        ]
        
        idx.rebuild(rows, "name")
        assert set(idx.lookup("A")) == {0, 1}
        assert idx.lookup("B") == [2]
    
    def test_len(self):
        """Length is number of unique keys."""
        idx = BTreeIndex("x")
        idx.insert(1, 0)
        idx.insert(1, 1)  # Same key
        idx.insert(2, 2)
        
        assert len(idx) == 2  # 2 unique keys
    
    def test_repr(self):
        """String representation."""
        idx = BTreeIndex("x")
        idx.insert(1, 0)
        idx.insert(1, 1)
        
        r = repr(idx)
        assert "BTreeIndex" in r
        assert "x" in r
