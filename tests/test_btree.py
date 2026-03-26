"""
Tests for B-Tree storage engine.

Tests cover:
- Basic insert and search
- Split operations (bug fix for promoted key value loss)
- Range scans
- Edge cases
"""

import pytest
from zed.storage import BTree, BTreeNode


class TestBTreeNode:
    """Tests for BTreeNode class."""
    
    def test_node_creation(self):
        """Test creating an empty node."""
        node = BTreeNode()
        assert node.keys == []
        assert node.values == []
        assert node.children == []
        assert node.is_leaf is True
    
    def test_is_full(self):
        """Test is_full check."""
        node = BTreeNode()
        assert node.is_full(4) is False  # order 4, max 3 keys
        node.keys = [1, 2]
        assert node.is_full(4) is False
        node.keys = [1, 2, 3]
        assert node.is_full(4) is True


class TestBTreeBasic:
    """Basic B-Tree operations."""
    
    def test_insert_and_search_single(self):
        """Insert and search single item."""
        bt = BTree(order=4)
        assert bt.insert(1, "A") is True
        assert bt.search(1) == "A"
        assert bt.search(2) is None
    
    def test_insert_duplicate_rejected(self):
        """Duplicate keys are rejected."""
        bt = BTree(order=4)
        assert bt.insert(1, "A") is True
        assert bt.insert(1, "B") is False  # Duplicate
        assert bt.search(1) == "A"  # Original value preserved
    
    def test_multiple_inserts(self):
        """Insert multiple items."""
        bt = BTree(order=4)
        for i in range(10):
            bt.insert(i, f"val{i}")
        
        for i in range(10):
            assert bt.search(i) == f"val{i}"


class TestBTreeSplit:
    """Tests for B-Tree split operations (bug fix)."""
    
    def test_split_preserves_all_values(self):
        """
        When B-Tree splits, promoted key values must be preserved.
        This was the bug: mid key's value was lost during split.
        """
        bt = BTree(order=3)  # Max 2 keys per node, forces early splits
        
        bt.insert(1, "A")
        bt.insert(2, "B")
        bt.insert(3, "C")
        
        # All values must be accessible
        assert bt.search(1) == "A"
        assert bt.search(2) == "B"  # This was None before fix!
        assert bt.search(3) == "C"
        
        # all_items should return all 3
        items = bt.all_items()
        assert len(items) == 3
        keys = [k for k, v in items]
        assert keys == [1, 2, 3]
    
    def test_split_preserves_values_order(self):
        """Split preserves key-value association in order."""
        bt = BTree(order=3)
        
        bt.insert(1, "one")
        bt.insert(2, "two")
        bt.insert(3, "three")
        
        items = bt.all_items()
        assert items == [(1, "one"), (2, "two"), (3, "three")]
    
    def test_multiple_splits(self):
        """Multiple splits preserve all data."""
        bt = BTree(order=3)
        
        for i in range(10):
            bt.insert(i, f"val{i}")
        
        # All 10 items must be searchable
        for i in range(10):
            assert bt.search(i) == f"val{i}", f"Missing key {i}"
        
        assert len(bt.all_items()) == 10


class TestBTreeScan:
    """Tests for range scan operations."""
    
    def test_scan_all(self):
        """Scan all items."""
        bt = BTree(order=4)
        for i in range(5):
            bt.insert(i, f"v{i}")
        
        results = bt.scan()
        assert len(results) == 5
    
    def test_scan_with_bounds(self):
        """Scan with start/end bounds."""
        bt = BTree(order=4)
        for i in range(10):
            bt.insert(i, f"v{i}")
        
        # Scan 3 to 7
        results = bt.scan(start=3, end=7)
        keys = [k for k, v in results]
        assert keys == [3, 4, 5, 6, 7]
    
    def test_scan_empty_range(self):
        """Scan empty range."""
        bt = BTree(order=4)
        bt.insert(5, "five")
        
        results = bt.scan(start=1, end=3)
        assert results == []


class TestBTreeEdgeCases:
    """Edge cases."""
    
    def test_empty_tree(self):
        """Empty tree behavior."""
        bt = BTree()
        assert len(bt) == 0
        assert bt.search(1) is None
        assert bt.all_items() == []
    
    def test_single_item(self):
        """Single item tree."""
        bt = BTree(order=4)
        bt.insert(42, "answer")
        
        assert bt.search(42) == "answer"
        assert bt.search(99) is None
        assert len(bt) == 1
    
    def test_string_keys(self):
        """String keys work."""
        bt = BTree(order=4)
        bt.insert("apple", 1)
        bt.insert("banana", 2)
        bt.insert("cherry", 3)
        
        assert bt.search("banana") == 2
    
    def test_large_order(self):
        """Large order tree."""
        bt = BTree(order=100)
        for i in range(50):
            bt.insert(i, f"v{i}")
        
        assert len(bt) == 50
        for i in range(50):
            assert bt.search(i) == f"v{i}"


class TestBTreeDiskPersistence:
    """Tests for B-Tree disk persistence."""
    
    def test_save_and_load(self, tmp_path):
        """Save B-Tree to disk and load it back."""
        import os
        
        tree = BTree(order=4)
        tree.insert(1, {"name": "Alice"})
        tree.insert(2, {"name": "Bob"})
        
        filepath = os.path.join(tmp_path, "test.btree")
        tree.save(filepath)
        
        loaded = BTree.load(filepath)
        assert loaded.search(1) == {"name": "Alice"}
        assert loaded.search(2) == {"name": "Bob"}
    
    def test_load_nonexistent_file(self, tmp_path):
        """Loading nonexistent file returns empty tree."""
        import os
        
        filepath = os.path.join(tmp_path, "nonexistent.btree")
        tree = BTree.load(filepath)
        assert len(tree) == 0
    
    def test_save_json_and_load_json(self, tmp_path):
        """Save/load B-Tree as JSON."""
        import os
        
        tree = BTree(order=4)
        tree.insert(1, "A")
        tree.insert(2, "B")
        
        filepath = os.path.join(tmp_path, "test.json")
        tree.save_json(filepath)
        
        loaded = BTree.load_json(filepath)
        assert loaded.search(1) == "A"
        assert loaded.search(2) == "B"
    
    def test_persistence_preserves_order(self, tmp_path):
        """Disk persistence preserves B-Tree order."""
        import os
        
        tree = BTree(order=4)
        for i in range(10):
            tree.insert(i, i * 2)
        
        filepath = os.path.join(tmp_path, "test_order.btree")
        tree.save(filepath)
        
        loaded = BTree.load(filepath)
        assert len(loaded) == 10
        assert loaded.search(5) == 10
