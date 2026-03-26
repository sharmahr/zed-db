"""
Zed Database - B-Tree Storage

Simple B-Tree implementation following KISS and Single Responsibility.
- BTreeNode: One node in the tree (internal or leaf)
- BTree: The tree structure with insert/search/scan operations
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


# =============================================================================
# BTreeNode - Single Responsibility: Hold keys and children/values
# =============================================================================

@dataclass
class BTreeNode:
    """A node in the B-Tree. Leaf nodes hold values, internal nodes hold children."""
    keys: List[Any] = field(default_factory=list)
    # For leaf: values[i] is value for keys[i]
    # For internal: children[i] is subtree for keys < keys[i], children[-1] for >= keys[-1]
    values: List[Any] = field(default_factory=list)  # Only for leaf nodes
    children: List["BTreeNode"] = field(default_factory=list)  # Only for internal
    is_leaf: bool = True
    
    def is_full(self, order: int) -> bool:
        """Check if node has max keys (order-1)."""
        return len(self.keys) >= order - 1


# =============================================================================
# BTree - Single Responsibility: Tree operations (insert, search, scan)
# =============================================================================

class BTree:
    """
    Simple B-Tree for in-memory storage.
    
    - order: max children per node (default 4 → 3 keys max per node)
    - Keys are sorted, duplicates not allowed
    """
    
    def __init__(self, order: int = 4):
        self.order = order  # B-Tree order (max children per node)
        self.root = BTreeNode(is_leaf=True)
    
    def insert(self, key: Any, value: Any) -> bool:
        """Insert key-value. Returns True if inserted, False if key exists."""
        # Check if key exists
        if self.search(key) is not None:
            return False
        
        # If root is full, split it
        if self.root.is_full(self.order):
            new_root = BTreeNode(is_leaf=False)
            new_root.children.append(self.root)
            self._split_child(new_root, 0)
            self.root = new_root
        
        self._insert_nonfull(self.root, key, value)
        return True
    
    def _insert_nonfull(self, node: BTreeNode, key: Any, value: Any):
        """Insert into a non-full node."""
        if node.is_leaf:
            # Find position and insert
            i = len(node.keys) - 1
            node.keys.append(None)
            node.values.append(None)
            
            while i >= 0 and key < node.keys[i]:
                node.keys[i + 1] = node.keys[i]
                node.values[i + 1] = node.values[i]
                i -= 1
            
            node.keys[i + 1] = key
            node.values[i + 1] = value
        else:
            # Find child to descend into
            i = len(node.keys) - 1
            while i >= 0 and key < node.keys[i]:
                i -= 1
            i += 1
            
            # Split child if full
            if node.children[i].is_full(self.order):
                self._split_child(node, i)
                if key > node.keys[i]:
                    i += 1
            
            self._insert_nonfull(node.children[i], key, value)
    
    def _split_child(self, parent: BTreeNode, index: int):
        """
        Split a full child at index into two nodes (B+Tree style).
        
        For leaf nodes: mid key stays in right child with its value.
        For internal nodes: mid key is separator, children[mid+1:] go right.
        """
        full_child = parent.children[index]
        mid = len(full_child.keys) // 2
        
        # Create new right sibling
        new_child = BTreeNode(is_leaf=full_child.is_leaf)
        
        # Save mid key before truncating
        mid_key = full_child.keys[mid]
        
        if full_child.is_leaf:
            # B+Tree: mid key stays in right child with its value
            new_child.keys = full_child.keys[mid:]
            new_child.values = full_child.values[mid:]
            full_child.keys = full_child.keys[:mid]
            full_child.values = full_child.values[:mid]
        else:
            # Internal node: mid key is separator, move children after it
            new_child.keys = full_child.keys[mid + 1:]
            new_child.children = full_child.children[mid + 1:]
            full_child.keys = full_child.keys[:mid]
            full_child.children = full_child.children[:mid + 1]
        
        # Insert mid key and new child into parent
        parent.keys.insert(index, mid_key)
        parent.children.insert(index + 1, new_child)
    
    def search(self, key: Any) -> Optional[Any]:
        """Search for key. Returns value or None."""
        return self._search_node(self.root, key)
    
    def _search_node(self, node: BTreeNode, key: Any) -> Optional[Any]:
        """Search in a node recursively (B+Tree style)."""
        if node.is_leaf:
            # Leaf: exact match on key
            for i, k in enumerate(node.keys):
                if k == key:
                    return node.values[i]
            return None
        else:
            # Internal: find child to descend
            i = 0
            while i < len(node.keys) and key >= node.keys[i]:
                i += 1
            return self._search_node(node.children[i], key)
    
    def scan(self, start: Any = None, end: Any = None) -> List[Tuple[Any, Any]]:
        """Range scan: return [(key, value), ...] where start <= key <= end."""
        results = []
        self._scan_node(self.root, start, end, results)
        return results
    
    def _scan_node(self, node: BTreeNode, start: Any, end: Any, results: List):
        """Scan a node recursively."""
        if node.is_leaf:
            for i, key in enumerate(node.keys):
                if (start is None or key >= start) and (end is None or key <= end):
                    results.append((key, node.values[i]))
        else:
            for i, key in enumerate(node.keys):
                # Visit left child
                if start is None or key >= start:
                    self._scan_node(node.children[i], start, end, results)
                # Visit this key
                if (start is None or key >= start) and (end is None or key <= end):
                    # Keys are in children, not stored in internal nodes
                    pass
            # Visit rightmost child
            if end is None or (node.keys and end >= node.keys[-1]):
                self._scan_node(node.children[-1], start, end, results)
    
    def all_items(self) -> List[Tuple[Any, Any]]:
        """Return all key-value pairs in order."""
        return self.scan()
    
    def __len__(self) -> int:
        """Count total items."""
        return len(self.all_items())
    
    def __repr__(self):
        return f"BTree({len(self)} items, order={self.order})"
    
    # =========================================================================
    # Disk Persistence
    # =========================================================================
    
    def save(self, filepath: str) -> None:
        """Save B-Tree to disk file for persistence."""
        import pickle
        # Serialize all items
        data = {
            'order': self.order,
            'items': self.all_items()
        }
        with open(filepath, 'wb') as f:
            pickle.dump(data, f)
    
    @classmethod
    def load(cls, filepath: str, order: int = 4) -> "BTree":
        """Load B-Tree from disk file."""
        import pickle
        import os
        if not os.path.exists(filepath):
            return cls(order=order)
        
        with open(filepath, 'rb') as f:
            data = pickle.load(f)
        
        tree = cls(order=data.get('order', order))
        for key, value in data.get('items', []):
            tree.insert(key, value)
        return tree
    
    def save_json(self, filepath: str) -> None:
        """Save B-Tree to JSON file (human readable)."""
        import json
        data = {
            'order': self.order,
            'items': [[k, v] for k, v in self.all_items()]
        }
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
    
    @classmethod
    def load_json(cls, filepath: str, order: int = 4) -> "BTree":
        """Load B-Tree from JSON file."""
        import json
        import os
        if not os.path.exists(filepath):
            return cls(order=order)
        
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        tree = cls(order=data.get('order', order))
        for key, value in data.get('items', []):
            tree.insert(key, value)
        return tree
