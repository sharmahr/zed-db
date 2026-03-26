"""
Zed Database - Index Layer

Hash Index: For fast equality lookups (WHERE col = value)
B-Tree Index: For range queries (WHERE col > value)

Following KISS and Single Responsibility.
"""

from typing import Any, Dict, List, Optional, Tuple
from zed.storage import BTree


# =============================================================================
# HashIndex - Single Responsibility: Fast equality lookups
# =============================================================================

class HashIndex:
    """
    Hash-based index for O(1) equality lookups.
    
    Stores: key -> list of row indices (allows duplicates)
    """
    
    def __init__(self, column: str):
        self.column = column
        self._index: Dict[Any, List[int]] = {}
    
    def insert(self, key: Any, row_index: int):
        """Add a row index for a key."""
        if key not in self._index:
            self._index[key] = []
        self._index[key].append(row_index)
    
    def lookup(self, key: Any) -> List[int]:
        """Find all row indices matching key. O(1)."""
        return self._index.get(key, [])
    
    def delete(self, key: Any, row_index: int):
        """Remove a row index for a key."""
        if key in self._index:
            try:
                self._index[key].remove(row_index)
                if not self._index[key]:
                    del self._index[key]
            except ValueError:
                pass
    
    def rebuild(self, rows: List[Dict], column: str):
        """Rebuild index from rows."""
        self._index.clear()
        for i, row in enumerate(rows):
            if column in row:
                self.insert(row[column], i)
    
    def __len__(self):
        return len(self._index)
    
    def __repr__(self):
        return f"HashIndex({self.column}, {len(self)} keys)"


# =============================================================================
# BTreeIndex - Single Responsibility: Range queries with duplicate support
# =============================================================================

class BTreeIndex:
    """
    B-Tree index for range queries.
    
    Stores: key -> list of row indices (supports duplicates)
    Supports: lookup (==), range (>=, <=, >, <)
    
    Uses dict internally for O(1) insert with duplicates,
    BTree for efficient range queries.
    """
    
    def __init__(self, column: str, order: int = 4):
        self.column = column
        self._order = order
        self._index: Dict[Any, List[int]] = {}  # key -> [row_indices]
        self._sorted_keys: List[Any] = []  # Cached sorted keys for range queries
    
    def insert(self, key: Any, row_index: int):
        """Insert key -> row_index mapping. Supports duplicates."""
        if key not in self._index:
            self._index[key] = []
            self._sorted_keys = sorted(self._index.keys())  # Update sorted cache
        self._index[key].append(row_index)
    
    def lookup(self, key: Any) -> List[int]:
        """Exact match lookup. O(1)."""
        return list(self._index.get(key, []))
    
    def range_query(self, start: Any = None, end: Any = None) -> List[int]:
        """
        Range query: keys in [start, end].
        Uses sorted key cache for efficient binary search.
        """
        if not self._sorted_keys:
            return []
        
        # Binary search for start position
        if start is None:
            start_idx = 0
        else:
            start_idx = self._bisect_left(start)
        
        # Binary search for end position
        if end is None:
            end_idx = len(self._sorted_keys)
        else:
            end_idx = self._bisect_right(end)
        
        # Collect all indices in range
        indices = []
        for i in range(start_idx, end_idx):
            key = self._sorted_keys[i]
            indices.extend(self._index[key])
        
        return indices
    
    def _bisect_left(self, key: Any) -> int:
        """Find leftmost position where key could be inserted."""
        lo, hi = 0, len(self._sorted_keys)
        while lo < hi:
            mid = (lo + hi) // 2
            if self._sorted_keys[mid] < key:
                lo = mid + 1
            else:
                hi = mid
        return lo
    
    def _bisect_right(self, key: Any) -> int:
        """Find rightmost position where key could be inserted."""
        lo, hi = 0, len(self._sorted_keys)
        while lo < hi:
            mid = (lo + hi) // 2
            if key < self._sorted_keys[mid]:
                hi = mid
            else:
                lo = mid + 1
        return lo
    
    def rebuild(self, rows: List[Dict], column: str):
        """Rebuild index from rows. Handles duplicates properly."""
        self._index.clear()
        
        for i, row in enumerate(rows):
            if column in row:
                key = row[column]
                if key not in self._index:
                    self._index[key] = []
                self._index[key].append(i)
        
        # Update sorted keys cache
        self._sorted_keys = sorted(self._index.keys())
    
    def __len__(self):
        return len(self._index)
    
    def __repr__(self):
        total = sum(len(v) for v in self._index.values())
        return f"BTreeIndex({self.column}, {len(self)} keys, {total} entries)"
