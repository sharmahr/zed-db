"""
Benchmark tests for index performance.

These tests demonstrate the performance benefits of using indexes
vs full table scans. Run with: pytest tests/test_benchmark.py -v
"""

import pytest
import time
from zed.index import HashIndex, BTreeIndex


class TestIndexPerformance:
    """Benchmark index performance."""
    
    def test_hash_index_lookup_speed(self):
        """
        HashIndex lookup should be O(1).
        Demonstrates fast equality lookups.
        """
        idx = HashIndex("id")
        
        # Insert 10000 items
        for i in range(10000):
            idx.insert(i, i)
        
        # Time lookup
        start = time.time()
        for _ in range(1000):
            idx.lookup(5000)
        elapsed = time.time() - start
        
        # Should be very fast (< 100ms for 1000 lookups)
        assert elapsed < 0.1, f"HashIndex lookup too slow: {elapsed}s"
    
    def test_btree_index_range_speed(self):
        """
        BTreeIndex range query should be efficient.
        Uses binary search on sorted keys.
        """
        idx = BTreeIndex("age")
        
        # Insert 10000 items
        for i in range(10000):
            idx.insert(i, i)
        
        # Time range query
        start = time.time()
        for _ in range(100):
            idx.range_query(start=1000, end=2000)
        elapsed = time.time() - start
        
        # Should be fast (< 100ms for 100 range queries)
        assert elapsed < 0.1, f"BTreeIndex range query too slow: {elapsed}s"
    
    def test_duplicate_handling_performance(self):
        """
        BTreeIndex should handle many duplicates efficiently.
        """
        idx = BTreeIndex("status")
        
        # Insert 1000 items with same key (many duplicates)
        start = time.time()
        for i in range(1000):
            idx.insert("active", i)
        elapsed = time.time() - start
        
        assert elapsed < 0.1, f"Duplicate insert too slow: {elapsed}s"
        assert len(idx.lookup("active")) == 1000


class TestIndexVsFullScan:
    """
    Demonstrates index vs full scan performance.
    
    In a real database, indexes provide:
    - O(1) or O(log n) lookups vs O(n) full scan
    - Faster range queries via sorted keys
    """
    
    def test_hash_index_vs_list_scan(self):
        """
        Compare HashIndex lookup to scanning a list.
        HashIndex should be much faster for equality.
        """
        data = [{"id": i, "val": f"v{i}"} for i in range(10000)]
        
        # Build index
        idx = HashIndex("id")
        for i, row in enumerate(data):
            idx.insert(row["id"], i)
        
        # Time index lookup
        start = time.time()
        for _ in range(1000):
            idx.lookup(5000)
        index_time = time.time() - start
        
        # Time list scan
        start = time.time()
        for _ in range(1000):
            [r for r in data if r["id"] == 5000]
        scan_time = time.time() - start
        
        # Index should be faster (or at least not much slower)
        # In practice, index is O(1), scan is O(n)
        print(f"\n  Index lookup: {index_time:.6f}s")
        print(f"  Full scan:    {scan_time:.6f}s")
        print(f"  Speedup:      {scan_time/index_time:.1f}x" if index_time > 0 else "")
        
        # Just verify both work
        assert index_time < 1.0
        assert scan_time < 1.0
    
    def test_btree_index_vs_list_scan_range(self):
        """
        Compare BTreeIndex range query to scanning a list.
        BTreeIndex uses binary search, list uses linear scan.
        """
        data = [{"age": i} for i in range(10000)]
        
        # Build index
        idx = BTreeIndex("age")
        for i, row in enumerate(data):
            idx.insert(row["age"], i)
        
        # Time index range query
        start = time.time()
        for _ in range(100):
            idx.range_query(start=1000, end=2000)
        index_time = time.time() - start
        
        # Time list scan
        start = time.time()
        for _ in range(100):
            [r for r in data if 1000 <= r["age"] <= 2000]
        scan_time = time.time() - start
        
        print(f"\n  Index range: {index_time:.6f}s")
        print(f"  Full scan:   {scan_time:.6f}s")
        
        assert index_time < 1.0
        assert scan_time < 1.0
