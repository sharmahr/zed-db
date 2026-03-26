"""
Tests for Execution Engine.
"""

import pytest
from zed.sql import Parser
from zed.engine import Engine


class TestEngineCreateTable:
    """Tests for CREATE TABLE execution."""
    
    def test_create_table(self):
        """Create a table."""
        engine = Engine()
        stmt = Parser("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)").parse()[0]
        
        result = engine.execute(stmt)
        assert result["status"] == "ok"
        assert "users" in engine.schema.list_tables()
    
    def test_create_duplicate_table(self):
        """Cannot create duplicate table."""
        engine = Engine()
        stmt = Parser("CREATE TABLE users (id INT)").parse()[0]
        engine.execute(stmt)
        
        stmt2 = Parser("CREATE TABLE users (id INT)").parse()[0]
        result = engine.execute(stmt2)
        assert result["status"] == "error"


class TestEngineInsert:
    """Tests for INSERT execution."""
    
    def test_insert_row(self):
        """Insert a row."""
        engine = Engine()
        Parser("CREATE TABLE users (id INT, name TEXT)").parse()[0]
        engine.execute(Parser("CREATE TABLE users (id INT, name TEXT)").parse()[0])
        
        stmt = Parser("INSERT INTO users (id, name) VALUES (1, 'Alice')").parse()[0]
        result = engine.execute(stmt)
        
        assert result["status"] == "ok"
        assert result["message"] == "Inserted 1 row(s)"
    
    def test_insert_multiple_rows(self):
        """Insert multiple rows."""
        engine = Engine()
        engine.execute(Parser("CREATE TABLE users (id INT, name TEXT)").parse()[0])
        
        stmt = Parser("INSERT INTO users (id, name) VALUES (1, 'A'), (2, 'B')").parse()[0]
        result = engine.execute(stmt)
        
        assert result["status"] == "ok"
    
    def test_insert_into_missing_table(self):
        """Insert into non-existent table fails."""
        engine = Engine()
        stmt = Parser("INSERT INTO missing (id) VALUES (1)").parse()[0]
        result = engine.execute(stmt)
        
        assert result["status"] == "error"


class TestEngineSelect:
    """Tests for SELECT execution."""
    
    def test_select_all(self):
        """Select all rows."""
        engine = Engine()
        engine.execute(Parser("CREATE TABLE users (id INT, name TEXT)").parse()[0])
        engine.execute(Parser("INSERT INTO users (id, name) VALUES (1, 'Alice')").parse()[0])
        
        stmt = Parser("SELECT * FROM users").parse()[0]
        result = engine.execute(stmt)
        
        assert result["status"] == "ok"
        assert result["count"] == 1
        assert result["rows"][0]["name"] == "Alice"
    
    def test_select_with_where(self):
        """Select with WHERE clause."""
        engine = Engine()
        engine.execute(Parser("CREATE TABLE users (id INT, age INT)").parse()[0])
        engine.execute(Parser("INSERT INTO users (id, age) VALUES (1, 30)").parse()[0])
        engine.execute(Parser("INSERT INTO users (id, age) VALUES (2, 20)").parse()[0])
        
        stmt = Parser("SELECT * FROM users WHERE age > 25").parse()[0]
        result = engine.execute(stmt)
        
        assert result["count"] == 1
        assert result["rows"][0]["id"] == 1
    
    def test_select_from_missing_table(self):
        """Select from non-existent table."""
        engine = Engine()
        stmt = Parser("SELECT * FROM missing").parse()[0]
        result = engine.execute(stmt)
        
        assert result["status"] == "error"
    
    def test_select_empty_table(self):
        """Select from empty table."""
        engine = Engine()
        engine.execute(Parser("CREATE TABLE users (id INT)").parse()[0])
        
        stmt = Parser("SELECT * FROM users").parse()[0]
        result = engine.execute(stmt)
        
        assert result["status"] == "ok"
        assert result["count"] == 0


class TestEngineExpressionEval:
    """Tests for expression evaluation."""
    
    def test_comparison_operators(self):
        """Test comparison operators in WHERE."""
        engine = Engine()
        engine.execute(Parser("CREATE TABLE t (x INT)").parse()[0])
        engine.execute(Parser("INSERT INTO t (x) VALUES (5)").parse()[0])
        
        # Test >
        result = engine.execute(Parser("SELECT * FROM t WHERE x > 3").parse()[0])
        assert result["count"] == 1
        
        # Test <
        result = engine.execute(Parser("SELECT * FROM t WHERE x < 3").parse()[0])
        assert result["count"] == 0
        
        # Test =
        result = engine.execute(Parser("SELECT * FROM t WHERE x = 5").parse()[0])
        assert result["count"] == 1
    
    def test_and_or_conditions(self):
        """Test AND/OR in WHERE."""
        engine = Engine()
        engine.execute(Parser("CREATE TABLE t (x INT, y INT)").parse()[0])
        engine.execute(Parser("INSERT INTO t (x, y) VALUES (1, 10)").parse()[0])
        engine.execute(Parser("INSERT INTO t (x, y) VALUES (2, 20)").parse()[0])
        
        # Test AND
        result = engine.execute(Parser("SELECT * FROM t WHERE x = 1 AND y = 10").parse()[0])
        assert result["count"] == 1
        
        # Test OR
        result = engine.execute(Parser("SELECT * FROM t WHERE x = 1 OR x = 2").parse()[0])
        assert result["count"] == 2
    
    def test_projection_specific_columns(self):
        """Test SELECT with specific columns."""
        engine = Engine()
        engine.execute(Parser("CREATE TABLE t (id INT, name TEXT, age INT)").parse()[0])
        engine.execute(Parser("INSERT INTO t (id, name, age) VALUES (1, 'A', 20)").parse()[0])
        
        result = engine.execute(Parser("SELECT name, age FROM t").parse()[0])
        assert result["count"] == 1
        assert "name" in result["columns"]
        assert "age" in result["columns"]


class TestEngineJoin:
    """Tests for JOIN operations."""
    
    def test_cross_join(self):
        """Test CROSS JOIN (cartesian product)."""
        engine = Engine()
        engine.execute(Parser("CREATE TABLE a (x INT)").parse()[0])
        engine.execute(Parser("CREATE TABLE b (y INT)").parse()[0])
        engine.execute(Parser("INSERT INTO a (x) VALUES (1), (2)").parse()[0])
        engine.execute(Parser("INSERT INTO b (y) VALUES (10), (20)").parse()[0])
        
        result = engine.execute(Parser("SELECT * FROM a CROSS JOIN b").parse()[0])
        assert result["count"] == 4  # 2 x 2
    
    def test_inner_join(self):
        """Test INNER JOIN with ON condition."""
        engine = Engine()
        engine.execute(Parser("CREATE TABLE users (id INT, name TEXT)").parse()[0])
        engine.execute(Parser("CREATE TABLE orders (id INT, user_id INT)").parse()[0])
        engine.execute(Parser("INSERT INTO users (id, name) VALUES (1, 'Alice')").parse()[0])
        engine.execute(Parser("INSERT INTO users (id, name) VALUES (2, 'Bob')").parse()[0])
        engine.execute(Parser("INSERT INTO orders (id, user_id) VALUES (1, 1)").parse()[0])
        engine.execute(Parser("INSERT INTO orders (id, user_id) VALUES (2, 1)").parse()[0])
        engine.execute(Parser("INSERT INTO orders (id, user_id) VALUES (3, 2)").parse()[0])
        
        result = engine.execute(Parser("SELECT * FROM users JOIN orders ON users.id = orders.user_id").parse()[0])
        assert result["count"] == 3  # Alice has 2, Bob has 1
    
    def test_left_join(self):
        """Test LEFT JOIN includes unmatched left rows."""
        engine = Engine()
        engine.execute(Parser("CREATE TABLE a (id INT)").parse()[0])
        engine.execute(Parser("CREATE TABLE b (id INT, val INT)").parse()[0])
        engine.execute(Parser("INSERT INTO a (id) VALUES (1), (2)").parse()[0])
        engine.execute(Parser("INSERT INTO b (id, val) VALUES (1, 100)").parse()[0])
        
        result = engine.execute(Parser("SELECT * FROM a LEFT JOIN b ON a.id = b.id").parse()[0])
        assert result["count"] == 2  # Both left rows included
    
    def test_right_join(self):
        """Test RIGHT JOIN includes unmatched right rows."""
        engine = Engine()
        engine.execute(Parser("CREATE TABLE a (id INT)").parse()[0])
        engine.execute(Parser("CREATE TABLE b (id INT)").parse()[0])
        engine.execute(Parser("INSERT INTO a (id) VALUES (1)").parse()[0])
        engine.execute(Parser("INSERT INTO b (id) VALUES (1), (2)").parse()[0])
        
        result = engine.execute(Parser("SELECT * FROM a RIGHT JOIN b ON a.id = b.id").parse()[0])
        assert result["count"] == 2  # Both right rows included


class TestEngineIndexTracking:
    """Tests for index usage tracking (scan_method)."""
    
    def test_scan_method_seq_scan_default(self):
        """Default scan method is seq_scan."""
        engine = Engine()
        engine.execute(Parser("CREATE TABLE users (id INT, age INT)").parse()[0])
        engine.execute(Parser("INSERT INTO users (id, age) VALUES (1, 30)").parse()[0])
        
        result = engine.execute(Parser("SELECT * FROM users").parse()[0])
        assert result["scan_method"] == "seq_scan"
    
    def test_scan_method_seq_scan_with_where_no_index(self):
        """Seq scan when no index registered for WHERE column."""
        engine = Engine()
        engine.execute(Parser("CREATE TABLE users (id INT, age INT)").parse()[0])
        engine.execute(Parser("INSERT INTO users (id, age) VALUES (1, 30)").parse()[0])
        engine.execute(Parser("INSERT INTO users (id, age) VALUES (2, 20)").parse()[0])
        
        result = engine.execute(Parser("SELECT * FROM users WHERE age = 30").parse()[0])
        # No index registered, so seq_scan
        assert result["scan_method"] == "seq_scan"
    
    def test_scan_method_index_scan_with_registered_index(self):
        """Index scan when index is registered."""
        engine = Engine()
        engine.execute(Parser("CREATE TABLE users (id INT, age INT)").parse()[0])
        engine.execute(Parser("INSERT INTO users (id, age) VALUES (1, 30)").parse()[0])
        engine.execute(Parser("INSERT INTO users (id, age) VALUES (2, 20)").parse()[0])
        
        # Register a fake index
        engine.register_index("users", "age", {"type": "hash"})
        
        result = engine.execute(Parser("SELECT * FROM users WHERE age = 30").parse()[0])
        assert result["scan_method"] == "index_scan"


class TestEngineAggregations:
    """Tests for aggregate functions (COUNT, SUM, AVG, MIN, MAX, GROUP BY)."""
    
    def test_count_star(self):
        """COUNT(*) counts all rows."""
        engine = Engine()
        engine.execute(Parser("CREATE TABLE users (id INT)").parse()[0])
        engine.execute(Parser("INSERT INTO users (id) VALUES (1), (2), (3)").parse()[0])
        
        result = engine.execute(Parser("SELECT COUNT(*) FROM users").parse()[0])
        assert result["status"] == "ok"
        assert result["rows"][0]["COUNT"] == 3
    
    def test_count_column(self):
        """COUNT(column) counts non-NULL values."""
        engine = Engine()
        engine.execute(Parser("CREATE TABLE users (id INT, name TEXT)").parse()[0])
        engine.execute(Parser("INSERT INTO users (id, name) VALUES (1, 'A')").parse()[0])
        engine.execute(Parser("INSERT INTO users (id, name) VALUES (2, NULL)").parse()[0])
        engine.execute(Parser("INSERT INTO users (id, name) VALUES (3, 'C')").parse()[0])
        
        result = engine.execute(Parser("SELECT COUNT(name) FROM users").parse()[0])
        assert result["rows"][0]["COUNT"] == 2  # Only 2 non-NULL
    
    def test_sum(self):
        """SUM aggregates numeric values."""
        engine = Engine()
        engine.execute(Parser("CREATE TABLE t (x INT)").parse()[0])
        engine.execute(Parser("INSERT INTO t (x) VALUES (10), (20), (30)").parse()[0])
        
        result = engine.execute(Parser("SELECT SUM(x) FROM t").parse()[0])
        assert result["rows"][0]["SUM"] == 60
    
    def test_avg(self):
        """AVG computes arithmetic mean."""
        engine = Engine()
        engine.execute(Parser("CREATE TABLE t (x INT)").parse()[0])
        engine.execute(Parser("INSERT INTO t (x) VALUES (10), (20), (30)").parse()[0])
        
        result = engine.execute(Parser("SELECT AVG(x) FROM t").parse()[0])
        assert abs(result["rows"][0]["AVG"] - 20.0) < 0.01
    
    def test_min(self):
        """MIN returns minimum value."""
        engine = Engine()
        engine.execute(Parser("CREATE TABLE t (x INT)").parse()[0])
        engine.execute(Parser("INSERT INTO t (x) VALUES (10), (5), (30)").parse()[0])
        
        result = engine.execute(Parser("SELECT MIN(x) FROM t").parse()[0])
        assert result["rows"][0]["MIN"] == 5
    
    def test_max(self):
        """MAX returns maximum value."""
        engine = Engine()
        engine.execute(Parser("CREATE TABLE t (x INT)").parse()[0])
        engine.execute(Parser("INSERT INTO t (x) VALUES (10), (5), (30)").parse()[0])
        
        result = engine.execute(Parser("SELECT MAX(x) FROM t").parse()[0])
        assert result["rows"][0]["MAX"] == 30
    
    def test_group_by_single_column(self):
        """GROUP BY groups rows by column."""
        engine = Engine()
        engine.execute(Parser("CREATE TABLE users (id INT, age INT)").parse()[0])
        engine.execute(Parser("INSERT INTO users (id, age) VALUES (1, 30)").parse()[0])
        engine.execute(Parser("INSERT INTO users (id, age) VALUES (2, 25)").parse()[0])
        engine.execute(Parser("INSERT INTO users (id, age) VALUES (3, 30)").parse()[0])
        
        result = engine.execute(Parser("SELECT age, COUNT(*) FROM users GROUP BY age").parse()[0])
        assert result["status"] == "ok"
        # Should have 2 groups: age 30 (count 2) and age 25 (count 1)
        assert result["count"] == 2
    
    def test_group_by_with_sum(self):
        """GROUP BY with SUM aggregate."""
        engine = Engine()
        engine.execute(Parser("CREATE TABLE sales (product TEXT, amount INT)").parse()[0])
        engine.execute(Parser("INSERT INTO sales (product, amount) VALUES ('A', 100)").parse()[0])
        engine.execute(Parser("INSERT INTO sales (product, amount) VALUES ('A', 200)").parse()[0])
        engine.execute(Parser("INSERT INTO sales (product, amount) VALUES ('B', 50)").parse()[0])
        
        result = engine.execute(Parser("SELECT product, SUM(amount) FROM sales GROUP BY product").parse()[0])
        assert result["count"] == 2
        # Find product A
        for row in result["rows"]:
            if row["product"] == "A":
                assert row["SUM"] == 300
            elif row["product"] == "B":
                assert row["SUM"] == 50
    
    def test_aggregate_empty_table(self):
        """Aggregates on empty table."""
        engine = Engine()
        engine.execute(Parser("CREATE TABLE t (x INT)").parse()[0])
        
        result = engine.execute(Parser("SELECT COUNT(*) FROM t").parse()[0])
        assert result["status"] == "ok"
        # Empty table, COUNT returns 0
        assert result["rows"][0]["COUNT"] == 0


class TestEngineTransactions:
    """Tests for transaction support (BEGIN, COMMIT, ROLLBACK)."""
    
    def test_begin_transaction(self):
        """BEGIN starts a transaction."""
        engine = Engine(wal_path="/tmp/test_wal1.wal")
        engine.execute(Parser("CREATE TABLE t (x INT)").parse()[0])
        
        result = engine.execute(Parser("BEGIN").parse()[0])
        assert result["status"] == "ok"
        assert "Transaction" in result["message"]
        assert engine.wal.in_transaction() is True
    
    def test_commit_transaction(self):
        """COMMIT ends a transaction."""
        engine = Engine(wal_path="/tmp/test_wal2.wal")
        engine.execute(Parser("CREATE TABLE t (x INT)").parse()[0])
        
        engine.execute(Parser("BEGIN").parse()[0])
        result = engine.execute(Parser("COMMIT").parse()[0])
        assert result["status"] == "ok"
        assert "committed" in result["message"]
        assert engine.wal.in_transaction() is False
    
    def test_rollback_transaction(self):
        """ROLLBACK ends a transaction."""
        engine = Engine(wal_path="/tmp/test_wal3.wal")
        engine.execute(Parser("CREATE TABLE t (x INT)").parse()[0])
        
        engine.execute(Parser("BEGIN").parse()[0])
        result = engine.execute(Parser("ROLLBACK").parse()[0])
        assert result["status"] == "ok"
        assert "rolled back" in result["message"]
        assert engine.wal.in_transaction() is False
    
    def test_nested_transaction_error(self):
        """Cannot start nested transaction."""
        engine = Engine(wal_path="/tmp/test_wal4.wal")
        engine.execute(Parser("CREATE TABLE t (x INT)").parse()[0])
        
        engine.execute(Parser("BEGIN").parse()[0])
        result = engine.execute(Parser("BEGIN").parse()[0])
        assert result["status"] == "error"
        assert "Already in a transaction" in result["message"]
    
    def test_commit_without_transaction(self):
        """COMMIT without BEGIN fails."""
        engine = Engine(wal_path="/tmp/test_wal5.wal")
        
        result = engine.execute(Parser("COMMIT").parse()[0])
        assert result["status"] == "error"
        assert "Not in a transaction" in result["message"]
    
    def test_rollback_without_transaction(self):
        """ROLLBACK without BEGIN fails."""
        engine = Engine(wal_path="/tmp/test_wal6.wal")
        
        result = engine.execute(Parser("ROLLBACK").parse()[0])
        assert result["status"] == "error"
        assert "Not in a transaction" in result["message"]
    
    def test_rollback_reverts_data(self):
        """ROLLBACK actually reverts in-memory data changes."""
        engine = Engine(wal_path="/tmp/test_wal_rollback_data.wal")
        engine.execute(Parser("CREATE TABLE users (id INT, name TEXT)").parse()[0])
        engine.execute(Parser("INSERT INTO users (id, name) VALUES (1, 'Alice')").parse()[0])
        
        # Start transaction
        engine.execute(Parser("BEGIN").parse()[0])
        
        # Insert in transaction
        engine.execute(Parser("INSERT INTO users (id, name) VALUES (2, 'Bob')").parse()[0])
        
        # Verify data has both rows
        result = engine.execute(Parser("SELECT * FROM users").parse()[0])
        assert result["count"] == 2
        
        # Rollback
        engine.execute(Parser("ROLLBACK").parse()[0])
        
        # Verify data reverted to original (only Alice)
        result = engine.execute(Parser("SELECT * FROM users").parse()[0])
        assert result["count"] == 1
        assert result["rows"][0]["id"] == 1
    
    def test_commit_preserves_data(self):
        """COMMIT preserves changes."""
        engine = Engine(wal_path="/tmp/test_wal_commit_data.wal")
        engine.execute(Parser("CREATE TABLE users (id INT, name TEXT)").parse()[0])
        
        engine.execute(Parser("BEGIN").parse()[0])
        engine.execute(Parser("INSERT INTO users (id, name) VALUES (1, 'Alice')").parse()[0])
        engine.execute(Parser("COMMIT").parse()[0])
        
        # Data should persist
        result = engine.execute(Parser("SELECT * FROM users").parse()[0])
        assert result["count"] == 1


class TestEngineTypeCoercion:
    """Tests for type coercion on INSERT."""
    
    def test_int_coercion(self):
        """String values coerced to INT."""
        engine = Engine()
        engine.execute(Parser("CREATE TABLE t (id INT)").parse()[0])
        engine.execute(Parser("INSERT INTO t (id) VALUES ('42')").parse()[0])
        
        result = engine.execute(Parser("SELECT * FROM t").parse()[0])
        assert result["rows"][0]["id"] == 42  # Coerced to int
    
    def test_text_coercion(self):
        """Numbers coerced to TEXT."""
        engine = Engine()
        engine.execute(Parser("CREATE TABLE t (name TEXT)").parse()[0])
        engine.execute(Parser("INSERT INTO t (name) VALUES (123)").parse()[0])
        
        result = engine.execute(Parser("SELECT * FROM t").parse()[0])
        assert result["rows"][0]["name"] == "123"  # Coerced to str
    
    def test_real_coercion(self):
        """String coerced to REAL."""
        engine = Engine()
        engine.execute(Parser("CREATE TABLE t (val REAL)").parse()[0])
        engine.execute(Parser("INSERT INTO t (val) VALUES ('3.14')").parse()[0])
        
        result = engine.execute(Parser("SELECT * FROM t").parse()[0])
        assert result["rows"][0]["val"] == 3.14  # Coerced to float


class TestEngineDelete:
    """Tests for DELETE statement."""
    
    def test_delete_all(self):
        """DELETE without WHERE removes all rows."""
        engine = Engine()
        engine.execute(Parser("CREATE TABLE t (x INT)").parse()[0])
        engine.execute(Parser("INSERT INTO t (x) VALUES (1), (2), (3)").parse()[0])
        
        result = engine.execute(Parser("DELETE FROM t").parse()[0])
        assert result["status"] == "ok"
        assert result["message"] == "Deleted 3 row(s)"
        
        result = engine.execute(Parser("SELECT * FROM t").parse()[0])
        assert result["count"] == 0
    
    def test_delete_with_where(self):
        """DELETE with WHERE removes matching rows."""
        engine = Engine()
        engine.execute(Parser("CREATE TABLE t (x INT)").parse()[0])
        engine.execute(Parser("INSERT INTO t (x) VALUES (1), (2), (3)").parse()[0])
        
        result = engine.execute(Parser("DELETE FROM t WHERE x > 1").parse()[0])
        assert result["message"] == "Deleted 2 row(s)"
        
        result = engine.execute(Parser("SELECT * FROM t").parse()[0])
        assert result["count"] == 1
        assert result["rows"][0]["x"] == 1
    
    def test_delete_no_match(self):
        """DELETE with WHERE that matches nothing."""
        engine = Engine()
        engine.execute(Parser("CREATE TABLE t (x INT)").parse()[0])
        engine.execute(Parser("INSERT INTO t (x) VALUES (1)").parse()[0])
        
        result = engine.execute(Parser("DELETE FROM t WHERE x > 100").parse()[0])
        assert result["message"] == "Deleted 0 row(s)"


class TestEngineUpdate:
    """Tests for UPDATE statement."""
    
    def test_update_all(self):
        """UPDATE without WHERE updates all rows."""
        engine = Engine()
        engine.execute(Parser("CREATE TABLE t (x INT)").parse()[0])
        engine.execute(Parser("INSERT INTO t (x) VALUES (1), (2)").parse()[0])
        
        result = engine.execute(Parser("UPDATE t SET x = 100").parse()[0])
        assert result["status"] == "ok"
        assert result["message"] == "Updated 2 row(s)"
        
        result = engine.execute(Parser("SELECT * FROM t").parse()[0])
        for row in result["rows"]:
            assert row["x"] == 100
    
    def test_update_with_where(self):
        """UPDATE with WHERE updates matching rows."""
        engine = Engine()
        engine.execute(Parser("CREATE TABLE t (x INT)").parse()[0])
        engine.execute(Parser("INSERT INTO t (x) VALUES (1), (2), (3)").parse()[0])
        
        result = engine.execute(Parser("UPDATE t SET x = 100 WHERE x = 2").parse()[0])
        assert result["message"] == "Updated 1 row(s)"
        
        result = engine.execute(Parser("SELECT * FROM t").parse()[0])
        values = [row["x"] for row in result["rows"]]
        assert values == [1, 100, 3]
    
    def test_update_multiple_columns(self):
        """UPDATE multiple columns."""
        engine = Engine()
        engine.execute(Parser("CREATE TABLE t (a INT, b INT)").parse()[0])
        engine.execute(Parser("INSERT INTO t (a, b) VALUES (1, 10)").parse()[0])
        
        result = engine.execute(Parser("UPDATE t SET a = 2, b = 20").parse()[0])
        assert result["message"] == "Updated 1 row(s)"
        
        result = engine.execute(Parser("SELECT * FROM t").parse()[0])
        assert result["rows"][0]["a"] == 2
        assert result["rows"][0]["b"] == 20


class TestQueryPlanner:
    """Tests for Query Planner functionality."""
    
    def test_planner_creates_plan(self):
        """Planner generates a plan for SELECT."""
        from zed.query_planner import QueryPlanner
        
        engine = Engine()
        engine.execute(Parser("CREATE TABLE t (x INT)").parse()[0])
        
        stmt = Parser("SELECT * FROM t").parse()[0]
        plan = engine.planner.plan(stmt)
        
        assert plan is not None
        assert plan.root is not None
    
    def test_planner_seq_scan_default(self):
        """Planner chooses seq_scan by default."""
        engine = Engine()
        engine.execute(Parser("CREATE TABLE t (x INT)").parse()[0])
        
        stmt = Parser("SELECT * FROM t").parse()[0]
        plan = engine.planner.plan(stmt)
        
        assert plan.scan_method == "seq_scan"
    
    def test_planner_explain(self):
        """Planner can generate EXPLAIN output."""
        engine = Engine()
        engine.execute(Parser("CREATE TABLE t (x INT)").parse()[0])
        
        stmt = Parser("SELECT * FROM t").parse()[0]
        explain = engine.planner.explain(stmt)
        
        assert "Query Plan" in explain
        assert "Scan Method" in explain
    
    def test_planner_plan_to_dict(self):
        """Plan can be serialized to dict."""
        engine = Engine()
        engine.execute(Parser("CREATE TABLE t (x INT)").parse()[0])
        
        stmt = Parser("SELECT * FROM t").parse()[0]
        plan = engine.planner.plan(stmt)
        
        d = plan.to_dict()
        assert "scan_method" in d
        assert "plan" in d


class TestEngineWAL:
    """Tests for Write-Ahead Log functionality."""
    
    def test_wal_created(self):
        """Engine creates WAL on init."""
        import os
        wal_path = "/tmp/test_wal7.wal"
        if os.path.exists(wal_path):
            os.remove(wal_path)
        
        engine = Engine(wal_path=wal_path)
        assert engine.wal is not None
        assert hasattr(engine.wal, 'log_insert')
    
    def test_wal_logs_insert(self):
        """WAL logs INSERT operations."""
        engine = Engine(wal_path="/tmp/test_wal8.wal")
        engine.execute(Parser("CREATE TABLE t (x INT)").parse()[0])
        
        # Insert a row
        engine.execute(Parser("INSERT INTO t (x) VALUES (1)").parse()[0])
        
        # WAL should have records
        records = engine.wal.recover()
        insert_records = [r for r in records if r.record_type == "INSERT"]
        assert len(insert_records) >= 1
    
    def test_wal_recover(self):
        """WAL can recover records."""
        engine = Engine(wal_path="/tmp/test_wal9.wal")
        engine.execute(Parser("CREATE TABLE t (x INT)").parse()[0])
        engine.execute(Parser("INSERT INTO t (x) VALUES (1)").parse()[0])
        
        # Recover
        records = engine.wal.recover()
        assert len(records) > 0
    
    def test_wal_replay(self):
        """WAL can replay to reconstruct state."""
        engine = Engine(wal_path="/tmp/test_wal10.wal")
        engine.execute(Parser("CREATE TABLE t (x INT)").parse()[0])
        engine.execute(Parser("INSERT INTO t (x) VALUES (1)").parse()[0])
        engine.execute(Parser("INSERT INTO t (x) VALUES (2)").parse()[0])
        
        # Replay
        records = engine.wal.recover()
        tables = engine.wal.replay(records)
        
        # Should have table 't' with rows
        assert "t" in tables
        assert len(tables["t"]["rows"]) >= 2
