"""
Tests for SQL Parser layer (tokenizer + parser).
"""

import pytest
from zed.sql import Tokenizer, Parser, ParseError


class TestTokenizer:
    """Tests for SQL tokenizer."""
    
    def test_tokenize_select(self):
        """Tokenize SELECT statement."""
        t = Tokenizer("SELECT id FROM users")
        tokens = t.tokenize()
        
        assert len(tokens) > 0
        assert tokens[0].type.name == "SELECT"
    
    def test_tokenize_where(self):
        """Tokenize WHERE clause."""
        t = Tokenizer("SELECT * FROM t WHERE x > 5")
        tokens = t.tokenize()
        
        types = [tok.type.name for tok in tokens]
        assert "WHERE" in types
        assert "GREATER" in types
    
    def test_tokenize_string(self):
        """Tokenize string literals."""
        t = Tokenizer("INSERT INTO t VALUES ('hello')")
        tokens = t.tokenize()
        
        string_tokens = [tok for tok in tokens if tok.type.name == "STRING"]
        assert len(string_tokens) == 1
        assert string_tokens[0].value == "hello"
    
    def test_tokenize_numbers(self):
        """Tokenize numbers."""
        t = Tokenizer("SELECT 123, 45.67")
        tokens = t.tokenize()
        
        num_tokens = [tok for tok in tokens if tok.type.name == "NUMBER"]
        assert len(num_tokens) == 2


class TestParserCreateTable:
    """Tests for CREATE TABLE parsing."""
    
    def test_parse_create_table(self):
        """Parse CREATE TABLE."""
        stmts = Parser("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)").parse()
        
        assert len(stmts) == 1
        stmt = stmts[0]
        assert stmt.table_name == "users"
        assert len(stmt.columns) == 2
    
    def test_parse_create_table_with_not_null(self):
        """Parse CREATE TABLE with NOT NULL."""
        stmts = Parser("CREATE TABLE t (id INT NOT NULL)").parse()
        
        assert stmts[0].columns[0].nullable is False


class TestParserInsert:
    """Tests for INSERT parsing."""
    
    def test_parse_insert(self):
        """Parse INSERT."""
        stmts = Parser("INSERT INTO users (id, name) VALUES (1, 'Alice')").parse()
        
        assert len(stmts) == 1
        assert stmts[0].table_name == "users"
        assert len(stmts[0].values) == 1
    
    def test_parse_insert_multiple_rows(self):
        """Parse INSERT with multiple rows."""
        stmts = Parser("INSERT INTO t (x) VALUES (1), (2), (3)").parse()
        
        assert len(stmts[0].values) == 3


class TestParserSelect:
    """Tests for SELECT parsing."""
    
    def test_parse_select_star(self):
        """Parse SELECT *."""
        stmts = Parser("SELECT * FROM users").parse()
        
        assert len(stmts) == 1
        assert len(stmts[0].columns) == 1
        assert stmts[0].columns[0].name == "*"
    
    def test_parse_select_columns(self):
        """Parse SELECT with columns."""
        stmts = Parser("SELECT id, name FROM users").parse()
        
        assert len(stmts[0].columns) == 2
    
    def test_parse_select_where(self):
        """Parse SELECT with WHERE."""
        stmts = Parser("SELECT * FROM t WHERE x > 5").parse()
        
        assert stmts[0].where is not None
    
    def test_parse_select_limit_offset(self):
        """Parse SELECT with LIMIT and OFFSET."""
        stmts = Parser("SELECT * FROM t LIMIT 10 OFFSET 5").parse()
        
        assert stmts[0].limit == 10
        assert stmts[0].offset == 5


class TestParserTransactions:
    """Tests for transaction statements."""
    
    def test_parse_begin(self):
        """Parse BEGIN."""
        stmts = Parser("BEGIN").parse()
        assert len(stmts) == 1
    
    def test_parse_commit(self):
        """Parse COMMIT."""
        stmts = Parser("COMMIT").parse()
        assert len(stmts) == 1
    
    def test_parse_rollback(self):
        """Parse ROLLBACK."""
        stmts = Parser("ROLLBACK").parse()
        assert len(stmts) == 1


class TestParserErrors:
    """Tests for parser error handling."""
    
    def test_invalid_syntax(self):
        """Invalid syntax raises or handles error."""
        # Parser should handle gracefully (synchronize)
        stmts = Parser("INVALID SQL STATEMENT").parse()
        # May return empty or partial
        assert isinstance(stmts, list)
