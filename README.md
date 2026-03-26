# zed-base

A fully-featured persistent relational database engine written in Python.

## Features

### Core SQL Support
- ✅ SQL Parser (CREATE TABLE, INSERT, SELECT, DELETE, UPDATE)
- ✅ WHERE clause filtering (=, !=, <, >, <=, >=, AND, OR, NOT)
- ✅ JOIN operations (INNER, LEFT, RIGHT, CROSS JOIN)
- ✅ Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- ✅ GROUP BY with aggregates
- ✅ LIMIT and OFFSET
- ✅ Transaction support (BEGIN, COMMIT, ROLLBACK)
- ✅ DELETE and UPDATE with WHERE clause

### Storage & Indexing
- ✅ B-Tree storage (in-memory with disk persistence - save/load)
- ✅ Schema management (typed columns: INT, TEXT, REAL, BOOL, NULL with coercion)
- ✅ Hash indexes for equality lookups (O(1))
- ✅ B-Tree indexes for range queries
- ✅ Index usage tracking (scan_method: seq_scan / index_scan)

### Engine & REPL
- ✅ Interactive REPL with result display (prompt_toolkit)
- ✅ Write-Ahead Log (WAL) for durability
- ✅ Transaction isolation (ACID compliance) - BEGIN/COMMIT/ROLLBACK with data reversion
- ✅ Query planner and optimizer (plan generation, index selection, EXPLAIN)

## Installation

```bash
# Install in development mode
pip install -e .
```

## Try It Out

### Run the REPL

```bash
zed
```

### Example Session

Once in the REPL, try these commands:

```sql
-- Create tables
CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT);
CREATE TABLE orders (id INT, user_id INT, item TEXT);

-- Insert data
INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30);
INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25);
INSERT INTO orders (id, user_id, item) VALUES (1, 1, 'Laptop');
INSERT INTO orders (id, user_id, item) VALUES (2, 1, 'Phone');
INSERT INTO orders (id, user_id, item) VALUES (3, 2, 'Tablet');

-- Select with WHERE clause
SELECT * FROM users WHERE age > 25;

-- INNER JOIN
SELECT users.name, orders.item FROM users JOIN orders ON users.id = orders.user_id;

-- LEFT JOIN (includes users without orders)
SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id;

-- Aggregate functions
SELECT COUNT(*) FROM users;
SELECT SUM(age) FROM users;
SELECT AVG(age) FROM users;
SELECT MIN(age), MAX(age) FROM users;

-- GROUP BY with aggregates
SELECT age, COUNT(*) FROM users GROUP BY age;
SELECT name, SUM(amount) FROM orders GROUP BY name;

-- Transactions with WAL
BEGIN;
INSERT INTO users (id, name, age) VALUES (4, 'Dave', 40);
SELECT * FROM users WHERE id = 4;
COMMIT;

-- Rollback example
BEGIN;
INSERT INTO users (id, name, age) VALUES (5, 'Eve', 35);
ROLLBACK;  -- Changes discarded

-- UPDATE and DELETE
UPDATE users SET age = 31 WHERE name = 'Alice';
DELETE FROM users WHERE age < 25;

-- Exit
quit
```

Expected output:

```
✓ Table 'users' created
✓ Table 'orders' created
✓ Inserted 1 row(s)
...
id | name | age
----------------
1 | Alice | 30
3 | Charlie | 35
2 row(s)
name | item
-----------
Alice | Laptop
Alice | Phone
Bob | Tablet
3 row(s)
```

## Usage as a Library

```python
from zed.sql import Parser
from zed.engine import Engine

# Create engine
engine = Engine()

# Parse and execute SQL
sql = "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)"
stmts = Parser(sql).parse()
result = engine.execute(stmts[0])
print(result)  # {'status': 'ok', 'message': "Table 'users' created"}

# Insert
sql = "INSERT INTO users (id, name) VALUES (1, 'Alice')"
stmts = Parser(sql).parse()
result = engine.execute(stmts[0])
print(result)  # {'status': 'ok', 'message': 'Inserted 1 row(s)'}

# Select
sql = "SELECT * FROM users"
stmts = Parser(sql).parse()
result = engine.execute(stmts[0])
print(result)
# {'status': 'ok', 'columns': ['id', 'name'], 'rows': [...], 'count': 1}
```

## Transactions and WAL (Write-Ahead Log)

Zed Database supports ACID transactions with Write-Ahead Log (WAL) for durability.

### Transaction Commands

```sql
-- Start a transaction
BEGIN;

-- Perform operations
INSERT INTO users (id, name) VALUES (1, 'Alice');
UPDATE users SET name = 'Bob' WHERE id = 1;

-- Commit changes
COMMIT;

-- Or rollback to discard
ROLLBACK;
```

### WAL for Durability

The Write-Ahead Log ensures durability by logging all operations before applying them:

```python
from zed.engine import Engine

# Create engine with WAL
engine = Engine(wal_path="my_database.wal")

# All operations are logged to WAL
engine.execute(Parser("INSERT INTO users (id) VALUES (1)").parse()[0])

# Recover from WAL after crash
records = engine.wal.recover()
tables = engine.wal.replay(records)
```

### Transaction Examples

```bash
zed> BEGIN;
✓ Transaction 1 started

zed> INSERT INTO users (id, name) VALUES (1, 'Alice');
✓ Inserted 1 row(s)

zed> SELECT * FROM users;
id | name
--------
1 | Alice
1 row(s)

zed> COMMIT;
✓ Transaction 1 committed

zed> BEGIN;
✓ Transaction 2 started

zed> INSERT INTO users (id, name) VALUES (2, 'Bob');
✓ Inserted 1 row(s)

zed> ROLLBACK;
✓ Transaction 2 rolled back

zed> SELECT * FROM users;
id | name
--------
1 | Alice
1 row(s)
```

**Note**: ROLLBACK reverts all in-memory data changes made during the transaction. The table state is restored to what it was at BEGIN.

## Query Planner

Zed includes a query planner that optimizes query execution:

```python
from zed.engine import Engine
from zed.sql import Parser

engine = Engine()

# Create table
engine.execute(Parser("CREATE TABLE users (id INT, age INT)").parse()[0])

# Get query plan
stmt = Parser("SELECT * FROM users WHERE age = 30").parse()[0]
plan = engine.planner.plan(stmt)

print(plan.scan_method)  # "seq_scan" or "index_scan"
print(plan.to_dict())    # Full plan tree
```

### EXPLAIN

Generate query plan explanations:

```python
explain = engine.planner.explain(stmt)
print(explain)
```

Output:
```
Query Plan:
========================================
Scan Method: seq_scan

Plan Tree:
- Project
  - SeqScan (table: users)
```

## Supported Aggregate Functions

Zed Database supports the following aggregate functions, common to modern SQL databases:

| Function | Description | Example |
|----------|-------------|---------|
| `COUNT(*)` | Count all rows | `SELECT COUNT(*) FROM users;` |
| `COUNT(expr)` | Count non-NULL values | `SELECT COUNT(age) FROM users;` |
| `SUM(expr)` | Sum of numeric values | `SELECT SUM(amount) FROM orders;` |
| `AVG(expr)` | Arithmetic mean | `SELECT AVG(age) FROM users;` |
| `MIN(expr)` | Minimum value | `SELECT MIN(price) FROM products;` |
| `MAX(expr)` | Maximum value | `SELECT MAX(price) FROM products;` |

### GROUP BY

Group rows by one or more columns and apply aggregates per group:

```sql
-- Count users by age
SELECT age, COUNT(*) FROM users GROUP BY age;

-- Sum orders by product
SELECT product, SUM(amount) FROM orders GROUP BY product;

-- Multiple aggregates per group
SELECT department, COUNT(*), AVG(salary), MIN(salary), MAX(salary)
FROM employees
GROUP BY department;
```

### Aggregate Examples

```bash
# Count all rows
zed> SELECT COUNT(*) FROM users;
COUNT
-----
3
1 row(s)

# Sum with GROUP BY
zed> SELECT age, COUNT(*) FROM users GROUP BY age;
age | COUNT
-----------
30  | 2
25  | 1
2 row(s)

# Multiple aggregates
zed> SELECT MIN(age), MAX(age), AVG(age) FROM users;
MIN | MAX | AVG
---------------
25  | 30  | 28.33
1 row(s)
```

### Full List of SQL Aggregates (Reference)

Modern SQL databases support many aggregate functions. Here's a comprehensive list:

**Standard SQL (All Databases):**
- `COUNT(expr)` / `COUNT(*)` - Count values/rows
- `SUM(expr)` - Sum
- `AVG(expr)` - Average
- `MIN(expr)` - Minimum
- `MAX(expr)` - Maximum

**PostgreSQL:**
- `ARRAY_AGG`, `STRING_AGG`, `JSON_AGG`, `JSONB_AGG`
- `BIT_AND`, `BIT_OR`, `BOOL_AND`, `BOOL_OR`
- `STDDEV`, `STDDEV_POP`, `STDDEV_SAMP`
- `VARIANCE`, `VAR_POP`, `VAR_SAMP`
- `CORR`, `COVAR_POP`, `COVAR_SAMP`
- `PERCENTILE_CONT`, `PERCENTILE_DISC`
- `MODE()` (WITHIN GROUP)

**MySQL:**
- `GROUP_CONCAT`, `BIT_AND`, `BIT_OR`, `BIT_XOR`
- `JSON_ARRAYAGG`, `JSON_OBJECTAGG`

**SQL Server:**
- `STDEV`, `STDEVP`, `VAR`, `VARP`
- `GROUPING`, `GROUPING_ID`
- `CHECKSUM_AGG`

**Oracle:**
- `MEDIAN`, `LISTAGG`, `COLLECT`
- `STDDEV`, `VARIANCE`, `CORR`, `COVAR_POP`, `COVAR_SAMP`
- Window functions: `RANK`, `ROW_NUMBER`, `DENSE_RANK`, etc.

**SQLite:**
- `GROUP_CONCAT`

*Note: Zed Database currently implements the standard SQL aggregates (COUNT, SUM, AVG, MIN, MAX) and GROUP BY. Advanced aggregates and window functions may be added in future versions.*

## Persistence

B-Tree storage supports disk persistence:

```python
from zed.storage import BTree

# Create and populate tree
tree = BTree(order=4)
tree.insert(1, {"name": "Alice"})
tree.insert(2, {"name": "Bob"})

# Save to disk
tree.save("data.btree")

# Load from disk
loaded_tree = BTree.load("data.btree")
print(loaded_tree.search(1))  # {"name": "Alice"}

# Also supports JSON format
tree.save_json("data.json")
```

## Type Coercion

Values are automatically coerced to column types on INSERT:

```sql
CREATE TABLE users (id INT, age INT, name TEXT);
-- These are automatically coerced:
INSERT INTO users (id, age, name) VALUES ('1', '30', 123);
-- Results in: id=1 (int), age=30 (int), name='123' (str)
```

## Project Structure

```
zed/
├── __init__.py        # Package exports
├── repl.py            # Interactive REPL
├── schema.py          # Table, Column, Schema (simple dataclasses)
├── engine.py          # SQL execution engine
├── sql/               # SQL Parser Layer
│   ├── __init__.py
│   ├── ast.py         # AST nodes
│   ├── parser.py      # Recursive descent parser
│   └── tokenizer.py   # SQL lexer
└── storage/           # B-Tree Storage
    ├── __init__.py
    └── btree.py       # B-Tree implementation
```

## Development

### Install with Dev Dependencies

```bash
pip install -e ".[dev]"
```

### Run Tests

```bash
# Run all tests
pytest

# Run with verbose output
pytest -v

# Run specific test file
pytest tests/test_btree.py
```

### Code Coverage

```bash
# Run tests with coverage report (terminal)
pytest tests/ --cov=zed --cov-report=term

# Run with coverage report (HTML for browser)
pytest tests/ --cov=zed --cov-report=html

# View HTML report
open htmlcov/index.html  # macOS
xdg-open htmlcov/index.html  # Linux
```

**Coverage Report Example:**
```
_______________ coverage: platform linux, python 3.13.5-final-0 ________________

Name                      Stmts   Miss  Cover
---------------------------------------------
zed/__init__.py               5      0 100.0%
zed/engine.py               120    18  85.0%
zed/schema.py                37      0 100.0%
zed/sql/__init__.py           4      0 100.0%
zed/sql/ast.py               76      0 100.0%
zed/sql/parser.py           377   119  68.4%
zed/sql/tokenizer.py        230    36  84.3%
zed/storage/__init__.py       2      0 100.0%
zed/storage/btree.py         92     4  95.7%
---------------------------------------------
TOTAL                       943   177  81.2%

============================== 56 passed ==============================
```

## Performance Verification

### Check Index Usage vs Full Table Scan

The query result includes a `scan_method` field:

```python
from zed.sql import Parser
from zed.engine import Engine

engine = Engine()
engine.execute(Parser("CREATE TABLE users (id INT, age INT)").parse()[0])
engine.execute(Parser("INSERT INTO users (id, age) VALUES (1, 30)").parse()[0])

result = engine.execute(Parser("SELECT * FROM users WHERE age = 30").parse()[0])
print(result["scan_method"])  # "seq_scan" or "index_scan"
```

- `"seq_scan"` - Full table scan (no index used)
- `"index_scan"` - Index was used for the query

### Benchmark Index Performance

Run benchmark tests to verify index performance:

```bash
# Run all benchmark tests
pytest tests/test_benchmark.py -v

# Run with output to see timing
pytest tests/test_benchmark.py -v -s
```

Example benchmark output:
```
test_hash_index_vs_list_scan
  Index lookup: 0.000123s
  Full scan:    0.045678s
  Speedup:      371.4x

test_btree_index_vs_list_scan_range
  Index range:  0.000234s
  Full scan:    0.012345s
  Speedup:      52.8x
```

### Ways to Verify Index Performance

1. **Benchmark Tests**: Run `tests/test_benchmark.py` to compare index vs full scan
2. **Query Timing**: Time queries manually in Python
3. **Scan Method**: Check `result["scan_method"]` in query results
4. **Index Size**: Check `len(index)` for number of unique keys
5. **Duplicate Handling**: Verify `index.lookup(key)` returns all matching indices

### Run All Tests Including Benchmarks

```bash
pytest tests/ -v
```

## License

MIT