# Zed Database Engine - Design Document

## Overview

Zed is a fully-featured persistent relational database engine written in Python. It supports SQL-like operations, disk-based storage, indexing, transactions, and query optimization.

---

## 1. Feature Roadmap

### Phase 1: Core Storage & Basic Operations
- [x] Basic REPL (prompt_toolkit)
- [x] B-Tree disk storage engine (save/load to disk)
- [x] Typed columns (INT, TEXT, REAL, BOOL, NULL) with coercion
- [x] CREATE TABLE statement
- [x] INSERT statement
- [x] Basic SELECT statement

### Phase 2: Query & Indexing
- [x] WHERE clause filtering
- [x] Hash indexing for equality lookups
- [x] B-Tree indexing for range queries
- [x] JOIN operations (INNER, LEFT, RIGHT, CROSS)
- [x] Aggregations (COUNT, SUM, AVG, MIN, MAX)

### Phase 3: Transactions & Query Optimization
- [x] Write-Ahead Log (WAL) for durability
- [x] Transaction support (BEGIN, COMMIT, ROLLBACK with data reversion)
- [x] Query planner and optimizer (basic plan generation, index selection, EXPLAIN)
- [ ] Query execution engine (Volcano iterator model - future)

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        REPL / Client Layer                       │
│                    (prompt_toolkit / API)                        │
└─────────────────────────────┬───────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────┐
│                       SQL Parser Layer                           │
│              (Tokenize → Parse → AST Generation)                 │
└─────────────────────────────┬───────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────┐
│                      Query Planner Layer                         │
│         (Analyze → Optimize → Generate Execution Plan)           │
└─────────────────────────────┬───────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────┐
│                    Query Execution Engine                        │
│            (Scan → Filter → Join → Aggregate → Project)          │
└─────────────────────────────┬───────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────┐
│                     Storage Engine                               │
│  ┌──────────────┐  ┌──────────────┐  ┌─────────────────────┐   │
│  │   B-Tree     │  │  Hash Index  │  │   Write-Ahead Log   │   │
│  │   (Disk)     │  │  (Disk/Mem)  │  │   (Durability)      │   │
│  └──────────────┘  └──────────────┘  └─────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. Component Design

### 3.1 SQL Parser Layer

**Purpose**: Convert raw SQL text into an Abstract Syntax Tree (AST).

**Components**:
- **Tokenizer (Lexer)**: Breaks SQL text into tokens (keywords, identifiers, literals, operators)
- **Parser**: Converts token stream into AST nodes
- **AST Nodes**:
  - `Statement` (base class)
    - `CreateTableStatement`
    - `InsertStatement`
    - `SelectStatement`
    - `BeginStatement`
    - `CommitStatement`
    - `RollbackStatement`

**Example AST for `SELECT id, name FROM users WHERE age > 18`**:
```
SelectStatement
├── columns: [ColumnRef("id"), ColumnRef("name")]
├── table: TableRef("users")
├── where: BinaryOp(">", ColumnRef("age"), Literal(18))
└── joins: []
```

### 3.2 Query Planner Layer

**Purpose**: Analyze AST and generate an optimized execution plan.

**Components**:
- **Semantic Analyzer**: Validates column/table references, types
- **Optimizer**:
  - Predicate pushdown (move WHERE into scans)
  - Join reordering (smallest table first)
  - Index selection (choose B-Tree vs Hash based on query)
  - Projection pushdown
- **Plan Generator**: Produces a tree of execution operators

**Execution Plan Operators**:
- `SeqScan` - Full table scan
- `IndexScan` - Use index for filtering
- `Filter` - Apply WHERE conditions
- `NestedLoopJoin` / `HashJoin` - Join tables
- `Aggregate` - Apply aggregations
- `Project` - Select columns

### 3.3 Query Execution Engine

**Purpose**: Execute the query plan and produce results.

**Execution Model**: Pipeline / Iterator-based (Volcano model)
- Each operator implements `open()`, `next()`, `close()`
- Data flows as tuples (rows) through the pipeline

**Components**:
- `Executor` - Orchestrates plan execution
- `Row` / `Tuple` - In-memory row representation
- `Schema` - Column definitions and types

### 3.4 Storage Engine

#### 3.4.1 B-Tree Disk Storage

**Purpose**: Persistent ordered key-value storage for tables and indexes.

**Design**:
- **Page-based storage**: Fixed 4KB pages on disk
- **B+ Tree structure**:
  - Internal nodes: keys + child pointers
  - Leaf nodes: keys + row IDs (or inline data)
  - All data in leaves (B+ tree, not B-tree)
- **Buffer Pool**: In-memory cache of pages (LRU eviction)
- **Page Layout**:
  ```
  ┌──────────┬──────────────────────────────┬──────────┐
  │ Header   │  Slot Array (offsets)        │ Free     │
  │ (meta)   │  [ptr1, ptr2, ...]           │ Space    │
  └──────────┴──────────────────────────────┴──────────┘
  ```
- **Operations**: Insert, Delete, Search, Range Scan

#### 3.4.2 Hash Index

**Purpose**: Fast equality lookups (WHERE col = value).

**Design**:
- **Hash Table on disk**: Bucket-based with overflow pages
- **Hash function**: Simple hash of key bytes
- **Structure**:
  - Directory page → bucket pages → overflow chain
- **Operations**: Insert, Delete, Lookup (O(1) avg)

#### 3.4.3 Write-Ahead Log (WAL)

**Purpose**: Ensure durability and support transactions.

**Design**:
- **Log records**: Written before data pages (WAL protocol)
- **Record types**:
  - BEGIN_TXN
  - COMMIT_TXN
  - ROLLBACK_TXN
  - INSERT_RECORD
  - DELETE_RECORD
  - UPDATE_RECORD
  - CHECKPOINT
- **Log file**: Sequential append-only file
- **Recovery**:
  1. Read WAL from last checkpoint
  2. Replay committed transactions
  3. Undo uncommitted transactions
- **Checkpointing**: Periodic snapshots to reduce recovery time

### 3.5 Data Types

| Type    | Python Type | Storage Size | Notes                  |
|---------|-------------|--------------|------------------------|
| INT     | int         | 8 bytes      | 64-bit signed integer  |
| REAL    | float       | 8 bytes      | 64-bit float           |
| TEXT    | str         | Variable     | UTF-8 encoded          |
| BOOL    | bool        | 1 byte       | True/False             |
| NULL    | None        | 0 bytes      | Special null value     |

**Column Definition**:
```python
@dataclass
class Column:
    name: str
    dtype: DataType
    nullable: bool = True
    primary_key: bool = False
    default: Any = None
```

### 3.6 Transaction Model

**Isolation Level**: Read Committed (initially)

**Operations**:
- `BEGIN` - Start a new transaction
- `COMMIT` - Persist all changes
- `ROLLBACK` - Undo all changes

**Implementation**:
- Each transaction gets a unique ID
- WAL records are tagged with txn_id
- On COMMIT: flush WAL, mark as committed
- On ROLLBACK: replay undo records

---

## 4. File System Layout

```
data/
├── zed.db              # Main data file (B-Tree pages)
├── zed.idx             # Index file (Hash + B-Tree indexes)
├── zed.wal             # Write-Ahead Log
├── zed.meta            # Metadata (schemas, table info)
└── checkpoints/
    └── ckpt-0001.db    # Checkpoint snapshots
```

---

## 5. Example SQL Commands

```sql
-- Create a table
CREATE TABLE users (
    id INT PRIMARY KEY,
    name TEXT NOT NULL,
    age INT,
    email TEXT
);

-- Insert data
INSERT INTO users (id, name, age, email) VALUES (1, 'Alice', 30, 'alice@example.com');

-- Select with WHERE
SELECT id, name FROM users WHERE age > 25;

-- Join
SELECT u.name, o.order_id FROM users u JOIN orders o ON u.id = o.user_id;

-- Aggregation
SELECT age, COUNT(*) FROM users GROUP BY age;

-- Transactions
BEGIN;
INSERT INTO users VALUES (2, 'Bob', 25, 'bob@example.com');
COMMIT;
```

---

## 6. Query Planning Example

**Query**: `SELECT name FROM users WHERE age > 30`

**Steps**:
1. Parse → SelectStatement
2. Semantic Analysis → Validate `users.age` exists
3. Optimizer:
   - Push filter into scan
   - Choose IndexScan if age has index
4. Generate Plan:
   ```
   Project(columns=[name])
     └── Filter(condition=age > 30)
           └── SeqScan(table=users)
   ```
5. Execute → Iterator returns rows

---

## 7. Implementation Order

1. **REPL** (done) - Basic input/output loop
2. **Tokenizer** - SQL tokenization
3. **Parser** - Build AST from tokens
4. **Schema Management** - Table/column definitions
5. **Storage Engine** - B-Tree pages, buffer pool
6. **Basic CRUD** - CREATE TABLE, INSERT, SELECT
7. **WHERE Clause** - Filter rows
8. **Hash Index** - Equality lookups
9. **B-Tree Index** - Range queries
10. **JOIN** - Multi-table queries
11. **Aggregations** - GROUP BY, aggregate functions
12. **WAL** - Durability
13. **Transactions** - BEGIN/COMMIT/ROLLBACK
14. **Query Planner** - Optimization

---

## 8. Dependencies

- `prompt_toolkit` - REPL interface
- `dataclasses` - Standard library for data structures
- No external DB libraries (pure Python implementation)

---

*Design document version: 1.0*
*Last updated: 2026-03-25*
