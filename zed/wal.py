"""
Zed Database - Write-Ahead Log (WAL)

Provides durability and transaction support by logging operations
before applying them to the database.

WAL Protocol:
1. Log record is written BEFORE data modification
2. On COMMIT: flush WAL, mark transaction as committed
3. On ROLLBACK: discard uncommitted entries
4. On recovery: replay committed transactions
"""

import json
import os
import time
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional, TextIO
from enum import Enum


class WALRecordType(Enum):
    """Types of WAL records."""
    BEGIN = "BEGIN"
    COMMIT = "COMMIT"
    ROLLBACK = "ROLLBACK"
    INSERT = "INSERT"
    DELETE = "DELETE"
    UPDATE = "UPDATE"
    CREATE_TABLE = "CREATE_TABLE"
    DROP_TABLE = "DROP_TABLE"
    CHECKPOINT = "CHECKPOINT"


@dataclass
class WALRecord:
    """A single WAL record."""
    record_type: str
    txn_id: int
    timestamp: float
    table_name: Optional[str] = None
    data: Optional[Dict[str, Any]] = None
    row_index: Optional[int] = None
    
    def to_json(self) -> str:
        """Serialize to JSON string."""
        return json.dumps({
            "type": self.record_type,
            "txn_id": self.txn_id,
            "timestamp": self.timestamp,
            "table": self.table_name,
            "data": self.data,
            "row_index": self.row_index
        })
    
    @classmethod
    def from_json(cls, json_str: str) -> "WALRecord":
        """Deserialize from JSON string."""
        d = json.loads(json_str)
        return cls(
            record_type=d["type"],
            txn_id=d["txn_id"],
            timestamp=d["timestamp"],
            table_name=d.get("table"),
            data=d.get("data"),
            row_index=d.get("row_index")
        )


class WriteAheadLog:
    """
    Write-Ahead Log for durability and transactions.
    
    Features:
    - Log records before applying changes
    - Transaction boundaries (BEGIN, COMMIT, ROLLBACK)
    - Persistence to disk
    - Recovery by replaying logs
    """
    
    def __init__(self, log_path: str = "zed.wal"):
        self.log_path = log_path
        self._file: Optional[TextIO] = None
        self._txn_counter = 0
        self._current_txn_id: Optional[int] = None
        self._uncommitted_records: List[WALRecord] = []
        self._committed_txns: set = set()
        
        # Open log file for appending
        self._open_log()
    
    def _open_log(self):
        """Open the log file for appending."""
        self._file = open(self.log_path, "a")
    
    def _write_record(self, record: WALRecord):
        """Write a record to the WAL."""
        if self._file:
            self._file.write(record.to_json() + "\n")
            self._file.flush()  # Ensure durability
    
    def begin_transaction(self) -> int:
        """
        Start a new transaction.
        Returns the transaction ID.
        """
        self._txn_counter += 1
        self._current_txn_id = self._txn_counter
        self._uncommitted_records = []
        
        record = WALRecord(
            record_type=WALRecordType.BEGIN.value,
            txn_id=self._current_txn_id,
            timestamp=time.time()
        )
        self._write_record(record)
        
        return self._current_txn_id
    
    def commit(self) -> bool:
        """
        Commit the current transaction.
        Returns True if committed successfully.
        """
        if self._current_txn_id is None:
            return False
        
        # Write commit record
        record = WALRecord(
            record_type=WALRecordType.COMMIT.value,
            txn_id=self._current_txn_id,
            timestamp=time.time()
        )
        self._write_record(record)
        
        # Mark as committed
        self._committed_txns.add(self._current_txn_id)
        self._current_txn_id = None
        self._uncommitted_records = []
        
        return True
    
    def rollback(self) -> bool:
        """
        Rollback the current transaction.
        Returns True if rolled back.
        """
        if self._current_txn_id is None:
            return False
        
        # Write rollback record
        record = WALRecord(
            record_type=WALRecordType.ROLLBACK.value,
            txn_id=self._current_txn_id,
            timestamp=time.time()
        )
        self._write_record(record)
        
        # Discard uncommitted records
        self._uncommitted_records = []
        self._current_txn_id = None
        
        return True
    
    def log_insert(self, table_name: str, row: Dict[str, Any], row_index: int):
        """Log an INSERT operation."""
        if self._current_txn_id is None:
            # Auto-commit for non-transactional operations
            txn_id = self.begin_transaction()
            record = WALRecord(
                record_type=WALRecordType.INSERT.value,
                txn_id=txn_id,
                timestamp=time.time(),
                table_name=table_name,
                data=row,
                row_index=row_index
            )
            self._write_record(record)
            self.commit()
        else:
            record = WALRecord(
                record_type=WALRecordType.INSERT.value,
                txn_id=self._current_txn_id,
                timestamp=time.time(),
                table_name=table_name,
                data=row,
                row_index=row_index
            )
            self._write_record(record)
            self._uncommitted_records.append(record)
    
    def log_delete(self, table_name: str, row_index: int, row: Dict[str, Any]):
        """Log a DELETE operation."""
        if self._current_txn_id is None:
            txn_id = self.begin_transaction()
            record = WALRecord(
                record_type=WALRecordType.DELETE.value,
                txn_id=txn_id,
                timestamp=time.time(),
                table_name=table_name,
                data=row,
                row_index=row_index
            )
            self._write_record(record)
            self.commit()
        else:
            record = WALRecord(
                record_type=WALRecordType.DELETE.value,
                txn_id=self._current_txn_id,
                timestamp=time.time(),
                table_name=table_name,
                data=row,
                row_index=row_index
            )
            self._write_record(record)
            self._uncommitted_records.append(record)
    
    def log_create_table(self, table_name: str, schema: Dict[str, Any]):
        """Log a CREATE TABLE operation."""
        record = WALRecord(
            record_type=WALRecordType.CREATE_TABLE.value,
            txn_id=self._current_txn_id or 0,
            timestamp=time.time(),
            table_name=table_name,
            data=schema
        )
        self._write_record(record)
    
    def log_drop_table(self, table_name: str):
        """Log a DROP TABLE operation."""
        record = WALRecord(
            record_type=WALRecordType.DROP_TABLE.value,
            txn_id=self._current_txn_id or 0,
            timestamp=time.time(),
            table_name=table_name
        )
        self._write_record(record)
    
    def checkpoint(self):
        """Create a checkpoint (truncate old logs)."""
        record = WALRecord(
            record_type=WALRecordType.CHECKPOINT.value,
            txn_id=0,
            timestamp=time.time()
        )
        self._write_record(record)
        
        # Close and reopen file (could truncate in production)
        if self._file:
            self._file.close()
        self._open_log()
    
    def in_transaction(self) -> bool:
        """Check if currently in a transaction."""
        return self._current_txn_id is not None
    
    def get_current_txn_id(self) -> Optional[int]:
        """Get current transaction ID."""
        return self._current_txn_id
    
    def recover(self) -> List[WALRecord]:
        """
        Recover from WAL by reading all records.
        Returns list of records for replay.
        """
        records = []
        if not os.path.exists(self.log_path):
            return records
        
        try:
            with open(self.log_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            record = WALRecord.from_json(line)
                            records.append(record)
                        except json.JSONDecodeError:
                            pass
        except IOError:
            pass
        
        return records
    
    def replay(self, records: List[WALRecord]) -> Dict[str, List[Dict]]:
        """
        Replay records to reconstruct state.
        Returns reconstructed tables data.
        """
        tables: Dict[str, Dict[str, Any]] = {}  # table_name -> {schema, rows}
        committed_txns = set()
        
        # First pass: find committed transactions
        for record in records:
            if record.record_type == WALRecordType.COMMIT.value:
                committed_txns.add(record.txn_id)
        
        # Second pass: apply committed operations
        for record in records:
            if record.record_type == WALRecordType.CREATE_TABLE.value:
                if record.table_name:
                    tables[record.table_name] = {
                        "schema": record.data,
                        "rows": []
                    }
            
            elif record.record_type == WALRecordType.INSERT.value:
                # Only apply if committed or no transaction
                if record.txn_id == 0 or record.txn_id in committed_txns:
                    if record.table_name and record.table_name in tables:
                        tables[record.table_name]["rows"].append(record.data)
            
            elif record.record_type == WALRecordType.DELETE.value:
                if record.txn_id == 0 or record.txn_id in committed_txns:
                    if record.table_name and record.table_name in tables:
                        rows = tables[record.table_name]["rows"]
                        if record.row_index is not None and 0 <= record.row_index < len(rows):
                            rows.pop(record.row_index)
        
        return tables
    
    def close(self):
        """Close the WAL file."""
        if self._file:
            self._file.close()
            self._file = None
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
    
    def __repr__(self):
        return f"WriteAheadLog(path={self.log_path}, txn_id={self._current_txn_id})"
