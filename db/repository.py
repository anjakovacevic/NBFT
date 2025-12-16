import sqlite3
import json
from datetime import datetime
from typing import List, Dict, Any

class Repository:
    def __init__(self, db_path="nbft_experiments.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Runs table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                algorithm TEXT,
                n INTEGER,
                m INTEGER,
                actual_byzantine INTEGER,
                success BOOLEAN,
                consensus_time REAL,
                total_messages INTEGER,
                messages_per_phase TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        conn.close()

    def save_run(self, config, result):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO runs (algorithm, n, m, actual_byzantine, success, consensus_time, total_messages, messages_per_phase)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            config.algorithm,
            config.n,
            config.m,
            config.actual_byzantine,
            result.success,
            result.consensus_time,
            result.total_messages,
            json.dumps(result.messages_per_phase)
        ))
        
        conn.commit()
        conn.close()

    def get_all_runs(self) -> List[Dict[str, Any]]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM runs ORDER BY timestamp DESC")
        rows = cursor.fetchall()
        conn.close()
        
        return [dict(row) for row in rows]
