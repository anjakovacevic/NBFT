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
);
