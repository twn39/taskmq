CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at DATETIME,
    updated_at DATETIME,
    deleted_at DATETIME,
    name TEXT,
    email TEXT
);
CREATE INDEX idx_users_deleted_at ON users(deleted_at);
