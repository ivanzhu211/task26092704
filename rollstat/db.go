package rollstat

import (
	"database/sql"

	_ "modernc.org/sqlite"
)

// Store is a rolling-window statistics store backed by a SQLite file.
// Record and Stats must not be called concurrently (caller guarantees).
type Store struct {
	db *sql.DB
}

const schema = `
CREATE TABLE IF NOT EXISTS samples (
	monitor_id TEXT NOT NULL,
	at         INTEGER NOT NULL,
	ok         INTEGER NOT NULL,
	latency_ms INTEGER NOT NULL,
	PRIMARY KEY (monitor_id, at)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS avail (
	monitor_id TEXT NOT NULL,
	bucket     INTEGER NOT NULL,
	up_ms      INTEGER NOT NULL,
	PRIMARY KEY (monitor_id, bucket)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS lat (
	monitor_id TEXT NOT NULL,
	bucket     INTEGER NOT NULL,
	cnt        INTEGER NOT NULL,
	sum        INTEGER NOT NULL,
	PRIMARY KEY (monitor_id, bucket)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS hist (
	monitor_id TEXT NOT NULL,
	bucket     INTEGER NOT NULL,
	lat_bucket INTEGER NOT NULL,
	cnt        INTEGER NOT NULL,
	PRIMARY KEY (monitor_id, bucket, lat_bucket)
) WITHOUT ROWID;
`

// Open opens (creating if necessary) the SQLite database at path.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;`); err != nil {
		db.Close()
		return nil, err
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

// Close flushes and closes the underlying database.
func (s *Store) Close() error {
	return s.db.Close()
}
