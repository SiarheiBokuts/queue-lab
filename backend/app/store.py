import json
import sqlite3
import threading
import time
from pathlib import Path


class Store:
    """SQLite storage for settings, runs and per-second samples (JSON blobs, MVP)."""

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(path), check_same_thread=False)
        self.lock = threading.Lock()
        with self.lock:
            self.conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT);
                CREATE TABLE IF NOT EXISTS runs (
                    id TEXT PRIMARY KEY, created REAL, status TEXT, config TEXT, summary TEXT);
                CREATE TABLE IF NOT EXISTS samples (run_id TEXT, kind TEXT, data TEXT);
                CREATE INDEX IF NOT EXISTS idx_samples_run ON samples(run_id);
                """
            )
            self.conn.commit()

    def get_setting(self, key, default=None):
        """Read one setting, or `default` if unset."""
        with self.lock:
            row = self.conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def set_setting(self, key, value):
        """Create or update one setting."""
        with self.lock:
            self.conn.execute(
                "INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, json.dumps(value)),
            )
            self.conn.commit()

    def save_run(self, run_id, created, status, config, summary, samples, csamples):
        """Store a finished run with its per-second samples (replaces an earlier version)."""
        with self.lock:
            self.conn.execute(
                "INSERT OR REPLACE INTO runs(id,created,status,config,summary) VALUES(?,?,?,?,?)",
                (run_id, created, status, json.dumps(config), json.dumps(summary)),
            )
            self.conn.execute("DELETE FROM samples WHERE run_id=?", (run_id,))
            self.conn.executemany(
                "INSERT INTO samples(run_id,kind,data) VALUES(?,?,?)",
                [(run_id, "run", json.dumps(s)) for s in samples]
                + [(run_id, "container", json.dumps(s)) for s in csamples],
            )
            self.conn.commit()

    def list_runs(self):
        """All runs, newest first, without samples."""
        with self.lock:
            rows = self.conn.execute(
                "SELECT id,created,status,config,summary FROM runs ORDER BY created DESC"
            ).fetchall()
        return [
            {"id": r[0], "created": r[1], "status": r[2], "config": json.loads(r[3]), "summary": json.loads(r[4])}
            for r in rows
        ]

    def get_run(self, run_id):
        """One run with all its samples, or None."""
        with self.lock:
            row = self.conn.execute(
                "SELECT id,created,status,config,summary FROM runs WHERE id=?", (run_id,)
            ).fetchone()
            if not row:
                return None
            rows = self.conn.execute("SELECT kind,data FROM samples WHERE run_id=?", (run_id,)).fetchall()
        return {
            "id": row[0],
            "created": row[1],
            "status": row[2],
            "config": json.loads(row[3]),
            "summary": json.loads(row[4]),
            "samples": [json.loads(d) for k, d in rows if k == "run"],
            "container_samples": [json.loads(d) for k, d in rows if k == "container"],
        }

    def delete_run(self, run_id):
        """Delete one run and its samples."""
        with self.lock:
            self.conn.execute("DELETE FROM runs WHERE id=?", (run_id,))
            self.conn.execute("DELETE FROM samples WHERE run_id=?", (run_id,))
            self.conn.commit()

    def delete_all_runs(self):
        """Delete the whole run history."""
        with self.lock:
            self.conn.execute("DELETE FROM runs")
            self.conn.execute("DELETE FROM samples")
            self.conn.commit()


def now() -> float:
    return time.time()
