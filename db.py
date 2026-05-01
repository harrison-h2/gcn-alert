"""
db.py

SQLite persistence layer for GCN alerts.

Tables:
  grb_events          — Only observable events with an official GRB name.
  notices             — All raw Kafka notices received (linked to grb_events if named/observable).
  circulars           — GCN Circulars (linked to grb_events if matched).
  pending_name_lookup — Observable events still waiting for an official GRB name.
"""

import json
import re
import sqlite3
from datetime import datetime, timezone, timedelta

PENDING_RETRY_MINUTES = 20
MAX_PENDING_ATTEMPTS  = 5

GRB_NAME_RE          = re.compile(r"GRB\s*\d{6}[A-Za-z]+", re.IGNORECASE)
TRIGGER_ID_RE        = re.compile(r'trigger[^0-9]*(\d{6,12})', re.IGNORECASE)
COUNTERPART_KEYWORDS = {"afterglow", "optical", "counterpart", "x-ray", "radio", "infrared"}

DB_PATH = "gcn_alerts.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS grb_events (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    grb_name         TEXT UNIQUE NOT NULL,
    ra_deg           REAL NOT NULL,
    dec_deg          REAL NOT NULL,
    magnitude        REAL,
    trigger_time     TEXT,
    first_seen_at    TEXT NOT NULL,
    last_updated_at  TEXT NOT NULL,
    best_airmass     REAL,
    observable_hours REAL
);

CREATE TABLE IF NOT EXISTS notices (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    grb_id       INTEGER,
    source       TEXT,
    event_id     TEXT,
    topic        TEXT NOT NULL,
    received_at  TEXT NOT NULL,
    instrument   TEXT,
    ra           REAL,
    dec          REAL,
    ra_dec_error REAL,
    snr          REAL,
    importance   REAL,
    trigger_time TEXT,
    ivorn        TEXT,
    raw_payload  TEXT,
    FOREIGN KEY(grb_id) REFERENCES grb_events(id)
);

CREATE TABLE IF NOT EXISTS circulars (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    grb_id          INTEGER,
    circular_number INTEGER UNIQUE,
    subject         TEXT,
    received_at     TEXT NOT NULL,
    body            TEXT,
    FOREIGN KEY(grb_id) REFERENCES grb_events(id)
);

CREATE TABLE IF NOT EXISTS pending_name_lookup (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    notice_id    INTEGER,
    source       TEXT NOT NULL,
    event_id     TEXT,
    trigger_time TEXT,
    ra           REAL,
    dec          REAL,
    instrument   TEXT,
    vis_json     TEXT NOT NULL,
    retry_after  TEXT NOT NULL,
    attempts     INTEGER DEFAULT 0,
    UNIQUE(source, event_id)
);
"""


def init_db(path=DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    conn.commit()
    return conn


def insert_notice(conn, event, raw_payload) -> int:
    grb_id = None

    if event.grb_name:
        row = conn.execute("SELECT id FROM grb_events WHERE grb_name = ?", (event.grb_name,)).fetchone()
        if row:
            grb_id = row["id"]

    if not grb_id and event.event_id:
        row = conn.execute(
            "SELECT grb_id FROM notices WHERE source = ? AND event_id = ? AND grb_id IS NOT NULL LIMIT 1",
            (event.source, event.event_id)
        ).fetchone()
        if row:
            grb_id = row["grb_id"]

    if not grb_id and event.trigger_time:
        row = conn.execute("""
            SELECT id FROM grb_events
            WHERE trigger_time IS NOT NULL
              AND ABS(JULIANDAY(trigger_time) - JULIANDAY(?)) * 86400 < 30
            ORDER BY ABS(JULIANDAY(trigger_time) - JULIANDAY(?)) ASC
            LIMIT 1
        """, (event.trigger_time, event.trigger_time)).fetchone()
        if row:
            grb_id = row["id"]

    cursor = conn.execute("""
        INSERT INTO notices
          (grb_id, source, event_id, topic, received_at, instrument, ra, dec,
           ra_dec_error, snr, importance, trigger_time, ivorn, raw_payload)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        grb_id, event.source, event.event_id, event.topic,
        datetime.now(timezone.utc).isoformat(),
        event.instrument,
        event.ra, event.dec, event.ra_dec_error,
        event.snr, event.importance, event.trigger_time,
        event.ivorn, raw_payload,
    ))
    conn.commit()
    return cursor.lastrowid


def promote_to_grb_event(conn, event, vis: dict, magnitude: float = None) -> int:
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("""
        INSERT INTO grb_events
          (grb_name, ra_deg, dec_deg, magnitude, trigger_time, first_seen_at, last_updated_at, best_airmass, observable_hours)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(grb_name) DO UPDATE SET
          ra_deg = excluded.ra_deg,
          dec_deg = excluded.dec_deg,
          magnitude = COALESCE(excluded.magnitude, magnitude),
          trigger_time = COALESCE(excluded.trigger_time, trigger_time),
          last_updated_at = excluded.last_updated_at,
          best_airmass = excluded.best_airmass,
          observable_hours = excluded.observable_hours
    """, (
        event.grb_name, event.ra, event.dec, magnitude, event.trigger_time, now, now,
        vis.get("best_airmass"), vis.get("observable_hours")
    ))
    conn.commit()

    row = conn.execute("SELECT id FROM grb_events WHERE grb_name = ?", (event.grb_name,)).fetchone()
    grb_id = row["id"]
    link_past_notices(conn, grb_id, event)
    return grb_id


def link_past_notices(conn, grb_id: int, event):
    if event.event_id:
        conn.execute(
            "UPDATE notices SET grb_id = ? WHERE source = ? AND event_id = ? AND grb_id IS NULL",
            (grb_id, event.source, event.event_id)
        )
    if event.trigger_time:
        conn.execute("""
            UPDATE notices SET grb_id = ?
            WHERE grb_id IS NULL
              AND source = ?
              AND trigger_time IS NOT NULL
              AND ABS(JULIANDAY(trigger_time) - JULIANDAY(?)) * 86400 < 30
        """, (grb_id, event.source, event.trigger_time))
    conn.execute("""
        UPDATE circulars SET grb_id = ?
        WHERE grb_id IS NULL AND (subject LIKE ? OR body LIKE ?)
    """, (grb_id, f"%{event.grb_name}%", f"%{event.grb_name}%"))
    conn.commit()


def _next_retry_time() -> str:
    return (datetime.now(timezone.utc) + timedelta(minutes=PENDING_RETRY_MINUTES)).isoformat()


def insert_pending(conn, event, vis: dict, notice_id: int):
    if not event.event_id:
        return
    conn.execute("""
        INSERT OR IGNORE INTO pending_name_lookup
          (notice_id, source, event_id, trigger_time, ra, dec, instrument, vis_json, retry_after, attempts)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
    """, (notice_id, event.source, event.event_id, event.trigger_time,
          event.ra, event.dec, event.instrument, json.dumps(vis), _next_retry_time()))
    conn.commit()


def get_due_pending(conn) -> list:
    now = datetime.now(timezone.utc).isoformat()
    return [dict(r) for r in conn.execute(
        "SELECT * FROM pending_name_lookup WHERE retry_after <= ?", (now,)
    ).fetchall()]


def bump_pending_retry(conn, pending_id: int, retry_after: str = None):
    conn.execute(
        "UPDATE pending_name_lookup SET retry_after = ?, attempts = attempts + 1 WHERE id = ?",
        (retry_after or _next_retry_time(), pending_id)
    )
    conn.commit()


def remove_pending(conn, pending_id: int):
    conn.execute("DELETE FROM pending_name_lookup WHERE id = ?", (pending_id,))
    conn.commit()


def _grb_name_to_date(grb_name: str) -> str | None:
    m = re.match(r"GRB\s*(\d{2})(\d{2})(\d{2})", grb_name, re.IGNORECASE)
    if not m:
        return None
    yy, mm, dd = m.group(1), m.group(2), m.group(3)
    return f"{2000 + int(yy)}-{mm}-{dd}"


def check_counterpart_circular(conn, data: dict) -> dict | None:
    subject = (data.get("subject") or "").lower()
    matched_keywords = [kw for kw in COUNTERPART_KEYWORDS if kw in subject]
    if not matched_keywords:
        return None

    body        = data.get("body") or ""
    trigger_ids = TRIGGER_ID_RE.findall(body)
    matched_notice = None

    if trigger_ids:
        placeholders = ",".join("?" * len(trigger_ids))
        row = conn.execute(
            f"SELECT * FROM notices WHERE event_id IN ({placeholders}) ORDER BY received_at DESC LIMIT 1",
            trigger_ids,
        ).fetchone()
        if row:
            matched_notice = dict(row)

    if not matched_notice:
        grb_m = GRB_NAME_RE.search(subject)
        if grb_m:
            date_str = _grb_name_to_date(grb_m.group(0))
            if date_str:
                row = conn.execute(
                    "SELECT * FROM notices WHERE trigger_time LIKE ? ORDER BY received_at DESC LIMIT 1",
                    (f"{date_str}%",),
                ).fetchone()
                if row:
                    matched_notice = dict(row)

    return {
        "circular_number": data.get("circular_number"),
        "subject":         data.get("subject"),
        "keywords":        matched_keywords,
        "trigger_ids":     trigger_ids,
        "matched_notice":  matched_notice,
    }


def _normalize_grb_name(name: str) -> str:
    return "GRB " + re.sub(r'\s*', '', name).upper()[3:]


def insert_circular(conn, data: dict) -> int | None:
    """Insert circular and return grb_id if linked to a tracked event, else None."""
    circular_number = data.get("circular_number")
    conn.execute("""
        INSERT OR IGNORE INTO circulars
          (circular_number, subject, received_at, body)
        VALUES (?, ?, ?, ?)
    """, (circular_number, data.get("subject"), data.get("received_at"), data.get("body")))
    conn.commit()

    subject  = (data.get("subject") or "")
    body     = (data.get("body") or "")
    event_id = (data.get("event_id") or "")

    def _lookup_grb_name(name: str) -> int | None:
        row = conn.execute("SELECT id FROM grb_events WHERE grb_name = ?",
                           (_normalize_grb_name(name),)).fetchone()
        return row["id"] if row else None

    def _link_and_return(grb_id: int) -> int:
        conn.execute("UPDATE circulars SET grb_id = ? WHERE circular_number = ?",
                     (grb_id, circular_number))
        conn.commit()
        return grb_id

    if event_id:
        grb_id = _lookup_grb_name(event_id)
        if grb_id:
            return _link_and_return(grb_id)

    m = GRB_NAME_RE.search(subject) or GRB_NAME_RE.search(body)
    if m:
        grb_id = _lookup_grb_name(m.group(0))
        if grb_id:
            return _link_and_return(grb_id)

    trigger_ids = TRIGGER_ID_RE.findall(body)
    if trigger_ids:
        placeholders = ",".join("?" * len(trigger_ids))
        row = conn.execute(
            f"SELECT grb_id FROM notices WHERE event_id IN ({placeholders}) AND grb_id IS NOT NULL LIMIT 1",
            trigger_ids,
        ).fetchone()
        if row:
            return _link_and_return(row["grb_id"])

    return None


def cleanup_old_records(conn) -> dict:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    notices_deleted = conn.execute(
        "DELETE FROM notices WHERE received_at < ? AND grb_id IS NULL", (cutoff,)
    ).rowcount
    circulars_deleted = conn.execute(
        "DELETE FROM circulars WHERE received_at < ? AND grb_id IS NULL", (cutoff,)
    ).rowcount
    conn.commit()
    return {"notices": notices_deleted, "circulars": circulars_deleted}


def daily_summary(conn, date_str: str) -> dict:
    prefix   = date_str + "T"
    next_day = (datetime.fromisoformat(date_str) + timedelta(days=1)).strftime("%Y-%m-%d") + "T"

    observable = conn.execute(
        "SELECT COUNT(*) FROM grb_events WHERE first_seen_at >= ? AND first_seen_at < ?", (prefix, next_day)
    ).fetchone()[0]
    total = conn.execute(
        "SELECT COUNT(*) FROM notices WHERE received_at >= ? AND received_at < ?", (prefix, next_day)
    ).fetchone()[0]

    retractions = conn.execute(
        """
        SELECT COUNT(*) FROM notices
        WHERE received_at >= ? AND received_at < ?
          AND (raw_payload LIKE '%cite="retraction"%' OR raw_payload LIKE '%cite=''retraction''%')
        """,
        (prefix, next_day),
    ).fetchone()[0]

    circulars_matched = conn.execute(
        """
        SELECT COUNT(*) FROM circulars
        WHERE received_at >= ? AND received_at < ? AND grb_id IS NOT NULL
        """,
        (prefix, next_day),
    ).fetchone()[0]

    by_source: dict[str, int] = {}
    for row in conn.execute(
        """
        SELECT COALESCE(NULLIF(TRIM(source), ''), 'unknown') AS src, COUNT(*) AS c
        FROM notices
        WHERE received_at >= ? AND received_at < ?
        GROUP BY src
        """,
        (prefix, next_day),
    ):
        by_source[row["src"]] = row["c"]

    top_row = conn.execute(
        """
        SELECT g.ra_deg, g.dec_deg, g.best_airmass, g.grb_name,
               (SELECT event_id FROM notices
                WHERE grb_id = g.id AND event_id IS NOT NULL AND TRIM(event_id) != ''
                ORDER BY received_at DESC LIMIT 1) AS event_id
        FROM grb_events g
        WHERE g.first_seen_at >= ? AND g.first_seen_at < ?
        ORDER BY (g.best_airmass IS NULL), g.best_airmass ASC, g.id ASC
        LIMIT 1
        """,
        (prefix, next_day),
    ).fetchone()

    top_event = None
    if top_row:
        top_event = {
            "event_id":     top_row["event_id"] or top_row["grb_name"] or "N/A",
            "ra_deg":       float(top_row["ra_deg"]),
            "dec_deg":      float(top_row["dec_deg"]),
            "best_airmass": top_row["best_airmass"],
        }

    return {
        "date":              date_str,
        "total":             total,
        "observable":        observable,
        "retractions":       retractions,
        "circulars_matched": circulars_matched,
        "by_source":         by_source,
        "top_event":         top_event,
    }


def get_circulars_for_grb_name(conn, grb_name: str) -> list:
    """Return circulars whose subject or body mention grb_name."""
    rows = conn.execute(
        "SELECT * FROM circulars WHERE subject LIKE ? OR body LIKE ? ORDER BY circular_number DESC",
        (f"%{grb_name}%", f"%{grb_name}%"),
    ).fetchall()
    return [dict(r) for r in rows]


def query_grb(conn, grb_name: str) -> dict | None:
    normalised = " ".join(grb_name.upper().split())
    event_row = conn.execute("SELECT * FROM grb_events WHERE grb_name = ?", (normalised,)).fetchone()
    if not event_row:
        return None

    grb_id    = event_row["id"]
    notices   = conn.execute("SELECT * FROM notices WHERE grb_id = ? ORDER BY received_at", (grb_id,)).fetchall()
    circulars = conn.execute("SELECT * FROM circulars WHERE grb_id = ? ORDER BY circular_number", (grb_id,)).fetchall()

    return {
        "event":     dict(event_row),
        "notices":   [dict(n) for n in notices],
        "circulars": [dict(c) for c in circulars],
    }
