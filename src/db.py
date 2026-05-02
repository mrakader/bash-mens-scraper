"""SQLite storage for bash.com mens (Markham + Fabiani) snapshots.

Schema:
    products           SCD2 dim
    variants           size+colour dim
    variant_snapshots  append-only daily fact (price, list_price, qty, is_avail)
    scrape_runs        observability
"""
import sqlite3
from contextlib import contextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterator


SCHEMA = """
CREATE TABLE IF NOT EXISTS products (
    surrogate_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id    TEXT NOT NULL,
    brand         TEXT,
    name          TEXT,
    link_text     TEXT,
    link          TEXT,
    category_path TEXT,
    leaf_label    TEXT,                      -- our internal label (tshirts/jeans/etc)
    valid_from    TEXT NOT NULL,
    valid_to      TEXT,
    scraped_at    TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_products_current ON products(product_id) WHERE valid_to IS NULL;
CREATE INDEX IF NOT EXISTS ix_products_brand_current ON products(brand) WHERE valid_to IS NULL;
CREATE INDEX IF NOT EXISTS ix_products_leaf ON products(leaf_label) WHERE valid_to IS NULL;

CREATE TABLE IF NOT EXISTS variants (
    variant_id     TEXT PRIMARY KEY,
    product_id     TEXT NOT NULL,
    size_label     TEXT,
    colour_label   TEXT,
    first_seen_at  TEXT NOT NULL,
    last_seen_at   TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_variants_product ON variants(product_id);

CREATE TABLE IF NOT EXISTS variant_snapshots (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    variant_id         TEXT NOT NULL,
    snapshot_date      TEXT NOT NULL,
    price              REAL,
    list_price         REAL,
    available_quantity INTEGER,
    is_available       INTEGER,
    seller_name        TEXT,
    scraped_at         TEXT NOT NULL,
    UNIQUE(variant_id, snapshot_date)
);

CREATE INDEX IF NOT EXISTS ix_snapshots_date ON variant_snapshots(snapshot_date);
CREATE INDEX IF NOT EXISTS ix_snapshots_variant_date ON variant_snapshots(variant_id, snapshot_date);

CREATE TABLE IF NOT EXISTS scrape_runs (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    run_date           TEXT NOT NULL,
    retailer           TEXT,
    scope_label        TEXT,
    started_at         TEXT NOT NULL,
    completed_at       TEXT,
    products_seen      INTEGER DEFAULT 0,
    products_in_brand  INTEGER DEFAULT 0,
    variants_seen      INTEGER DEFAULT 0,
    snapshots_written  INTEGER DEFAULT 0,
    errors_count       INTEGER DEFAULT 0,
    notes              TEXT
);
"""


_SCD_FIELDS = ("brand", "name", "link_text", "link", "category_path", "leaf_label")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def init_db(db_path: str | Path) -> None:
    p = Path(db_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(p)
    try:
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.executescript(SCHEMA)
        conn.commit()
    finally:
        conn.close()


@contextmanager
def connect(db_path: str | Path) -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.row_factory = sqlite3.Row
        yield conn
    finally:
        conn.close()


def upsert_product_scd2(conn, product: dict, now: str) -> bool:
    cur = conn.cursor()
    col_list = ", ".join(_SCD_FIELDS)
    row = cur.execute(
        f"SELECT surrogate_id, {col_list} "
        "FROM products WHERE product_id = ? AND valid_to IS NULL",
        (product["product_id"],),
    ).fetchone()
    new_values = tuple(product.get(f) for f in _SCD_FIELDS)
    if row is not None:
        existing = tuple(row[f] for f in _SCD_FIELDS)
        if existing == new_values:
            return False
        cur.execute("UPDATE products SET valid_to = ? WHERE surrogate_id = ?",
                    (now, row["surrogate_id"]))
    cols = ["product_id"] + list(_SCD_FIELDS) + ["valid_from", "scraped_at"]
    placeholders = ",".join("?" * len(cols))
    vals = (product["product_id"], *new_values, now, now)
    cur.execute(f"INSERT INTO products ({','.join(cols)}) VALUES ({placeholders})", vals)
    return True


def upsert_variant(conn, variant: dict, now: str) -> None:
    conn.execute(
        """
        INSERT INTO variants (variant_id, product_id, size_label, colour_label,
                              first_seen_at, last_seen_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(variant_id) DO UPDATE SET last_seen_at = excluded.last_seen_at
        """,
        (variant["variant_id"], variant["product_id"],
         variant.get("size_label"), variant.get("colour_label"), now, now),
    )


def insert_snapshot(conn, snap: dict, now: str) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO variant_snapshots
            (variant_id, snapshot_date, price, list_price,
             available_quantity, is_available, seller_name, scraped_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (snap["variant_id"], snap["snapshot_date"], snap.get("price"),
         snap.get("list_price"), snap.get("available_quantity"),
         1 if snap.get("is_available") else 0, snap.get("seller_name"), now),
    )


def start_run(conn, *, retailer: str, scope_label: str) -> int:
    now = utc_now_iso()
    today = date.today().isoformat()
    cur = conn.execute(
        "INSERT INTO scrape_runs (run_date, retailer, scope_label, started_at) VALUES (?, ?, ?, ?)",
        (today, retailer, scope_label, now),
    )
    conn.commit()
    return cur.lastrowid


def finish_run(conn, run_id: int, *, products_seen, products_in_brand,
               variants_seen, snapshots_written, errors_count, notes=""):
    now = utc_now_iso()
    conn.execute(
        """
        UPDATE scrape_runs
        SET completed_at=?, products_seen=?, products_in_brand=?,
            variants_seen=?, snapshots_written=?, errors_count=?, notes=?
        WHERE id=?
        """,
        (now, products_seen, products_in_brand, variants_seen,
         snapshots_written, errors_count, notes, run_id),
    )
    conn.commit()
