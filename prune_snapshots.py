"""Prune old daily snapshots to a rolling window, then VACUUM.

Caps DB growth so the gzipped file never balloons past GitHub limits / LFS quota.
Only touches tables that have a `snapshot_date` column (the time-series rows);
SCD2 dimension tables (products/variants current state) are left alone.
Older history is NOT lost — it remains in the repo's git history.

Usage:
    python prune_snapshots.py [db_path] [days] [--dry-run]
    python prune_snapshots.py                 # auto-find *.db, keep 60 days
    python prune_snapshots.py tfg.db 60
    python prune_snapshots.py tfg.db 60 --dry-run
"""
import sys, os, glob, sqlite3, datetime

DEFAULT_DAYS = 45


def find_db(arg):
    if arg and os.path.exists(arg):
        return arg
    cands = sorted(glob.glob("*.db"))
    return cands[0] if cands else None


def main():
    pos = [a for a in sys.argv[1:] if not a.startswith("--")]
    dry = "--dry-run" in sys.argv
    arg = pos[0] if pos else None
    days = int(pos[1]) if len(pos) > 1 else DEFAULT_DAYS

    db = find_db(arg)
    if not db:
        print("prune: no .db file found — skipping (nothing to prune)")
        return 0

    size0 = os.path.getsize(db)
    con = sqlite3.connect(db)
    cur = con.cursor()
    tables = [r[0] for r in cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")]
    targets = []
    for t in tables:
        cols = [c[1] for c in cur.execute(f'PRAGMA table_info("{t}")')]
        if "snapshot_date" in cols:
            targets.append(t)
    if not targets:
        print("prune: no tables with snapshot_date — nothing to do")
        con.close()
        return 0

    maxd = None
    for t in targets:
        m = cur.execute(f'SELECT MAX(snapshot_date) FROM "{t}"').fetchone()[0]
        if m and (maxd is None or m > maxd):
            maxd = m
    if not maxd:
        print("prune: tables empty — nothing to do")
        con.close()
        return 0

    cutoff = (datetime.date.fromisoformat(maxd[:10])
              - datetime.timedelta(days=days)).isoformat()

    total = 0
    for t in targets:
        n = cur.execute(
            f'SELECT COUNT(*) FROM "{t}" WHERE snapshot_date < ?',
            (cutoff,)).fetchone()[0]
        total += n
        print(f"  {t}: latest={maxd[:10]} cutoff={cutoff} -> "
              f"{n:,} rows older than window")
        if not dry and n:
            cur.execute(f'DELETE FROM "{t}" WHERE snapshot_date < ?', (cutoff,))

    if dry:
        print(f"DRY-RUN: would delete {total:,} rows (keep last {days}d). "
              "No changes written.")
        con.close()
        return 0

    con.commit()
    if total:
        cur.execute("VACUUM")
        con.commit()
    con.close()
    size1 = os.path.getsize(db)
    print(f"PRUNED {total:,} rows | keep {days}d (cutoff {cutoff}) | "
          f"{size0/1e6:.1f}MB -> {size1/1e6:.1f}MB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
