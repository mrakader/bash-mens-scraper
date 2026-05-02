"""Daily bash.com Mens (Markham + Fabiani) snapshot orchestrator.

Pipeline:
  For each of 6 leaves (tshirts/jeans/jackets/shirts/sweaters/pants):
    Walk pages until empty.
    For each item, KEEP only if brand matches markham/fabiani (case-insensitive).
    Upsert product + variants + snapshot row.

Hardening (full stack from day 1):
  - PoliteSession (curl_cffi Chrome 124 TLS + Monte Carlo timing)
  - Honors Retry-After on 429/503; treats 403 as a hard block
  - Continues past single page errors; aborts after 3 in a row
  - VTEX 2500-cap: caught at offset >= 2400, treated as graceful end-of-leaf
  - Writes run_status.json so workflow refuses to commit a degraded DB
"""
import argparse
import json
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src import config, db
from src.polite import HardBlock
from src.bash_client import BashClient


def _to_float(v):
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _extract_product_row(p: dict, leaf_label: str) -> dict:
    cats = p.get("categories") or []
    return {
        "product_id": str(p.get("productId") or ""),
        "brand": p.get("brand"),
        "name": p.get("productName"),
        "link_text": p.get("linkText"),
        "link": p.get("link"),
        "category_path": cats[0] if cats else None,
        "leaf_label": leaf_label,
    }


def _extract_variants(p: dict) -> list[dict]:
    out = []
    product_id = str(p.get("productId") or "")
    for sku in p.get("items") or []:
        variant_id = str(sku.get("itemId") or "")
        if not variant_id:
            continue
        size_label = colour_label = None
        for vname in (sku.get("variations") or []):
            vals = sku.get(vname) or []
            if vals:
                lname = vname.lower()
                if lname in ("size", "size_"):
                    size_label = vals[0]
                elif lname in ("colour", "color"):
                    colour_label = vals[0]
        sellers = sku.get("sellers") or []
        co = (sellers[0].get("commertialOffer") if sellers else None) or {}
        out.append({
            "variant_id": variant_id,
            "product_id": product_id,
            "size_label": size_label,
            "colour_label": colour_label,
            "price": _to_float(co.get("Price")),
            "list_price": _to_float(co.get("ListPrice")),
            "available_quantity": co.get("AvailableQuantity"),
            "is_available": bool(co.get("IsAvailable")),
            "seller_name": (sellers[0] if sellers else {}).get("sellerName"),
        })
    return out


def _walk_leaf(client, conn, leaf_label, path, *, page_size, max_pages,
               today_iso, counters):
    """Walk one leaf to its end, returning 'complete', 'page_errors', or 'vtex_cap'."""
    page = 1
    offset = 0
    consec_errs = 0
    while True:
        if max_pages is not None and page > max_pages:
            return "complete"
        try:
            items = client.get_page(path=path, _from=offset, _to=offset + page_size - 1)
            consec_errs = 0
        except HardBlock as e:
            if offset >= 2400:
                print(f"  [vtex-cap] {leaf_label}: kill switch at offset {offset} "
                      "is the VTEX 2500-cap, moving on", file=sys.stderr)
                return "vtex_cap"
            print(f"  [HARD-BLOCK] {leaf_label} offset {offset}: {e}", file=sys.stderr)
            raise
        except Exception as e:
            print(f"  [error] {leaf_label} page {page} offset {offset}: {e}",
                  file=sys.stderr)
            counters["errors"] += 1
            consec_errs += 1
            if consec_errs >= 3:
                print(f"  [error] {leaf_label}: 3 consecutive errors, moving on",
                      file=sys.stderr)
                return "page_errors"
            offset += page_size
            page += 1
            continue

        print(f"  {leaf_label}: page {page} ({offset}-{offset + len(items) - 1}) "
              f"-> {len(items)} products")
        if not items:
            return "complete"

        now = db.utc_now_iso()
        for item in items:
            counters["products"] += 1
            brand = item.get("brand")
            if not config.brand_in_scope(brand):
                continue
            counters["in_brand"] += 1
            prod = _extract_product_row(item, leaf_label)
            try:
                db.upsert_product_scd2(conn, prod, now)
            except Exception as e:
                print(f"    [error] product {prod['product_id']}: {e}", file=sys.stderr)
                counters["errors"] += 1
                continue
            for v in _extract_variants(item):
                try:
                    db.upsert_variant(conn, v, now)
                    counters["variants"] += 1
                    snap = {**v, "snapshot_date": today_iso}
                    db.insert_snapshot(conn, snap, now)
                    counters["snapshots"] += 1
                except Exception as e:
                    print(f"      [error] variant {v.get('variant_id')}: {e}",
                          file=sys.stderr)
                    counters["errors"] += 1
        conn.commit()
        offset += page_size
        page += 1


def run(*, max_pages_per_leaf=config.MAX_PAGES_PER_LEAF,
        db_path=config.DB_PATH) -> dict:
    db.init_db(db_path)
    today_iso = date.today().isoformat()
    counters = {"products": 0, "in_brand": 0, "variants": 0,
                "snapshots": 0, "errors": 0}
    aborted = False
    log_path = Path(db_path).parent / "logs" / f"bash_mens_{today_iso}.jsonl"

    with db.connect(db_path) as conn, BashClient(log_path=log_path) as client:
        run_id = db.start_run(
            conn, retailer="bash-mens-mfb",
            scope_label="men/clothing × {markham, fabiani}",
        )
        print(f"[run {run_id}] scope=mens × markham+fabiani  "
              f"max_pages_per_leaf={max_pages_per_leaf}")

        consecutive_hardblocks = 0
        for leaf_label, path in config.SCOPE_LEAVES:
            try:
                _walk_leaf(
                    client, conn, leaf_label, path,
                    page_size=config.PAGE_SIZE,
                    max_pages=max_pages_per_leaf,
                    today_iso=today_iso, counters=counters,
                )
                consecutive_hardblocks = 0
            except HardBlock as e:
                consecutive_hardblocks += 1
                print(f"  [HARD-BLOCK] {leaf_label}: {e} "
                      f"(consec={consecutive_hardblocks})", file=sys.stderr)
                counters["errors"] += 1
                if consecutive_hardblocks >= 2:
                    print("  [HARD-BLOCK] 2 leaves blocked — aborting run",
                          file=sys.stderr)
                    aborted = True
                    break

        db.finish_run(
            conn, run_id,
            products_seen=counters["products"],
            products_in_brand=counters["in_brand"],
            variants_seen=counters["variants"],
            snapshots_written=counters["snapshots"],
            errors_count=counters["errors"],
            notes="aborted by kill switch" if aborted else "",
        )

    error_rate = counters["errors"] / max(1, counters["in_brand"] + counters["errors"])
    is_healthy = (
        not aborted
        and counters["in_brand"] > 0
        and error_rate < 0.05
    )
    status_path = Path(db_path).parent / "run_status.json"
    status_path.parent.mkdir(parents=True, exist_ok=True)
    status_path.write_text(json.dumps({
        "retailer": "bash-mens-mfb",
        "status": "healthy" if is_healthy else "degraded",
        "aborted": aborted,
        **counters,
        "error_rate": round(error_rate, 4),
        "snapshot_date": today_iso,
    }, indent=2), encoding="utf-8")

    print()
    print("=== RUN COMPLETE ===")
    for k, v in counters.items():
        print(f"  {k:20s} {v:,}")
    print(f"  status              {'HEALTHY' if is_healthy else 'DEGRADED'}")
    if not is_healthy:
        sys.exit(2)
    return counters


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-pages-per-leaf", type=int, default=None)
    args = parser.parse_args()
    run(max_pages_per_leaf=args.max_pages_per_leaf
        if args.max_pages_per_leaf is not None else config.MAX_PAGES_PER_LEAF)
    return 0


if __name__ == "__main__":
    sys.exit(main())
