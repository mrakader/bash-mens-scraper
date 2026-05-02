# bash-mens-scraper — Project Context

Daily snapshot of bash.com mens-clothing items in the **Markham** and **Fabiani**
brands. Independent of the other three retailer scrapers (mrakader/scraper,
mrakader/tfg-scraper, mrakader/superbalist-scraper). Same hardening pattern.

## Scope

Six leaf categories under `/men/clothing/` walked sequentially:

| Internal label | bash.com path |
|---|---|
| tshirts | `/men/clothing/tops---t-shirts` |
| jeans | `/men/clothing/jeans` |
| jackets | `/men/clothing/jackets---coats` |
| shirts | `/men/clothing/shirts` |
| sweaters | `/men/clothing/jerseys---cardigans` |
| pants | `/men/clothing/pants` |

Brand filter: case-insensitive substring match on `brand` field.
Needles: `markham`, `fabiani`. Catches:
- `Markham`, `MARKHAM PREMIUM`, `CIGNAL DESIGNED BY MARKHAM`,
  `CIGNAL TAILORED BY MARKHAM`, `FABIANI`.

## Hardening (same playbook)

- `polite.PoliteSession` — curl_cffi Chrome 124 TLS impersonation, Monte Carlo
  inter-request timing, content-type guard, kill switch on 3 consecutive bad
  responses, Retry-After honoring, 403 = hard block (no retry).
- VTEX 2,500-cap recognised as graceful end-of-leaf, not whole-run abort.
- Workflow refuses to commit a degraded DB.
- **bash.com requires a homepage warmup** for cookies before the API calls work
  reliably (lesson learned from earlier sessions).

## Schema

| Table | Purpose |
|---|---|
| `products` | SCD2; new row when `brand`, `name`, `link`, `category_path`, `leaf_label` change |
| `variants` | size + colour dim |
| `variant_snapshots` | append-only daily fact: price, list_price, qty, is_available |
| `scrape_runs` | observability |

## Schedule

Daily at 12:00 SAST nominal (10:00 UTC) — same as the other retailers.
