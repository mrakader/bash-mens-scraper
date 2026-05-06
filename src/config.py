"""Scraper configuration for the bash.com men's Markham + Fabiani slice.

Walks 6 mens-clothing leaf categories (t-shirts, jeans, jackets, shirts,
sweaters/jerseys, pants), filters products to brands matching `markham`
or `fabiani` (case-insensitive substring — catches MARKHAM PREMIUM, CIGNAL
DESIGNED BY MARKHAM, CIGNAL TAILORED BY MARKHAM, FABIANI, etc).
"""
import os
import platform
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# --- Endpoints ----------------------------------------------------------
TFG_HOST = "https://bash.com"
SEARCH_PATH = "/api/catalog_system/pub/products/search"
HOMEPAGE = "https://bash.com/"

# --- Scope --------------------------------------------------------------
# Six mens-clothing leaves the user requested. Names are the human label
# users see; paths are the bash.com URL slugs.
SCOPE_LEAVES = [
    ("tshirts",  "men/clothing/tops---t-shirts"),
    ("jeans",    "men/clothing/jeans"),
    ("jackets",  "men/clothing/jackets---coats"),
    ("shirts",   "men/clothing/shirts"),
    ("sweaters", "men/clothing/jerseys---cardigans"),
    ("pants",    "men/clothing/pants"),
]

# Brand filter (case-insensitive substring match). Captures all Markham
# variants (Markham, MARKHAM PREMIUM, CIGNAL DESIGNED BY MARKHAM, CIGNAL
# TAILORED BY MARKHAM) and FABIANI.
BRAND_NEEDLES = ("markham", "fabiani")

PAGE_SIZE = 50
MAX_PAGES_PER_LEAF: int | None = None   # None = walk to end of category

# --- Politeness ---------------------------------------------------------
TIMEOUT_S = 30.0
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/147.0.0.0 Safari/537.36"
)

# --- Storage ------------------------------------------------------------
_default_db = (
    Path(r"C:\Users\Muhammad\dev\bash-mens-scraper-data") / "bash_mens.db"
    if platform.system() == "Windows"
    else PROJECT_ROOT / "data" / "bash_mens.db"
)
DB_PATH = Path(os.environ.get("BASH_MENS_DB_PATH", str(_default_db)))


def brand_in_scope(brand: str | None) -> bool:
    """True if brand string contains markham or fabiani (case-insensitive)."""
    if not brand:
        return False
    b = brand.lower()
    return any(needle in b for needle in BRAND_NEEDLES)
