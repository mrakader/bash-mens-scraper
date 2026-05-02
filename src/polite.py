"""Bullet-proofed HTTP session for retail scraping.

Combines four bot-defense mitigations in one place:

1. **curl_cffi browser TLS impersonation** — the JA3/JA4 fingerprint matches
   real Chrome instead of plain Python.  Cloudflare/DataDome routinely block
   the latter on its own.

2. **Monte Carlo human-behavior timing** — three log-normal regimes
   (80% fast click-through, 15% reading pause, 5% deep dwell) plus a
   burst-then-pause pattern. Inter-request intervals vary like a real
   browsing session, not on a clean rhythm a heuristic can detect.

3. **Bot-defense awareness** — Content-Type validation rejects HTML where
   JSON was expected (catches Cloudflare interstitials returning 200).
   Honors Retry-After on 429/503. Treats 403 as a hard block (no retry).

4. **Kill switch + diagnostics** — aborts the run after N consecutive bad
   responses and logs cf-ray / cf-cache-status / vtex-rate-limit / elapsed
   on every response so we can audit drift.

Drop-in for any client that needs JSON-over-HTTP.
"""
import json
import math
import random
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from curl_cffi import requests as cffi


# ---------------------------------------------------------------------
# Monte Carlo human-behavior timing
# ---------------------------------------------------------------------

@dataclass
class HumanTiming:
    """Inter-request delay sampler that mimics human browsing.

    Three log-normal regimes:
        80% fast click-through  (~0.8-2.5s, mode ~1.2s)
        15% reading pause       (~3-10s)
         5% deep dwell          (~10-50s)

    Plus a burst-then-pause: every N=5..15 requests, insert a 5-30s pause.
    """
    fast_mu: float = math.log(1.2)
    fast_sigma: float = 0.30
    med_mu: float = math.log(5.0)
    med_sigma: float = 0.40
    long_mu: float = math.log(20.0)
    long_sigma: float = 0.50
    p_fast: float = 0.80
    p_med: float = 0.15

    min_floor: float = 0.7      # never go below this many seconds
    max_ceiling: float = 90.0   # cap pathological samples

    burst_size_min: int = 5
    burst_size_max: int = 15
    burst_pause_min: float = 5.0
    burst_pause_max: float = 30.0

    _bursts_remaining: int = 0

    def __post_init__(self):
        self._bursts_remaining = random.randint(
            self.burst_size_min, self.burst_size_max
        )

    def next_interval(self) -> float:
        """Compute the wait before the next request (may be a burst pause)."""
        if self._bursts_remaining <= 0:
            self._bursts_remaining = random.randint(
                self.burst_size_min, self.burst_size_max
            )
            return random.uniform(self.burst_pause_min, self.burst_pause_max)
        self._bursts_remaining -= 1

        r = random.random()
        if r < self.p_fast:
            sample = random.lognormvariate(self.fast_mu, self.fast_sigma)
        elif r < self.p_fast + self.p_med:
            sample = random.lognormvariate(self.med_mu, self.med_sigma)
        else:
            sample = random.lognormvariate(self.long_mu, self.long_sigma)
        return max(self.min_floor, min(self.max_ceiling, sample))


# ---------------------------------------------------------------------
# Polite session (curl_cffi-backed)
# ---------------------------------------------------------------------

class HardBlock(RuntimeError):
    """Raised when the run should be aborted (403 / kill-switch / repeated CF)."""


# Body markers that almost always indicate a Cloudflare challenge / interstitial
_CF_BODY_MARKERS = (
    "just a moment",
    "attention required",
    "cf-chl",
    "ray id:",
    "<!doctype html",
)

# Headers we tag every log entry with for later post-mortem
_DIAG_HEADERS = (
    "cf-ray", "cf-cache-status", "server", "retry-after",
    "content-type", "content-length",
    "x-vtex-cache", "x-vtex-rate-limit-limit", "x-vtex-rate-limit-remaining",
)


@dataclass
class PoliteSession:
    """JSON-aware, polite, bot-defense-resilient HTTP session.

    Use as a context manager:
        with PoliteSession(log_path=Path("logs/headers.jsonl")) as s:
            data = s.get_json("https://api.example.com/items?page=1")
    """
    user_agent: str = "mrakader-fashion-tracker/1.0 (+https://github.com/mrakader)"
    impersonate: str = "chrome124"
    timeout: float = 30.0
    max_retries: int = 4
    kill_switch_threshold: int = 3
    log_path: Path | None = None
    timing: HumanTiming = field(default_factory=HumanTiming)

    _session: Any = None
    _consecutive_bad: int = 0
    _last_request_t: float = 0.0
    _request_count: int = 0

    def __post_init__(self):
        self._session = cffi.Session(impersonate=self.impersonate)

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        self.close()

    def close(self) -> None:
        if self._session is not None:
            try:
                self._session.close()
            except Exception:
                pass
            self._session = None

    # --- public --------------------------------------------------------

    def warm_up(self, homepage_url: str) -> None:
        """Visit a homepage to acquire cookies (and a CDN-warm session).

        Mimics a real browser navigation. Sends an HTML Accept header (not the
        API JSON one) — bash.com's Cloudflare layer rejects the homepage if
        Accept is application/json.
        """
        html_headers = {
            "User-Agent": self.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-ZA,en;q=0.9",
        }
        try:
            r = self._session.get(
                homepage_url,
                headers=html_headers,
                timeout=self.timeout,
            )
            self._log_response(r, kind="warmup")
            self._last_request_t = time.monotonic()
        except Exception as e:
            print(f"  [warmup] non-fatal error contacting {homepage_url}: {e}")

    def get_json(self, url: str, **kw) -> Any:
        return self._request_json("GET", url, **kw)

    def post_json(self, url: str, **kw) -> Any:
        return self._request_json("POST", url, **kw)

    # --- internals -----------------------------------------------------

    def _base_headers(self) -> dict:
        return {
            "User-Agent": self.user_agent,
            "Accept": "application/json,text/plain;q=0.9,*/*;q=0.5",
            "Accept-Language": "en-ZA,en;q=0.9",
        }

    def _throttle(self) -> None:
        wait = self.timing.next_interval()
        elapsed = time.monotonic() - self._last_request_t
        if elapsed < wait:
            time.sleep(wait - elapsed)

    def _log_response(self, r, *, kind: str = "request") -> None:
        if self.log_path is None:
            return
        try:
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            entry = {
                "ts": time.time(),
                "kind": kind,
                "url": str(r.url),
                "status": r.status_code,
                "headers": {
                    k: r.headers.get(k)
                    for k in _DIAG_HEADERS
                    if r.headers.get(k) is not None
                },
            }
            with self.log_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(entry) + "\n")
        except Exception:
            pass

    def _validate_json(self, r) -> None:
        ct = (r.headers.get("content-type") or "").lower()
        body_first = (r.content[:200] or b"").lstrip()
        if not ("json" in ct or body_first.startswith(b"[") or body_first.startswith(b"{")):
            raise ValueError(
                f"non-JSON response (Content-Type={ct!r}, "
                f"body[:60]={body_first[:60]!r})"
            )
        body_lc = body_first.decode("utf-8", "replace").lower()
        for needle in _CF_BODY_MARKERS:
            if needle in body_lc:
                raise ValueError(
                    f"Cloudflare-style challenge body detected: {needle!r}"
                )

    def _record(self, ok: bool) -> None:
        if ok:
            self._consecutive_bad = 0
        else:
            self._consecutive_bad += 1
            if self._consecutive_bad >= self.kill_switch_threshold:
                raise HardBlock(
                    f"kill switch tripped after {self._consecutive_bad} "
                    "consecutive bad responses"
                )

    def _request_json(
        self,
        method: str,
        url: str,
        *,
        headers: dict | None = None,
        params: dict | None = None,
        json_body: dict | None = None,
    ) -> Any:
        h = dict(self._base_headers())
        if headers:
            h.update(headers)

        last_exc: Exception | None = None
        for attempt in range(self.max_retries):
            self._throttle()
            try:
                if method == "GET":
                    r = self._session.get(
                        url, headers=h, params=params, timeout=self.timeout
                    )
                else:
                    r = self._session.post(
                        url, headers=h, params=params,
                        json=json_body, timeout=self.timeout,
                    )
                self._last_request_t = time.monotonic()
                self._request_count += 1
                self._log_response(r)

                # 429 / 503 — back off, honor Retry-After
                if r.status_code in (429, 503):
                    ra = r.headers.get("retry-after")
                    wait = (
                        float(ra) if ra and ra.replace(".", "", 1).isdigit()
                        else 5 * (2 ** attempt)
                    )
                    wait = min(wait + 60, 300)  # buffer + cap
                    print(
                        f"  [back-off] {r.status_code} on attempt "
                        f"{attempt + 1}, sleeping {wait:.0f}s"
                    )
                    time.sleep(wait)
                    last_exc = RuntimeError(f"HTTP {r.status_code}")
                    continue

                # 403 — hard block, don't retry
                if r.status_code == 403:
                    self._record(False)
                    raise HardBlock("403 Forbidden — hard block")

                r.raise_for_status()
                self._validate_json(r)

                data = r.json()
                self._record(True)
                return data

            except HardBlock:
                raise
            except Exception as e:
                last_exc = e
                # only count as bad if we got a response and it failed validation
                # (network errors are also "bad" but more transient)
                self._record(False)
                wait = 2 * (2 ** attempt)
                time.sleep(wait)

        if last_exc:
            raise last_exc
        raise RuntimeError("_request_json: exhausted retries without exception")
