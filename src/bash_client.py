"""Polite VTEX REST client for bash.com (mens scope).

Important lesson from earlier projects: bash.com's Cloudflare layer requires
a valid Chrome TLS fingerprint AND a homepage warmup to seed cookies.
PoliteSession.warm_up() handles the warmup; curl_cffi handles the TLS.
"""
from pathlib import Path

from src import config
from src.polite import HumanTiming, PoliteSession


class BashClient:
    def __init__(self, *, log_path: Path | None = None) -> None:
        self._session = PoliteSession(
            log_path=log_path,
            timing=HumanTiming(),
            user_agent=config.USER_AGENT,
        )

    def __enter__(self):
        self._session.warm_up(config.HOMEPAGE)
        return self

    def __exit__(self, *_exc):
        self._session.close()

    def close(self) -> None:
        self._session.close()

    def get_page(self, *, path: str, _from: int, _to: int) -> list[dict]:
        url = f"{config.TFG_HOST}{config.SEARCH_PATH}/{path.lstrip('/')}"
        items = self._session.get_json(
            url, params={"_from": _from, "_to": _to},
        )
        if not isinstance(items, list):
            raise ValueError(f"expected JSON array, got {type(items).__name__}")
        return items
