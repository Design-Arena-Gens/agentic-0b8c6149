import argparse
import asyncio
import sqlite3
import sys
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, List, Optional
from urllib.parse import urljoin, urlparse
import urllib.robotparser as robotparser

import aiohttp


@dataclass
class FetchResult:
    url: str
    status: Optional[int]
    error: Optional[str]
    content: Optional[bytes]
    fetched_at: datetime


class RateLimiter:
    """Simple sliding-window rate limiter for asyncio.

    Allows up to `max_calls` per `interval_seconds` across awaiters.
    """

    def __init__(self, max_calls: int, interval_seconds: float) -> None:
        if max_calls <= 0 or interval_seconds <= 0:
            raise ValueError("max_calls and interval_seconds must be > 0")
        self.max_calls = max_calls
        self.interval = interval_seconds
        self._timestamps: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            loop = asyncio.get_running_loop()
            now = loop.time()
            # Drop old timestamps outside the window
            while self._timestamps and (now - self._timestamps[0]) > self.interval:
                self._timestamps.popleft()
            # If full, wait until earliest timestamp falls out
            if len(self._timestamps) >= self.max_calls:
                wait_seconds = self.interval - (now - self._timestamps[0])
                if wait_seconds > 0:
                    await asyncio.sleep(wait_seconds)
                # Recalculate after sleeping
                now = loop.time()
                while self._timestamps and (now - self._timestamps[0]) > self.interval:
                    self._timestamps.popleft()
            self._timestamps.append(now)


class DatabaseManager:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._conn = sqlite3.connect(self.db_path)
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                status INTEGER,
                error TEXT,
                content BLOB,
                fetched_at TEXT NOT NULL
            );
            """
        )
        self._conn.commit()

    def insert(self, result: FetchResult) -> None:
        self._conn.execute(
            "INSERT INTO results (url, status, error, content, fetched_at) VALUES (?, ?, ?, ?, ?)",
            (
                result.url,
                result.status,
                result.error,
                result.content,
                result.fetched_at.isoformat(),
            ),
        )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()


async def fetch_one(
    session: aiohttp.ClientSession,
    url: str,
    rate_limiter: RateLimiter,
    timeout_seconds: int,
    max_retries: int,
    backoff_base_seconds: float,
) -> FetchResult:
    await rate_limiter.acquire()
    fetched_at = datetime.now(timezone.utc)

    last_error: Optional[str] = None
    for attempt in range(max_retries + 1):
        try:
            async with session.get(url, timeout=timeout_seconds) as resp:
                content = await resp.read()
                return FetchResult(
                    url=url,
                    status=resp.status,
                    error=None,
                    content=content,
                    fetched_at=fetched_at,
                )
        except Exception as exc:  # Narrowed by aiohttp exceptions in real-world usage
            last_error = f"{type(exc).__name__}: {exc}"
            # Exponential backoff
            await asyncio.sleep(backoff_base_seconds * (2 ** attempt))
    return FetchResult(url=url, status=None, error=last_error, content=None, fetched_at=fetched_at)


def load_robots_txt(base_url: str, respect_robots: bool) -> Optional[robotparser.RobotFileParser]:
    if not respect_robots:
        return None
    parsed = urlparse(base_url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    rp = robotparser.RobotFileParser()
    try:
        rp.set_url(robots_url)
        rp.read()
        return rp
    except Exception:
        # If robots cannot be read, default to conservative behavior (disallow)
        return rp


def is_allowed_by_robots(rp: Optional[robotparser.RobotFileParser], url: str) -> bool:
    if rp is None:
        return True
    try:
        return rp.can_fetch("*", url)
    except Exception:
        return False


async def worker(
    name: int,
    queue: "asyncio.Queue[str]",
    session: aiohttp.ClientSession,
    db: DatabaseManager,
    rate_limiter: RateLimiter,
    timeout_seconds: int,
    max_retries: int,
    backoff_base_seconds: float,
    rp: Optional[robotparser.RobotFileParser],
) -> None:
    while True:
        url = await queue.get()
        try:
            if is_allowed_by_robots(rp, url):
                result = await fetch_one(
                    session=session,
                    url=url,
                    rate_limiter=rate_limiter,
                    timeout_seconds=timeout_seconds,
                    max_retries=max_retries,
                    backoff_base_seconds=backoff_base_seconds,
                )
            else:
                result = FetchResult(
                    url=url,
                    status=None,
                    error="Blocked by robots.txt",
                    content=None,
                    fetched_at=datetime.now(timezone.utc),
                )
            db.insert(result)
        finally:
            queue.task_done()


def build_urls(base_url: Optional[str], paths_file: Optional[str], urls_file: Optional[str]) -> List[str]:
    urls: List[str] = []
    if urls_file:
        with open(urls_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    urls.append(line)
    if base_url and paths_file:
        with open(paths_file, "r", encoding="utf-8") as f:
            for line in f:
                path = line.strip()
                if path:
                    urls.append(urljoin(base_url, path))
    if not urls:
        raise ValueError("No URLs provided. Use --urls-file or --base-url with --paths-file.")
    return urls


async def run(
    urls: Iterable[str],
    db_path: str,
    concurrency: int,
    requests_per_second: int,
    timeout_seconds: int,
    max_retries: int,
    backoff_base_seconds: float,
    base_url: Optional[str],
    respect_robots: bool,
) -> None:
    db = DatabaseManager(db_path)
    try:
        rp = load_robots_txt(base_url, respect_robots) if base_url else (load_robots_txt(list(urls)[0], respect_robots) if respect_robots else None)
        rate_limiter = RateLimiter(max_calls=requests_per_second, interval_seconds=1.0)
        queue: asyncio.Queue[str] = asyncio.Queue()
        for u in urls:
            await queue.put(u)
        timeout = aiohttp.ClientTimeout(total=None)
        connector = aiohttp.TCPConnector(limit=concurrency)
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            workers = [
                asyncio.create_task(
                    worker(
                        i,
                        queue,
                        session,
                        db,
                        rate_limiter,
                        timeout_seconds,
                        max_retries,
                        backoff_base_seconds,
                        rp,
                    )
                )
                for i in range(concurrency)
            ]
            await queue.join()
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
    finally:
        db.close()


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Ethical, rate-limited collector. Only crawl domains you own or have permission for."
        )
    )
    src = parser.add_argument_group("Sources")
    src.add_argument("--urls-file", type=str, default=None, help="File with absolute URLs (one per line)")
    src.add_argument("--base-url", type=str, default=None, help="Base URL to join with paths (e.g., https://example.com)")
    src.add_argument("--paths-file", type=str, default=None, help="File with path fragments (one per line)")

    out = parser.add_argument_group("Output")
    out.add_argument("--db-path", type=str, default="./data.sqlite", help="SQLite database file path")

    perf = parser.add_argument_group("Performance & Safety")
    perf.add_argument("--concurrency", type=int, default=10, help="Concurrent workers")
    perf.add_argument("--requests-per-second", type=int, default=5, help="Global requests per second limit")
    perf.add_argument("--timeout-seconds", type=int, default=15, help="Per-request timeout in seconds")
    perf.add_argument("--max-retries", type=int, default=2, help="Max retries per request")
    perf.add_argument("--backoff-base-seconds", type=float, default=0.5, help="Base seconds for exponential backoff")
    perf.add_argument("--respect-robots", type=str, default="true", choices=["true", "false"], help="Respect robots.txt rules")

    args = parser.parse_args(argv)

    if not args.urls_file and not (args.base_url and args.paths_file):
        parser.error("Provide --urls-file OR both --base-url and --paths-file")

    # Basic sanity checks
    if args.requests_per_second <= 0 or args.concurrency <= 0:
        parser.error("--requests-per-second and --concurrency must be > 0")

    return args


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    urls = build_urls(args.base_url, args.paths_file, args.urls_file)

    try:
        asyncio.run(
            run(
                urls=urls,
                db_path=args.db_path,
                concurrency=args.concurrency,
                requests_per_second=args.requests_per_second,
                timeout_seconds=args.timeout_seconds,
                max_retries=args.max_retries,
                backoff_base_seconds=args.backoff_base_seconds,
                base_url=args.base_url,
                respect_robots=(args.respect_robots.lower() == "true"),
            )
        )
        return 0
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
