#!/usr/bin/env python3
import asyncio
from asyncio.subprocess import Process
import collections
import hashlib
import http.server
import os
from random import random
import re
import shutil
import sys
import tempfile
import threading
from typing import Any, Callable
import urllib.parse
import webbrowser
from pathlib import Path

import click
import httpx
import jwt
from pathvalidate import sanitize_filepath
from rich import box
from rich.console import Console, Group
from rich.live import Live
from rich.rule import Rule
from rich.table import Table
from rich.text import Text


# ── Config ──────────────────────────────────────────────────────────────────

VERSION = "1.2.4"
SERVER_URL = os.environ.get("MINERVA_SERVER", "https://api.minerva-archive.org")
UPLOAD_SERVER_URL = os.environ.get("MINERVA_UPLOAD_SERVER", "https://gate.minerva-archive.org")
TOKEN_FILE = Path.home() / ".minerva-dpn" / "token"
TEMP_DIR = Path.home() / ".minerva-dpn" / "tmp"
MAX_RETRIES = 3
RETRY_DELAY = 5
ARIA2C_SIZE_THRESHOLD = 1 * 1024 * 1024  # skip aria2c for files < 1 MB
QUEUE_PREFETCH = 2                       # queue depth = concurrency * this
HISTORY_LINES = 5                        # completed jobs shown above active table

os.environ["PATH"] += os.pathsep + os.path.abspath("thirdparty/aria2") # append bundled aria2 to path

console = Console()

# ── Auth ────────────────────────────────────────────────────────────────────


def auth_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "X-Minerva-Worker-Version": VERSION,
    }


def save_token(token: str):
    TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    TOKEN_FILE.write_text(token)


def load_token() -> str | None:
    if TOKEN_FILE.exists():
        t = TOKEN_FILE.read_text().strip()
        return t if t else None
    return None


def do_login(server_url: str) -> str:
    token = None
    event = threading.Event()

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            nonlocal token
            params = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            if "token" in params:
                token = params["token"][0]
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                self.wfile.write(b"<h2>Logged in! You can close this tab.</h2>")
                event.set()
            else:
                self.send_response(400)
                self.end_headers()

        def log_message(self, *a):
            pass

    srv = http.server.HTTPServer(("127.0.0.1", 19283), Handler)
    srv.timeout = 120

    url = f"{server_url}/auth/discord/login?worker_callback=http://127.0.0.1:19283/"
    console.print("[bold]Opening browser for Discord login...")
    console.print(f"[dim]If it doesn't open: {url}")
    webbrowser.open(url)

    while not event.is_set():
        srv.handle_request()
    srv.server_close()

    if not token:
        raise RuntimeError("Login failed")
    save_token(token)
    console.print("[bold green]Login successful!")
    return token


# ── Download ────────────────────────────────────────────────────────────────

HAS_ARIA2C = shutil.which("aria2c") is not None


def parse_aria2(filename: Path) -> dict[str, Any]:
    with open(filename, "rb") as fp:
        fp.read(2)  # version
        fp.read(4)  # ext
        infohashlength = int.from_bytes(fp.read(4), byteorder="big", signed=False)
        fp.read(infohashlength)

        piece_length = int.from_bytes(fp.read(4), byteorder="big", signed=False)
        total_length = int.from_bytes(fp.read(8), byteorder="big", signed=False)
        fp.read(8)  # upload_length
        bitfield_length = int.from_bytes(fp.read(4), byteorder="big", signed=False)
        bitfield = fp.read(bitfield_length)
        downloaded_chunks = int.from_bytes(bitfield, "big").bit_count()

        return {
            "filename": filename,
            "total_length": total_length,
            "downloaded_length": downloaded_chunks * piece_length,
            "total_chunks": bitfield_length * 8,
            "downloaded_chunks": downloaded_chunks,
        }


async def aria2_progress(filename: Path, proc: Process, on_progress: Callable[[int, int], int] | None = None) -> None:
    try:
        while proc.returncode is None:
            try:
                data = parse_aria2(filename.with_suffix(f"{filename.suffix}.aria2"))
                if on_progress:
                    on_progress(data["downloaded_length"], data["total_length"])
                await asyncio.sleep(1)
            except FileNotFoundError:
                pass
    except asyncio.CancelledError:
        pass


async def download_file(url: str, dest: Path, aria2c_connections: int = 16, known_size: int = 0, pre_allocation: str = "prealloc", on_progress=None) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    use_aria2c = HAS_ARIA2C and (known_size == 0 or known_size >= ARIA2C_SIZE_THRESHOLD)
    if use_aria2c:
        proc = await asyncio.create_subprocess_exec(
            "aria2c",
            f"--max-connection-per-server={aria2c_connections}",
            f"--split={aria2c_connections}",
            f"--file-allocation={pre_allocation}",
            "--min-split-size=1M",
            "--dir", str(dest.parent),
            "--out", dest.name,
            "--auto-file-renaming=false",
            "--allow-overwrite=true",
            "--console-log-level=warn",
            "--retry-wait=3",
            "--max-tries=5",
            "--timeout=120",
            "--connect-timeout=15",
            "--continue",
            url,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        aria2_controller = asyncio.create_task(aria2_progress(dest, proc, on_progress))
        try:
            _, stderr = await proc.communicate()
        finally:
            aria2_controller.cancel()
            await asyncio.gather(aria2_controller, return_exceptions=True)
        if proc.returncode != 0:
            raise RuntimeError(f"aria2c exit {proc.returncode}: {stderr.decode()[:200]}")
    else:
        # Fresh client per download — avoids stream/pool contention between workers
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=httpx.Timeout(connect=15, read=300, write=60, pool=10),
        ) as client:
            async with client.stream("GET", url) as resp:
                resp.raise_for_status()
                with open(dest, "wb") as f:
                    async for chunk in resp.aiter_bytes(1024 * 1024):
                        f.write(chunk)


# ── Upload ──────────────────────────────────────────────────────────────────

UPLOAD_CHUNK_SIZE = 8 * 1024 * 1024  # Cloudflare limit is 50 MB, but using 8 MB to reduce total chunks and speed up uploads
UPLOAD_START_RETRIES = 12
UPLOAD_CHUNK_RETRIES = 30
UPLOAD_FINISH_RETRIES = 12
REPORT_RETRIES = 20
RETRIABLE_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524}


def _retryable_status(code: int) -> bool:
    return code in RETRIABLE_STATUS_CODES


def _retry_sleep(attempt: int, cap: float = 25.0) -> float:
    return min(cap, (0.85 * attempt) + random() * 1.25)


def _raise_if_upgrade_required(resp: httpx.Response):
    if resp.status_code == 426:
        try:
            detail = resp.json().get("detail")
        except Exception:
            detail = resp.text.strip() or "Worker update required"
        raise RuntimeError(detail)


def _response_detail(resp: httpx.Response) -> str:
    try:
        body = resp.json()
        if isinstance(body, dict):
            detail = body.get("detail")
            if detail is not None:
                return str(detail)
    except Exception:
        pass
    return (resp.text or "").strip()


async def upload_file(upload_server_url: str, token: str, file_id: int, path: Path, on_progress=None) -> dict:
    # Fresh client per upload: multipart state must not be shared across coroutines
    headers = auth_headers(token)
    timeout = httpx.Timeout(connect=30, read=300, write=300, pool=30)
    async with httpx.AsyncClient(timeout=timeout) as client:
        # 1. Start session
        session_id = None
        for attempt in range(1, UPLOAD_START_RETRIES + 1):
            try:
                resp = await client.post(f"{upload_server_url}/api/upload/{file_id}/start", headers=headers)
                _raise_if_upgrade_required(resp)
                if _retryable_status(resp.status_code):
                    if attempt == UPLOAD_START_RETRIES:
                        raise RuntimeError(f"upload start failed ({resp.status_code})")
                    await asyncio.sleep(_retry_sleep(attempt))
                    continue
                resp.raise_for_status()
                session_id = resp.json()["session_id"]
                break
            except httpx.HTTPError as e:
                if attempt == UPLOAD_START_RETRIES:
                    raise RuntimeError(f"upload start failed ({e})") from e
                await asyncio.sleep(_retry_sleep(attempt))
        if not session_id:
            raise RuntimeError("Failed to create upload session")

        # 2. Send chunks
        file_size = path.stat().st_size
        sent = 0
        hasher = hashlib.sha256()
        with open(path, "rb") as f:
            while True:
                data = f.read(UPLOAD_CHUNK_SIZE)
                if not data:
                    break
                hasher.update(data)
                for attempt in range(1, UPLOAD_CHUNK_RETRIES + 1):
                    try:
                        resp = await client.post(
                            f"{upload_server_url}/api/upload/{file_id}/chunk",
                            params={"session_id": session_id},
                            headers={**headers, "Content-Type": "application/octet-stream"},
                            content=data,
                        )
                        _raise_if_upgrade_required(resp)
                        if _retryable_status(resp.status_code):
                            if attempt == UPLOAD_CHUNK_RETRIES:
                                raise RuntimeError(f"upload chunk failed ({resp.status_code})")
                            await asyncio.sleep(_retry_sleep(attempt, cap=20.0))
                            continue
                        resp.raise_for_status()
                        break
                    except httpx.HTTPError as e:
                        if attempt == UPLOAD_CHUNK_RETRIES:
                            raise RuntimeError(f"upload chunk failed ({e})") from e
                        await asyncio.sleep(_retry_sleep(attempt, cap=20.0))
                sent += len(data)
                if on_progress is not None:
                    on_progress(sent, file_size)

        # 3. Finish
        expected_sha256 = hasher.hexdigest()
        for attempt in range(1, UPLOAD_FINISH_RETRIES + 1):
            try:
                resp = await client.post(
                    f"{upload_server_url}/api/upload/{file_id}/finish",
                    params={
                        "session_id": session_id,
                        "expected_sha256": expected_sha256
                    },
                    headers=headers,
                )
                _raise_if_upgrade_required(resp)
                if _retryable_status(resp.status_code):
                    if attempt == UPLOAD_FINISH_RETRIES:
                        raise RuntimeError(f"upload finish failed ({resp.status_code})")
                    await asyncio.sleep(_retry_sleep(attempt, cap=20.0))
                    continue
                resp.raise_for_status()
                result = resp.json()
                break
            except httpx.HTTPError as e:
                if attempt == UPLOAD_FINISH_RETRIES:
                    raise RuntimeError(f"upload finish failed ({e})") from e
                await asyncio.sleep(_retry_sleep(attempt, cap=20.0))

        return result


async def report_job(server_url: str, token: str, file_id: int, status: str,
                     bytes_downloaded: int | None = None, error: str | None = None):
    async with httpx.AsyncClient(timeout=30) as client:
        for attempt in range(1, REPORT_RETRIES + 1):
            try:
                resp = await client.post(
                    f"{server_url}/api/jobs/report",
                    headers=auth_headers(token),
                    json={
                        "file_id": file_id,
                        "status": status,
                        "bytes_downloaded": bytes_downloaded,
                        "error": error
                    },
                )
                _raise_if_upgrade_required(resp)
                if resp.status_code == 401:
                    raise RuntimeError("Token expired. Run: python worker.py login")
                if resp.status_code == 409 and status == "completed":
                    # Async finalize race: upload accepted, but finalize/verify not visible yet.
                    detail = _response_detail(resp).lower()
                    if "not finalized" in detail or "upload" in detail:
                        if attempt == REPORT_RETRIES:
                            resp.raise_for_status()
                        await asyncio.sleep(min(2.0, 0.25 + attempt * 0.1))
                        continue
                if _retryable_status(resp.status_code):
                    if attempt == REPORT_RETRIES:
                        resp.raise_for_status()
                    await asyncio.sleep(_retry_sleep(attempt, cap=20.0))
                    continue
                resp.raise_for_status()
                return
            except httpx.HTTPError as e:
                if attempt == REPORT_RETRIES:
                    raise
                await asyncio.sleep(_retry_sleep(attempt, cap=20.0))


# ── Live UI ──────────────────────────────────────────────────────────────────


def _fmt_bytes(n: int | float) -> str:
    for unit in ("B", "K", "M", "G"):
        if n < 1024:
            return f"{n:.0f}{unit}"
        n = n / 1024
    return f"{n:.1f}T"


class WorkerDisplay:
    """
    Fixed-height terminal display:
      • HISTORY_LINES of recent completed/failed jobs (oldest scrolls off)
      • divider rule
      • one row per active worker slot
    """

    def __init__(self):
        self.history: collections.deque = collections.deque(maxlen=HISTORY_LINES)
        self.active: dict = {}  # file_id -> dict
        self._lock = threading.Lock()

    def job_start(self, file_id: int, label: str):
        with self._lock:
            self.active[file_id] = dict(label=label, status="DL", size=0, done=0)

    def job_update(self, file_id: int, status: str, size: int = 0, done: int = 0):
        with self._lock:
            if file_id in self.active:
                self.active[file_id].update(status=status, size=size, done=done)

    def job_done(self, file_id: int, label: str, ok: bool, note: str = ""):
        with self._lock:
            self.active.pop(file_id, None)
            icon = "[green]✓[/green]" if ok else "[red]✗[/red]"
            color = "green" if ok else "red"
            entry = f"{icon} [{color}]{label}[/{color}]"
            if note:
                entry += f"  [dim]{note}[/dim]"
            self.history.append(entry)

    def __rich__(self):
        # History panel — always HISTORY_LINES tall so active table stays anchored
        lines = list(self.history)
        while len(lines) < HISTORY_LINES:
            lines.insert(0, "[dim]—[/dim]")

        # Active jobs table
        table = Table(box=box.SIMPLE, show_header=True, expand=True,
                      header_style="bold dim", padding=(0, 1))
        table.add_column("", width=3)        # status badge
        table.add_column("File")
        table.add_column("Size", width=7, justify="right")
        table.add_column("Progress", width=24)

        with self._lock:
            snapshot = list(self.active.values())

        for info in snapshot:
            st = info["status"]
            color = {"DL": "cyan", "UL": "yellow", "RT": "magenta"}.get(st, "white")
            size = info["size"]
            done = info["done"]

            if size:
                pct = done / size
                bar_w = 14
                filled = int(bar_w * pct)
                bar = (f"[{color}]" + "█" * filled + f"[/{color}]" +
                       "[dim]" + "░" * (bar_w - filled) + "[/dim]" +
                       f" {pct * 100:4.0f}%")
            else:
                bar = "[dim]working…[/dim]"

            table.add_row(
                f"[{color}]{st}[/{color}]",
                info["label"],
                _fmt_bytes(size) if size else "?",
                bar,
            )

        return Group(
            *[Text.from_markup(l) for l in lines],
            Rule(style="dim"),
            table,
        )


# ── Job processing ──────────────────────────────────────────────────────────

_STOP = object()


async def process_job(
    server_url: str,
    upload_server_url: str,
    token: str,
    job: dict,
    temp_dir: Path,
    keep_files: bool,
    aria2c_connections: int,
    pre_allocation: str,
    display: WorkerDisplay
):
    file_id = job["file_id"]
    url = job["url"]
    dest_path = job["dest_path"]
    label = urllib.parse.unquote(dest_path if len(dest_path) <= 60 else "…" + dest_path[-57:])
    known_size = job.get("size", 0) or 0

    display.job_start(file_id, label)
    last_err: Exception

    # get local file path, mirroring URL path to avoid collisions, and sanitize for NTFS
    # decodes percent-encoded characters, removes invalid characters for Windows paths
    try:
        parsed_url = urllib.parse.urlparse(url)
        url_path = urllib.parse.unquote(parsed_url.path).lstrip("/")
        unsafe_local_path = temp_dir / parsed_url.netloc / url_path
        local_path = sanitize_filepath(unsafe_local_path, platform="auto", normalize=True)
    except ValueError as e:
        last_err = e
        display.job_done(file_id, label, ok=False, note=f"Invalid filename: {e}")
        try:
            await report_job(server_url, token, file_id, "failed", error=str(last_err)[:500])
        except Exception:
            pass
        console.print(f"[red]  {dest_path}: Invalid filename: {e}")
        return

    last_err = None
    file_size = 0
    uploaded = False

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # Download
            display.job_update(file_id, "DL")
            await download_file(
                url, local_path, aria2c_connections, known_size, pre_allocation,
                on_progress=lambda done, size: display.job_update(file_id, "DL", size=size, done=done)
            )
            file_size = local_path.stat().st_size
            # Upload
            display.job_update(file_id, "UL", size=file_size)
            await upload_file(
                upload_server_url, token, file_id, local_path,
                on_progress=lambda done, size: display.job_update(file_id, "UL", size=size, done=done)
            )
            await report_job(server_url, token, file_id, "completed", bytes_downloaded=file_size)
            uploaded = True
            break
        except Exception as e:
            last_err = e
            if attempt < MAX_RETRIES:
                display.job_update(file_id, "RT")
                await asyncio.sleep(RETRY_DELAY * attempt)

    if not uploaded:
        # All retries exhausted on download/upload path
        display.job_done(file_id, label, ok=False, note=f"[{MAX_RETRIES} attempts] {str(last_err)}")
        try:
            await report_job(server_url, token, file_id, "failed", error=str(last_err)[:500])
        except Exception:
            pass
        local_path.unlink(missing_ok=True)
        return

    # Surface success to user immediately once upload bytes + finish call succeeded
    display.job_done(file_id, label, ok=True, note=_fmt_bytes(file_size))
    if not keep_files:
        local_path.unlink(missing_ok=True)

    # Best-effort completion report; do not re-run transfer on control-plane flakiness.
    try:
        await report_job(server_url, token, file_id, "completed", bytes_downloaded=file_size)
    except Exception as e:
        console.print(f"[yellow]  {dest_path}: uploaded but report delayed ({str(e)[:120]})")


# ── Main Loop ───────────────────────────────────────────────────────────────


async def worker_loop(
    server_url: str,
    upload_server_url: str,
    token: str,
    temp_dir: Path,
    concurrency: int,
    batch_size: int,
    aria2c_connections: int,
    pre_allocation: str,
    keep_files: bool
):
    token_dec = jwt.decode(token, options={"verify_signature": False})

    console.print(f"Username:      {token_dec.get('username', '-')}")
    console.print(f"Job Gateway:   {server_url}")
    console.print(f"Upload Server: {upload_server_url}")
    console.print(f"Concurrency:   {concurrency}")
    console.print(f"Retries:       {MAX_RETRIES}")
    console.print(f"Keep files:    {'yes' if keep_files else 'no'}")
    console.print(f"Downloader:    {f'aria2c ({aria2c_connections} conns/job), httpx if file <{ARIA2C_SIZE_THRESHOLD // (1024*1024)}MB' if HAS_ARIA2C else 'httpx'}")
    console.print()

    temp_dir.mkdir(parents=True, exist_ok=True)

    display = WorkerDisplay()

    queue: asyncio.Queue = asyncio.Queue(maxsize=concurrency * QUEUE_PREFETCH)
    stop_event = asyncio.Event()
    seen_ids: set[int] = set()

    # ── Producer ────────────────────────────────────────────────────────────
    async def producer():
        no_jobs_warned = False
        async with httpx.AsyncClient(timeout=30) as api:
            while not stop_event.is_set():
                if queue.qsize() >= concurrency:
                    await asyncio.sleep(0.5)
                    continue

                free_slots = max(1, queue.maxsize - queue.qsize())
                fetch_count = min(4, batch_size, free_slots)
                try:
                    resp = await api.get(
                        f"{server_url}/api/jobs",
                        params={"count": fetch_count},
                        headers=auth_headers(token),
                    )
                    if resp.status_code == 426:
                        _raise_if_upgrade_required(resp)
                    if resp.status_code == 401:
                        console.print("[red]Token expired. Run: python worker.py login")
                        stop_event.set()
                        break
                    resp.raise_for_status()
                    jobs = resp.json().get("jobs", [])

                    if not jobs:
                        if not no_jobs_warned:
                            console.print("[dim]No jobs available, waiting 30s…")
                            no_jobs_warned = True
                        await asyncio.sleep(12 + random() * 8)
                        continue

                    no_jobs_warned = False
                    for job in jobs:
                        file_id = job["file_id"]
                        if file_id in seen_ids:
                            continue
                        seen_ids.add(file_id)
                        await queue.put(job)

                except httpx.HTTPError as e:
                    console.print(f"[red]Server error: {e}. Retrying in 10s…")
                    await asyncio.sleep(6 + random() * 4)

        for _ in range(concurrency):
            await queue.put(_STOP)

    # ── Workers ─────────────────────────────────────────────────────────────
    async def worker():
        while True:
            job = await queue.get()
            if job is _STOP:
                queue.task_done()
                return
            try:
                await process_job(server_url, upload_server_url, token, job, temp_dir, keep_files, aria2c_connections, pre_allocation, display)
            finally:
                seen_ids.discard(job["file_id"])
                queue.task_done()

    with Live(display, console=console, refresh_per_second=4, screen=False):
        workers = [asyncio.create_task(worker()) for _ in range(concurrency)]
        producer_task = asyncio.create_task(producer())
        try:
            await asyncio.gather(producer_task, *workers)
        except KeyboardInterrupt:
            console.print("\n[yellow]Shutting down…")
            stop_event.set()
            producer_task.cancel()
            for t in workers:
                t.cancel()
            return


# ── CLI ─────────────────────────────────────────────────────────────────────


@click.group(invoke_without_command=True)
@click.pass_context
def cli(ctx):
    """Minerva Worker — help archive the internet."""
    console.print(f"[bold green]Minerva Worker v{VERSION}[/bold green]")
    if ctx.invoked_subcommand is None:
        ctx.invoke(run)


@cli.command()
@click.option("--server", default=SERVER_URL, help="Manager server URL")
def login(server) -> str:
    """Authenticate with Discord."""
    return do_login(server)


@cli.command()
@click.pass_context
@click.option("--server", default=SERVER_URL, help="Manager server URL")
@click.option("--upload-server", default=UPLOAD_SERVER_URL, help="Upload API URL")
@click.option("-c", "--concurrency", default=2, help="Concurrent downloads")
@click.option("-b", "--batch-size", default=10, help="Max files to fetch per API call")
@click.option("-a", "--aria2c-connections", default=8, help="aria2c connections per file")
@click.option("-p", "--pre-allocation", default="prealloc", help="Pre-allocation method when using aria2c (prealloc, falloc, none)")
@click.option("--temp-dir", default=str(TEMP_DIR), help="Temp download dir")
@click.option("--keep-files", is_flag=True, help="Keep downloaded files after upload")
def run(ctx, server, upload_server, concurrency, batch_size, aria2c_connections, pre_allocation, temp_dir, keep_files):
    """Start downloading and uploading files."""
    # Ensure user is logged-in first
    token = load_token()
    if not token:
        token = ctx.invoke(login, server=server)
    if not token:
        console.print("[red]Could not login, please try again...")
        return

    # start main loop
    asyncio.run(
        worker_loop(
            server,
            upload_server,
            token,
            Path(temp_dir),
            concurrency,
            batch_size,
            aria2c_connections,
            pre_allocation,
            keep_files
        )
    )


@cli.command()
def status():
    """Show login status."""
    token = load_token()
    console.print("[green]Logged in" if token else "[red]Not logged in")


if __name__ == "__main__":
    cli()
