#!/usr/bin/env python3
"""
A_txid_collector.py

Collects ordered transaction IDs (txids), in-block order, for every Bitcoin
mainnet block in a height range, using the mempool.space public REST API.

Output: /<script folder>/transaction_ID/<start>-<end>.parquet files, each
capped at ~1 GiB. Two columns:
    marker (int64) : block height on a "block marker" row, OR a 1-based
                      tx counter on a transaction row.
    txid   (string): "" on a marker row, the txid hex string on a tx row.

Rows are always written to the current file in strict block/tx order
(this is required to later reconstruct merkle roots).

--- Crash-safety note (read this before assuming file layout) --------------
Parquet files are only valid/readable once their footer is written at
close(). That means a parquet file that is still "being filled" cannot
safely be the thing we resume from after a hard crash -- it may have no
footer at all. So the *currently open* file is not a real .parquet on disk
yet: it lives in a plain-text, crash-safe write-ahead log (WAL) inside
transaction_ID/ (one line per row, fsynced after every completed block).
On startup we replay the WAL (a local file read -- no network request) to
rebuild in-memory state and know exactly which block to resume from. Once
a file's accumulated data would cross the 1 GiB cap, we finalize it as a
real, closed, correctly-named .parquet file and start a fresh WAL for the
next file. This preserves your intended behavior (capped .parquet files,
in-order rows, no wasted requests on resume) while avoiding the failure
mode of a parquet file that's unreadable because it got cut off mid-write.
-----------------------------------------------------------------------------
"""

import argparse
import io
import json
import logging
import logging.handlers
import os
import sys
import time
import threading
import hashlib
import queue
import shutil
from collections import deque
from contextlib import nullcontext
from pathlib import Path

import requests
import pyarrow as pa
import pyarrow.parquet as pq

try:
    import psutil
    HAVE_PSUTIL = True
except ImportError:
    HAVE_PSUTIL = False

try:
    import msvcrt  # Windows stdlib -- non-blocking single-key reads, no pip needed
    HAVE_MSVCRT = True
except ImportError:
    HAVE_MSVCRT = False

from rich.console import Console
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.progress_bar import ProgressBar
from rich.text import Text
from rich.align import Align

# ============================================================================
# Config (all overridable via CLI flags -- see build_arg_parser())
# ============================================================================

if getattr(sys, "frozen", False):
    SCRIPT_DIR = Path(sys.executable).resolve().parent
    ROOT_DIR = SCRIPT_DIR
else:
    SCRIPT_DIR = Path(__file__).resolve().parent
    ROOT_DIR = SCRIPT_DIR.parent
DEFAULT_OUTPUT_DIR_NAME = "data"
LEGACY_OUTPUT_DIR_NAME = "transaction_ID"  # pre-restructure default; auto-migrated below

DEFAULT_START_HEIGHT = 1
DEFAULT_END_HEIGHT = 958052

DEFAULT_API_BASE = "https://mempool.space/api"

DEFAULT_FILE_CAP_BYTES = 1_073_741_824  # 1 GiB
CHEAP_SIZE_CHECK_FRACTION = 0.90        # start exact-checking size at 90% of cap
EXACT_CHECK_INTERVAL = 10                # only run expensive exact-size check every N blocks past 90%

DEFAULT_MAX_THREADS = 8
DEFAULT_MULTI_TARGET_RPS = 8.0
DEFAULT_SINGLE_FALLBACK_RPS = 3.0

# AIMD: +5%/10s while clean, x0.70 on a 429, 5s pause escalating on repeats.
AIMD_DECREASE = 0.70
AIMD_INCREASE_FRACTION = 0.05
AIMD_INCREASE_MIN = 0.5
AIMD_AI_INTERVAL = 10.0
AIMD_PAUSE_BASE = 5.0
AIMD_PAUSE_MAX = 160.0
AIMD_CLEAN_RESET = 120.0

DEFAULT_REFRESH_PER_SECOND = 170

WAL_GLOB_PATTERN = "WIP_*.wal"
CHECKPOINT_FILENAME = "checkpoint.json"

REQUEST_TIMEOUT = 15
MAX_RETRIES_PER_REQUEST = 6

LOG_FILENAME = "collector.log"
STATS_JSON_FILENAME = "stats.json"
DEFAULT_STATS_JSON_INTERVAL = 5.0

RATE_STEP = 1.0          # how much +/- adjusts multi_rps per keypress (legacy)
MIN_RATE = 0.5

MULTIPLIER_STEP = 0.1    # how much +/- / arrow keys adjust the speed multiplier
MIN_MULTIPLIER = 0.1     # floor for the speed multiplier

# --- Adaptive controller tuning (see main loop) ----------------------------
ADAPTIVE_DEADBAND_BPS = 0.15      # ignore rate errors smaller than this
ADAPTIVE_MIN_INTERVAL = 4.0       # seconds between multiplier adjustments
ADAPTIVE_MAX_STEP = 0.15          # max multiplier change per adjustment
ADAPTIVE_IDLE_FLOOR_MULT = 0.5    # idle-worker slowdown never goes below this

# --- Learned rate ceiling (persisted by A_speed_probe.py) ------------------
SPEED_PROFILE_FILENAME = "speed_profile.json"
RATE_FAILURE_SAFETY = 0.70        # ceiling = failing_rate * this

SPARKLINE_CHARS = "▁▂▃▄▅▆▇█"

THEMES = {
    "dark":  {"header": "cyan",    "progress": "green",   "network": "magenta",
              "file": "blue",      "system": "white",      "controls": "yellow",
              "recent": "cyan",    "threads": "blue",      "errors": "red",
              "latency": "magenta","compression": "green", "io": "white",
              "session": "cyan"},
    "neon":  {"header": "bright_magenta", "progress": "bright_green", "network": "bright_cyan",
              "file": "bright_blue", "system": "bright_white", "controls": "bright_yellow",
              "recent": "bright_magenta", "threads": "bright_blue", "errors": "bright_red",
              "latency": "bright_cyan", "compression": "bright_green", "io": "bright_white",
              "session": "bright_magenta"},
    "mono":  {"header": "white", "progress": "white", "network": "white",
              "file": "white", "system": "white", "controls": "white",
              "recent": "white", "threads": "white", "errors": "white",
              "latency": "white", "compression": "white", "io": "white",
              "session": "white"},
}


def build_arg_parser():
    p = argparse.ArgumentParser(description="Collect ordered Bitcoin txids per block into capped parquet files.")
    p.add_argument("--start-height", type=int, default=DEFAULT_START_HEIGHT)
    p.add_argument("--end-height", type=int, default=DEFAULT_END_HEIGHT)
    p.add_argument("--output-dir", type=str, default=None,
                   help="Defaults to <script folder>/transaction_ID")
    p.add_argument("--max-blocks", type=int, default=None,
                   help="Process at most N blocks from the (resume) start height, then exit. For safe testing.")
    p.add_argument("--dry-run", action="store_true",
                   help="Fetch only the first 3 blocks into a dryrun/ subdir and exit. Validates the full pipeline.")
    p.add_argument("--verify-ntx", action="store_true",
                   help="Verify fetched txid count against the block header tx_count (one extra request per block).")
    p.add_argument("--store", choices=["txids", "roots", "headers"], default="txids",
                   help="what to keep per block: every txid, one merkle root, or the header")
    p.add_argument("--root-only", action="store_true",
                   help="alias for --store roots")
    p.add_argument("--quiet", action="store_true",
                   help="No fullscreen dashboard: print a one-line status update instead. stats.json is still written.")
    p.add_argument("--api-base", type=str, default=DEFAULT_API_BASE)
    p.add_argument("--file-cap-bytes", type=int, default=DEFAULT_FILE_CAP_BYTES)
    p.add_argument("--max-threads", type=int, default=DEFAULT_MAX_THREADS)
    p.add_argument("--multi-rps", type=float, default=DEFAULT_MULTI_TARGET_RPS)
    p.add_argument("--single-rps", type=float, default=DEFAULT_SINGLE_FALLBACK_RPS)
    p.add_argument("--refresh-rate", type=float, default=DEFAULT_REFRESH_PER_SECOND)
    p.add_argument("--no-fullscreen", action="store_true",
                   help="Disable the alternate-screen (fullscreen) UI mode.")
    p.add_argument("--theme", type=str, default="dark", choices=list(THEMES.keys()),
                   help="Panel border color theme.")
    p.add_argument("--minimal", action="store_true",
                   help="dashboard shows only progress, ETA and speed")
    p.add_argument("--compact", action="store_true",
                   help="Show only the essential panels (smaller terminal friendly).")
    p.add_argument("--verbose", action="store_true",
                   help="Show raw HTTP status/timing debug panel and log at DEBUG level.")
    p.add_argument("--log-file", type=str, default=None,
                   help="Defaults to <output-dir>/collector.log")
    p.add_argument("--stats-json-interval", type=float, default=DEFAULT_STATS_JSON_INTERVAL,
                   help="Seconds between stats.json exports (0 disables).")
    return p


# ============================================================================
# Rate limiter (shared token bucket, rate is mutable at runtime)
# ============================================================================

class RateLimiter:
    def __init__(self, rate_per_sec):
        self._lock = threading.Lock()
        self.rate = rate_per_sec
        self._last = time.monotonic()
        self._allowance = rate_per_sec
        self._pause_until = 0.0

    def set_rate(self, rate_per_sec):
        with self._lock:
            self.rate = max(0.1, rate_per_sec)

    def pause_for(self, seconds):
        with self._lock:
            self._pause_until = max(self._pause_until, time.monotonic() + seconds)

    def acquire(self):
        while True:
            with self._lock:
                now = time.monotonic()
                if now < self._pause_until:
                    wait = self._pause_until - now
                    self._last = self._pause_until
                else:
                    self._allowance += (now - self._last) * self.rate
                    self._last = now
                    if self._allowance > self.rate:
                        self._allowance = self.rate
                    if self._allowance >= 1.0:
                        self._allowance -= 1.0
                        return
                    wait = (1.0 - self._allowance) / self.rate
            time.sleep(min(max(wait, 0.005), 5.0))


class RateLimited(Exception):
    """Raised when the API responds 429 (or equivalent throttling signal).
    Carries the server's Retry-After hint (seconds) when present, so backoff
    can honor it instead of the fixed ladder."""

    def __init__(self, path, retry_after_s=None):
        super().__init__(path)
        self.path = path
        self.retry_after_s = retry_after_s


def parse_retry_after(value):
    """Parse a Retry-After header value -> seconds (float) or None.
    Accepts delta-seconds or an HTTP-date."""
    if value is None:
        return None
    value = str(value).strip()
    if value.isdigit():
        return float(value)
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(value)
        if dt is None:
            return None
        return max(0.0, dt.timestamp() - time.time())
    except Exception:
        return None


# ============================================================================
# Mode controller: single-threaded <-> multi-threaded, with backoff ladder
# ============================================================================

class ModeController:
    """
    Owns how many worker slots are currently allowed to fetch concurrently.
    Starts multi-threaded. On a 429 anywhere, immediately drops to 1 active
    worker (single-threaded fallback) and schedules a retry of multi-threaded
    mode using the backoff ladder. If a retry attempt gets blocked again
    quickly, it advances to the next (longer) backoff stage. A sustained
    clean stretch in multi-threaded mode resets the ladder back to stage 0.
    """

    def __init__(self, max_threads, multi_rps, single_rps, rate_limiter, on_blocked=None):
        self.max_threads = max_threads
        self.multi_rps = multi_rps
        self.single_rps = single_rps
        self.rate_limiter = rate_limiter
        # Called with the request rate that just got throttled. Returns (or
        # sets) the new hard ceiling in req/s, or None.
        self.on_blocked = on_blocked
        self.max_rps = None          # learned hard ceiling, None = unknown

        self._lock = threading.Lock()
        self.mode = "multi"
        self.allowed_workers = max_threads
        self.backoff_stage = 0
        self.next_retry_at = None
        self.last_block_event_at = time.monotonic()
        self.last_event_desc = "startup"
        self.rate_limiter.set_rate(multi_rps)

        self.consec_blocks = 0
        self.last_blocked_at = 0.0
        self.ai_last = time.monotonic()
        self.auto_ai = True

        # Speed multiplier: displayed as x1.00 by default.
        # Actual RPS = base_rps * rate_multiplier.
        self.rate_multiplier = 1.0
        self.base_rps = multi_rps

    def _clamp_mult(self, mult):
        """Clamp a multiplier to [MIN_MULTIPLIER, learned ceiling]."""
        mult = max(MIN_MULTIPLIER, mult)
        if self.max_rps:
            mult = min(mult, max(MIN_MULTIPLIER, self.max_rps / self.base_rps))
        return mult

    def set_ceiling(self, max_rps):
        """Install a hard req/s ceiling learned from an earlier throttling event."""
        with self._lock:
            self.max_rps = max(0.5, float(max_rps))
            if self.rate_multiplier > self._clamp_mult(self.rate_multiplier):
                self.rate_multiplier = self._clamp_mult(self.rate_multiplier)
                self.multi_rps = self.base_rps * self.rate_multiplier
                if self.mode == "multi":
                    self.rate_limiter.set_rate(self.multi_rps)
            self.last_event_desc = (
                f"ceiling {self.max_rps:.1f} req/s "
                f"({self.rate_multiplier:.2f}x) learned from throttling")

    def report_blocked(self, wait_override_s=None):
        # Learn BEFORE taking the lock: the callback persists the failure and
        # then calls set_ceiling(), which needs the same lock.
        if self.on_blocked is not None:
            try:
                ceiling = self.on_blocked(self.base_rps * self.rate_multiplier)
                if ceiling:
                    self.set_ceiling(ceiling)
            except Exception:
                pass
        with self._lock:
            now = time.monotonic()
            if now - self.last_blocked_at > AIMD_CLEAN_RESET:
                self.consec_blocks = 0
            self.consec_blocks += 1
            self.last_blocked_at = now

            cut = 0.5 if wait_override_s is not None else AIMD_DECREASE
            self.multi_rps = max(self.single_rps, self.multi_rps * cut)
            self.rate_multiplier = max(MIN_MULTIPLIER, self.multi_rps / self.base_rps)
            self.rate_limiter.set_rate(self.multi_rps)

            pause_s = wait_override_s
            if pause_s is None:
                pause_s = min(AIMD_PAUSE_MAX,
                              AIMD_PAUSE_BASE * (2 ** (self.consec_blocks - 1)))
            pause_s = max(1.0, min(float(pause_s), 3600.0))
            self.rate_limiter.pause_for(pause_s)
            self.next_retry_at = now + pause_s
            self.backoff_stage = self.consec_blocks
            self.last_event_desc = (f"429 #{self.consec_blocks} -> "
                                    f"{self.multi_rps:.1f} req/s "
                                    f"({self.rate_multiplier:.2f}x), pause {pause_s:.0f}s")

    def set_multiplier(self, new_mult):
        """Set rate multiplier, recalculate RPS = base_rps * new_mult,
        and apply to rate limiter if in multi mode. Returns (new_mult, new_rps)."""
        with self._lock:
            new_mult = self._clamp_mult(new_mult)
            self.rate_multiplier = new_mult
            new_rps = self.base_rps * new_mult
            self.multi_rps = new_rps
            if self.mode == "multi":
                self.rate_limiter.set_rate(new_rps)
            self.last_event_desc = f"multiplier set to {new_mult:.2f}x ({new_rps:.1f} req/s)"
        return new_mult, self.multi_rps

    def bump_multiplier(self, delta):
        """Adjust the multiplier by delta (thread-safe read-modify-write).
        Returns (new_mult, new_rps)."""
        with self._lock:
            cur = self.rate_multiplier
            new_mult = self._clamp_mult(cur + delta)
            self.rate_multiplier = new_mult
            new_rps = self.base_rps * new_mult
            self.multi_rps = new_rps
            if self.mode == "multi":
                self.rate_limiter.set_rate(new_rps)
            self.last_event_desc = f"multiplier bumped by {delta:+.2f} -> {new_mult:.2f}x ({new_rps:.1f} req/s)"
        return new_mult, new_rps

    def tick(self):
        with self._lock:
            now = time.monotonic()
            if self.next_retry_at is not None and now >= self.next_retry_at:
                self.next_retry_at = None
            if self.consec_blocks and now - self.last_blocked_at > AIMD_CLEAN_RESET:
                self.consec_blocks = 0
            if not self.auto_ai:
                self.ai_last = now
                return
            if now - self.ai_last < AIMD_AI_INTERVAL:
                return
            self.ai_last = now
            step = max(AIMD_INCREASE_MIN, self.multi_rps * AIMD_INCREASE_FRACTION)
            new_rps = self.multi_rps + step
            if self.max_rps:
                new_rps = min(new_rps, self.max_rps)
            if new_rps <= self.multi_rps + 1e-9:
                return
            self.multi_rps = new_rps
            self.rate_multiplier = max(MIN_MULTIPLIER, new_rps / self.base_rps)
            self.rate_limiter.set_rate(new_rps)
            self.last_event_desc = f"clean window -> {new_rps:.1f} req/s"

    def snapshot(self):
        with self._lock:
            retry_in = None
            if self.next_retry_at is not None:
                retry_in = max(0.0, self.next_retry_at - time.monotonic())
            return {
                "mode": self.mode,
                "allowed_workers": self.allowed_workers,
                "backoff_stage": self.backoff_stage,
                "retry_in_s": retry_in,
                "last_event": self.last_event_desc,
                "rate_multiplier": self.rate_multiplier,
                "rps": self.multi_rps,
            }


# ============================================================================
# Keyboard controller: non-blocking single-key hotkeys (Windows only).
#
# Uses msvcrt (Python's Windows-only stdlib module) instead of ctypes'
# GetAsyncKeyState -- msvcrt.kbhit()/getch() gives a clean non-blocking
# single-key read in a few lines with zero pip installs, whereas polling
# GetAsyncKeyState needs manual virtual-key-code handling and is longer for
# no benefit here. If msvcrt isn't available (non-Windows), hotkeys are
# silently disabled and the script still runs fine.
# ============================================================================

class KeyboardController:
    def __init__(self, rate_limiter, mode_ctrl, pause_event, verbose_flag,
                 force_finalize_event, adaptive_flag, restart_event, logger, stop_event,
                 auto_restart_flag=None):
        self.rate_limiter = rate_limiter
        self.mode_ctrl = mode_ctrl
        self.pause_event = pause_event
        self.verbose_flag = verbose_flag
        self.force_finalize_event = force_finalize_event
        self.adaptive_flag = adaptive_flag
        self.restart_event = restart_event
        self.logger = logger
        self.stop_event = stop_event
        self.auto_restart_flag = auto_restart_flag or threading.Event()
        self.last_key_desc = "-"
        self._lock = threading.Lock()
        self.adaptive_target = 4.0

    def snapshot(self):
        with self._lock:
            return {"last_key": self.last_key_desc,
                    "paused": not self.pause_event.is_set(),
                    "adaptive_active": self.adaptive_flag.is_set(),
                    "adaptive_target": self.adaptive_target}

    def _set_desc(self, desc):
        with self._lock:
            self.last_key_desc = desc

    def _bump_multiplier(self, delta):
        new_mult, _ = self.mode_ctrl.bump_multiplier(delta)
        self._set_desc(f"rate -> {new_mult:.2f}x")
        self.logger.info(f"Rate multiplier adjusted to {new_mult:.2f}x via hotkey")

    def _reset_multiplier(self):
        new_mult, _ = self.mode_ctrl.set_multiplier(1.0)
        self._set_desc("rate reset to 1.00x")
        self.logger.info("Rate multiplier reset to 1.00x via hotkey")

    def _adjust_adaptive_target(self, delta):
        self.adaptive_target = max(0.5, self.adaptive_target + delta)
        self._set_desc(f"target -> {self.adaptive_target:.3f}")
        self.logger.info(f"Adaptive target adjusted by {delta:+.3f} -> {self.adaptive_target:.3f}")

    def loop(self):
        if not HAVE_MSVCRT:
            return
        while not self.stop_event.is_set():
            if msvcrt.kbhit():
                ch = msvcrt.getch()
                # Arrow keys / extended keys start with \xe0
                if ch == b'\xe0':
                    ch2 = msvcrt.getch()
                    if ch2 == b'H':   # Up arrow
                        self._bump_multiplier(MULTIPLIER_STEP)
                    elif ch2 == b'P':  # Down arrow
                        self._bump_multiplier(-MULTIPLIER_STEP)
                    elif ch2 == b'M':  # Right arrow — raise adaptive target
                        self._adjust_adaptive_target(0.100)
                    elif ch2 == b'K':  # Left arrow — lower adaptive target
                        self._adjust_adaptive_target(-0.100)
                    continue
                try:
                    key = ch.decode("utf-8", errors="ignore")
                except Exception:
                    key = ""
                if key == " ":
                    if self.pause_event.is_set():
                        self.pause_event.clear()
                        self._set_desc("PAUSED")
                        self.logger.info("Paused via hotkey (SPACE)")
                    else:
                        self.pause_event.set()
                        self._set_desc("RESUMED")
                        self.logger.info("Resumed via hotkey (SPACE)")
                elif key in ("+", "="):
                    self._bump_multiplier(MULTIPLIER_STEP)
                elif key == "-":
                    self._bump_multiplier(-MULTIPLIER_STEP)
                elif key.lower() == "v":
                    new_val = not self.verbose_flag.is_set()
                    if new_val:
                        self.verbose_flag.set()
                    else:
                        self.verbose_flag.clear()
                    self._set_desc(f"verbose -> {new_val}")
                    self.logger.info(f"Verbose panel toggled to {new_val} via hotkey")
                elif key.lower() == "f":
                    self.force_finalize_event.set()
                    self._set_desc("force finalize requested")
                    self.logger.info("Force-finalize requested via hotkey (F)")
                elif key.lower() == "r":
                    self._reset_multiplier()
                elif key.lower() == "a":
                    new_val = not self.adaptive_flag.is_set()
                    if new_val:
                        self.adaptive_flag.set()
                    else:
                        self.adaptive_flag.clear()
                    self._set_desc("adaptive -> " + ("ON" if new_val else "OFF"))
                    self.logger.info(f"Adaptive mode toggled to {new_val} via hotkey")
                elif key.lower() == "l":
                    new_val = not self.auto_restart_flag.is_set()
                    if new_val:
                        self.auto_restart_flag.set()
                    else:
                        self.auto_restart_flag.clear()
                    self._set_desc("auto-restart -> " + ("ON" if new_val else "OFF"))
                    self.logger.info(f"Auto-restart toggled to {new_val} via hotkey")
                elif key.lower() == "p":
                    self.restart_event.set()
                    self._set_desc("restart requested")
                    self.logger.info("Worker restart requested via hotkey (P)")
            time.sleep(0.05)


# ============================================================================
# WAL (write-ahead log): crash-safe local record of blocks not yet finalized
# into a real .parquet file. Plain text, one atomic append per block.
# Format per block:
#   BLOCK <height>
#   <counter> <txid>
#   <counter> <txid>
#   ...
# ============================================================================

class WalManager:
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.path = None  # Path to current WIP wal file

    def start_new(self, start_height: int):
        self.path = self.output_dir / f"WIP_{start_height}.wal"
        # truncate/create
        with open(self.path, "w", encoding="utf-8"):
            pass

    def append_block(self, height: int, txids: list):
        """Single atomic write for the whole block: either fully present
        on disk after this call, or (on crash before it) not present at all."""
        lines = [f"BLOCK {height}\n"]
        for i, txid in enumerate(txids, start=1):
            lines.append(f"{i} {txid}\n")
        data = "".join(lines)
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())

    def clear(self, new_start_height: int):
        """Called right after a successful finalize: wipe old WAL, start fresh."""
        if self.path and self.path.exists():
            self.path.unlink()
        self.start_new(new_start_height)

    @staticmethod
    def find_existing(output_dir: Path):
        matches = sorted(output_dir.glob(WAL_GLOB_PATTERN))
        return matches[-1] if matches else None

    @staticmethod
    def clear_stale(output_dir: Path, keep: Path):
        """Remove any WAL files other than the one we're keeping.
        Prevents stale WALs from earlier sessions from confusing restart logic."""
        for w in output_dir.glob(WAL_GLOB_PATTERN):
            if w != keep:
                try:
                    w.unlink()
                except OSError:
                    pass

    @staticmethod
    def parse(wal_path: Path):
        """Returns (start_height, [(height, [txids]), ...]) parsed from an
        existing WAL file. No network access -- pure local file read."""
        blocks = []
        cur_height = None
        cur_txids = []
        with open(wal_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.rstrip("\n")
                if not line:
                    continue
                if line.startswith("BLOCK "):
                    if cur_height is not None:
                        blocks.append((cur_height, cur_txids))
                    cur_height = int(line.split(" ", 1)[1])
                    cur_txids = []
                else:
                    # "<counter> <txid>"
                    _, txid = line.split(" ", 1)
                    cur_txids.append(txid)
            if cur_height is not None:
                blocks.append((cur_height, cur_txids))
        start_height = int(wal_path.stem.split("_", 1)[1])
        return start_height, blocks


# ============================================================================
# Checkpoint (small JSON summary, used for UI continuity / sanity-check only;
# the WAL is the real source of truth for what data is pending)
# ============================================================================

ROOTS_FILENAME = "roots.csv"
HEADERS_FILENAME = "headers.csv"


def dsha256(data: bytes) -> bytes:
    return hashlib.sha256(hashlib.sha256(data).digest()).digest()


def merkle_root(txids):
    stack = []
    for txid in txids:
        level = 0
        node = bytes.fromhex(txid)[::-1]
        while stack and stack[-1][0] == level:
            level += 1
            node = dsha256(stack.pop()[1] + node)
        stack.append((level, node))
    if not stack:
        return None
    level, node = stack.pop()
    while stack:
        while level < stack[-1][0]:
            node = dsha256(node + node)
            level += 1
        below_level, below = stack.pop()
        node = dsha256(below + node)
        level = below_level + 1
    return node[::-1].hex()


class CsvSink:
    def __init__(self, path: Path):
        self.path = path
        self._fh = None
        self._since_sync = 0

    def last_height(self):
        if not self.path.exists():
            return None
        last = None
        with open(self.path, "r", encoding="utf-8") as fh:
            for line in fh:
                head = line.strip().split(",")[0]
                try:
                    last = int(head)
                except ValueError:
                    continue
        return last

    def append(self, line: str):
        if self._fh is None:
            self._fh = open(self.path, "a", encoding="utf-8", newline="")
        self._fh.write(line + "\n")
        self._fh.flush()
        self._since_sync += 1
        if self._since_sync >= 64:
            os.fsync(self._fh.fileno())
            self._since_sync = 0

    def close(self):
        if self._fh is not None:
            self._fh.close()
            self._fh = None


class RootsWriter(CsvSink):
    def __init__(self, output_dir: Path):
        super().__init__(output_dir / ROOTS_FILENAME)

    def append(self, height: int, root: str):
        super().append(f"{height},{root}")


class HeadersWriter(CsvSink):
    def __init__(self, output_dir: Path):
        super().__init__(output_dir / HEADERS_FILENAME)

    def append(self, height: int, hdr: dict):
        super().append(f"{height},{hdr['version']},{hdr['prev']},{hdr['merkle']},"
                       f"{hdr['time']},{hdr['bits']},{hdr['nonce']}")


class CheckpointManager:
    def __init__(self, output_dir: Path):
        self.path = output_dir / CHECKPOINT_FILENAME

    def load(self):
        if self.path.exists():
            try:
                return json.loads(self.path.read_text(encoding="utf-8"))
            except Exception:
                return {}
        return {}

    def save(self, data: dict):
        tmp = self.path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
        os.replace(tmp, self.path)


# ============================================================================
# Parquet building
# ============================================================================

TXID_TABLE_SCHEMA = pa.schema([
    pa.field("marker", pa.int64()),
    pa.field("txid", pa.string()),
])


def blocks_to_table(blocks: list) -> pa.Table:
    """blocks: [(height, [txid, ...]), ...] -> pyarrow table with the
    marker/txid row layout described at the top of this file."""
    markers = []
    txids = []
    for height, block_txids in blocks:
        markers.append(height)
        txids.append("")
        for i, txid in enumerate(block_txids, start=1):
            markers.append(i)
            txids.append(txid)
    return pa.table({
        "marker": pa.array(markers, type=pa.int64()),
        "txid": pa.array(txids, type=pa.string()),
    }, schema=TXID_TABLE_SCHEMA)


def serialized_size_bytes(table: pa.Table) -> int:
    sink = io.BytesIO()
    pq.write_table(table, sink, compression="snappy")
    return sink.tell()


def write_final_parquet(output_dir: Path, blocks: list) -> Path:
    start_h = blocks[0][0]
    end_h = blocks[-1][0]
    table = blocks_to_table(blocks)
    final_path = output_dir / f"{start_h}-{end_h}.parquet"
    pq.write_table(table, final_path, compression="snappy")
    return final_path


# ============================================================================
# HTTP client
# ============================================================================

class MempoolClient:
    payload = "txids"
    def __init__(self, api_base: str, rate_limiter: RateLimiter, stats=None, verify_ntx: bool = False):
        self.api_base = api_base.rstrip("/")
        self.rate_limiter = rate_limiter
        self.stats = stats
        self.verify_ntx = verify_ntx
        self._local = threading.local()

    def _session(self) -> requests.Session:
        if not hasattr(self._local, "session"):
            self._local.session = requests.Session()
        return self._local.session

    def _get(self, path: str) -> requests.Response:
        self.rate_limiter.acquire()
        s = self._session()
        last_exc = None
        for attempt in range(MAX_RETRIES_PER_REQUEST):
            try:
                if self.stats:
                    self.stats.note_request()
                t0 = time.monotonic()
                resp = s.get(f"{self.api_base}{path}", timeout=REQUEST_TIMEOUT)
                latency = time.monotonic() - t0
                if resp.status_code == 429:
                    if self.stats:
                        self.stats.note_429()
                        self.stats.note_error_type("HTTP 429")
                    raise RateLimited(path, parse_retry_after(resp.headers.get("Retry-After")))
                if resp.status_code >= 500:
                    last_exc = RuntimeError(f"HTTP {resp.status_code} on {path}")
                    if self.stats:
                        self.stats.note_retry()
                        self.stats.note_error_type(f"HTTP {resp.status_code}")
                    time.sleep(min(2 ** attempt, 20))
                    continue
                resp.raise_for_status()
                if self.stats:
                    self.stats.note_latency(latency, resp.status_code)
                return resp
            except RateLimited:
                raise
            except (requests.ConnectionError, requests.Timeout) as e:
                last_exc = e
                if self.stats:
                    self.stats.note_retry()
                    self.stats.note_error_type(type(e).__name__)
                time.sleep(min(2 ** attempt, 20))
                continue
        raise RuntimeError(f"Exhausted retries on {path}: {last_exc}")

    def height_to_hash(self, height: int) -> str:
        resp = self._get(f"/block-height/{height}")
        return resp.text.strip()

    def block_txids(self, block_hash: str) -> list:
        resp = self._get(f"/block/{block_hash}/txids")
        return resp.json()

    def fetch_block(self, height: int):
        h = self.height_to_hash(height)
        if self.payload == "headers":
            j = self._get(f"/block/{h}").json()
            return height, {
                "hash": j["id"],
                "version": j["version"],
                "prev": j["previousblockhash"],
                "merkle": j["merkle_root"],
                "time": j["timestamp"],
                "bits": j["bits"],
                "nonce": j["nonce"],
                "nTx": j.get("tx_count", 0),
            }
        txids = self.block_txids(h)
        if self.verify_ntx:
            header = self._get(f"/block/{h}").json()
            expected = header.get("tx_count")
            if expected is not None and int(expected) != len(txids):
                raise RuntimeError(
                    f"nTx mismatch at height {height}: header says {expected}, got {len(txids)} txids")
        return height, txids


# ============================================================================
# Stats
# ============================================================================

class Stats:
    def __init__(self, start_height, end_height, prior_blocks_done=0, prior_tx_total=0,
                 prior_bytes_written=0, prior_files_finalized=0):
        self._lock = threading.Lock()
        self.start_height = start_height
        self.end_height = end_height
        self.total_target = end_height - start_height + 1

        self.session_start = time.monotonic()
        self.wall_start = time.time()

        self.blocks_done = 0                 # this session
        self.prior_blocks_done = prior_blocks_done  # from earlier runs (checkpoint)
        self.prior_tx_total = prior_tx_total
        self.tx_total = prior_tx_total
        self.tx_counts_recent = deque(maxlen=500)
        self.block_done_times = deque(maxlen=2000)  # monotonic timestamps

        self.requests_sent = 0
        self.requests_429 = 0
        self.requests_retried = 0
        self.requests_failed_other = 0

        self.bytes_written_total = prior_bytes_written
        self.bytes_written_session = 0
        self.files_finalized = prior_files_finalized

        self.current_file_start_height = None
        self.current_file_block_count = 0
        self.current_file_est_bytes = 0

        self.last_error = None
        self.last_error_time = None

        self.min_tx_in_block = None
        self.max_tx_in_block = None
        self.last_success_time = time.monotonic()

        # --- latency tracking ---
        self.latency_samples = deque(maxlen=300)
        self.last_status_code = None

        # --- error breakdown by type ---
        self.error_counts = {}

        # --- recent completed blocks log (height, tx_count, timestamp) ---
        self.recent_blocks_log = deque(maxlen=15)

        # --- per-thread worker status ---
        self.thread_status = {}  # worker_index -> dict

        # --- network I/O baseline (psutil) ---
        self._net_baseline = None
        self._net_last_sample = None
        self._net_last_time = None
        if HAVE_PSUTIL:
            try:
                self._net_baseline = psutil.net_io_counters()
                self._net_last_sample = self._net_baseline
                self._net_last_time = time.monotonic()
            except Exception:
                self._net_baseline = None

        # --- disk write throughput tracking ---
        self._disk_last_bytes = prior_bytes_written
        self._disk_last_time = time.monotonic()
        self._disk_mbps = 0.0

    def note_request(self):
        with self._lock:
            self.requests_sent += 1

    def note_429(self):
        with self._lock:
            self.requests_429 += 1

    def note_retry(self):
        with self._lock:
            self.requests_retried += 1

    def note_error(self, msg):
        with self._lock:
            self.last_error = msg
            self.last_error_time = time.time()

    def note_latency(self, latency_s, status_code):
        with self._lock:
            self.latency_samples.append(latency_s)
            self.last_status_code = status_code

    def note_error_type(self, type_name):
        with self._lock:
            self.error_counts[type_name] = self.error_counts.get(type_name, 0) + 1

    def note_thread_status(self, worker_index, state, height=None):
        with self._lock:
            self.thread_status[worker_index] = {
                "state": state,
                "height": height,
                "updated": time.monotonic(),
            }

    def note_block_done(self, height, tx_count):
        with self._lock:
            self.blocks_done += 1
            self.tx_total += tx_count
            self.tx_counts_recent.append(tx_count)
            now = time.monotonic()
            self.block_done_times.append(now)
            self.last_success_time = now
            self.recent_blocks_log.append((height, tx_count, time.time()))
            if self.min_tx_in_block is None or tx_count < self.min_tx_in_block:
                self.min_tx_in_block = tx_count
            if self.max_tx_in_block is None or tx_count > self.max_tx_in_block:
                self.max_tx_in_block = tx_count

    def note_file_finalized(self, size_bytes):
        with self._lock:
            self.files_finalized += 1
            self.bytes_written_total += size_bytes
            self.bytes_written_session += size_bytes

    def set_current_file(self, start_height, block_count, est_bytes):
        with self._lock:
            self.current_file_start_height = start_height
            self.current_file_block_count = block_count
            self.current_file_est_bytes = est_bytes

    def snapshot(self):
        with self._lock:
            now = time.monotonic()
            elapsed = now - self.session_start
            done_total = self.blocks_done + self.prior_blocks_done
            remaining = max(0, self.total_target - done_total)
            pct = (done_total / self.total_target * 100) if self.total_target else 100.0

            overall_bps = (self.blocks_done / elapsed) if elapsed > 0 else 0.0

            recent_window = [t for t in self.block_done_times if now - t <= 60.0]
            recent_bps = len(recent_window) / min(60.0, elapsed) if elapsed > 0 else 0.0

            window_10s = [t for t in self.block_done_times if now - t <= 10.0]
            recent_bps_10s = len(window_10s) / min(10.0, elapsed) if elapsed > 0 else 0.0

            eta_s = (remaining / overall_bps) if overall_bps > 0 else None
            sec_per_block = (1.0 / overall_bps) if overall_bps > 0 else None

            avg_tx_per_block = (sum(self.tx_counts_recent) / len(self.tx_counts_recent)
                                if self.tx_counts_recent else 0)
            overall_avg_tx = (self.tx_total / done_total) if done_total else 0

            # --- latency percentiles ---
            lat_avg = lat_min = lat_max = lat_p95 = None
            if self.latency_samples:
                samples = sorted(self.latency_samples)
                lat_avg = sum(samples) / len(samples)
                lat_min = samples[0]
                lat_max = samples[-1]
                lat_p95 = samples[int(0.95 * (len(samples) - 1))]

            # --- tx-count sparkline over recent blocks ---
            sparkline = ""
            if self.tx_counts_recent:
                recent_slice = list(self.tx_counts_recent)[-40:]
                lo, hi = min(recent_slice), max(recent_slice)
                span = (hi - lo) or 1
                sparkline = "".join(
                    SPARKLINE_CHARS[min(len(SPARKLINE_CHARS) - 1,
                                         int((v - lo) / span * (len(SPARKLINE_CHARS) - 1)))]
                    for v in recent_slice
                )

            # --- network I/O throughput (psutil) ---
            net_sent_mbps = net_recv_mbps = None
            net_sent_total = net_recv_total = None
            if HAVE_PSUTIL and self._net_baseline is not None:
                try:
                    cur = psutil.net_io_counters()
                    dt = max(1e-6, now - self._net_last_time) if self._net_last_time else None
                    if dt:
                        net_sent_mbps = (cur.bytes_sent - self._net_last_sample.bytes_sent) / dt / (1024 * 1024)
                        net_recv_mbps = (cur.bytes_recv - self._net_last_sample.bytes_recv) / dt / (1024 * 1024)
                    net_sent_total = cur.bytes_sent - self._net_baseline.bytes_sent
                    net_recv_total = cur.bytes_recv - self._net_baseline.bytes_recv
                    self._net_last_sample = cur
                    self._net_last_time = now
                except Exception:
                    pass

            # --- disk write throughput (derived from our own bytes_written_total) ---
            dt_disk = now - self._disk_last_time
            if dt_disk >= 1.0:
                self._disk_mbps = ((self.bytes_written_total - self._disk_last_bytes)
                                    / dt_disk / (1024 * 1024))
                self._disk_last_bytes = self.bytes_written_total
                self._disk_last_time = now

            # --- compression ratio: raw estimate vs actual parquet bytes ---
            raw_estimate = self.tx_total * 66  # ~64 hex chars + row/string overhead
            compression_ratio = (raw_estimate / self.bytes_written_total
                                  if self.bytes_written_total else None)

            return {
                "done_total": done_total,
                "remaining": remaining,
                "total_target": self.total_target,
                "pct": pct,
                "elapsed_s": elapsed,
                "overall_bps": overall_bps,
                "recent_bps": recent_bps,
                "recent_bps_10s": recent_bps_10s,
                "eta_s": eta_s,
                "sec_per_block": sec_per_block,
                "tx_total": self.tx_total,
                "avg_tx_per_block_recent": avg_tx_per_block,
                "avg_tx_per_block_overall": overall_avg_tx,
                "min_tx_in_block": self.min_tx_in_block,
                "max_tx_in_block": self.max_tx_in_block,
                "requests_sent": self.requests_sent,
                "requests_429": self.requests_429,
                "requests_retried": self.requests_retried,
                "requests_failed_other": self.requests_failed_other,
                "bytes_written_total": self.bytes_written_total,
                "bytes_written_session": self.bytes_written_session,
                "files_finalized": self.files_finalized,
                "current_file_start_height": self.current_file_start_height,
                "current_file_block_count": self.current_file_block_count,
                "current_file_est_bytes": self.current_file_est_bytes,
                "last_error": self.last_error,
                "last_error_time": self.last_error_time,
                "last_status_code": self.last_status_code,
                "lat_avg": lat_avg, "lat_min": lat_min, "lat_max": lat_max, "lat_p95": lat_p95,
                "sparkline": sparkline,
                "net_sent_mbps": net_sent_mbps, "net_recv_mbps": net_recv_mbps,
                "net_sent_total": net_sent_total, "net_recv_total": net_recv_total,
                "disk_mbps": self._disk_mbps,
                "compression_ratio": compression_ratio,
                "raw_estimate_bytes": raw_estimate,
                "error_counts": dict(self.error_counts),
                "recent_blocks_log": list(self.recent_blocks_log),
                "thread_status": dict(self.thread_status),
                "blocks_done_session": self.blocks_done,
                "tx_total_session": self.tx_total - self.prior_tx_total,
                "blocks_done_lifetime": done_total,
                "tx_total_lifetime": self.tx_total,
            }


# ============================================================================
# Worker pool + ordered writer
# ============================================================================

class Coordinator:
    def __init__(self, client: MempoolClient, mode_ctrl: ModeController,
                 stats: Stats, wal: WalManager, ckpt: CheckpointManager,
                 output_dir: Path, file_cap_bytes: int,
                 start_height: int, end_height: int,
                 resume_height: int, current_file_start: int,
                 pending_blocks: list, pause_event=None, force_finalize_event=None,
                 logger=None, roots=None, headers=None, store="txids"):
        self.client = client
        self.mode_ctrl = mode_ctrl
        self.stats = stats
        self.wal = wal
        self.ckpt = ckpt
        self.output_dir = output_dir
        self.file_cap_bytes = file_cap_bytes
        self.start_height = start_height
        self.end_height = end_height
        self.pause_event = pause_event or threading.Event()
        if pause_event is None:
            self.pause_event.set()
        self.force_finalize_event = force_finalize_event or threading.Event()
        self.logger = logger or logging.getLogger("txid_collector")
        self.roots = roots
        self.headers = headers
        self.store = store

        self.next_to_assign = resume_height
        self.next_expected = resume_height
        self.assign_lock = threading.Lock()

        self.results = {}
        self.results_lock = threading.Lock()
        self.results_cv = threading.Condition(self.results_lock)

        self.retry_queue = queue.Queue()

        self.stop_event = threading.Event()

        self.current_file_start = current_file_start
        self.pending_blocks = pending_blocks  # [(height, [txids]), ...] not yet finalized
        self.pending_lock = threading.Lock()

        self.stats.set_current_file(self.current_file_start, len(pending_blocks), 0)
        self._block_count_since_exact_check = 0

    def _claim_height(self):
        try:
            return self.retry_queue.get_nowait()
        except queue.Empty:
            pass
        with self.assign_lock:
            if self.next_to_assign > self.end_height:
                return None
            h = self.next_to_assign
            self.next_to_assign += 1
            return h

    def worker_loop(self, worker_index: int):
        while not self.stop_event.is_set():
            if not self.pause_event.is_set():
                self.stats.note_thread_status(worker_index, "paused")
                time.sleep(0.15)
                continue
            snap = self.mode_ctrl.snapshot()
            if worker_index >= snap["allowed_workers"]:
                self.stats.note_thread_status(worker_index, "idle (backoff)")
                time.sleep(0.1)
                continue
            height = self._claim_height()
            if height is None:
                self.stats.note_thread_status(worker_index, "idle (no work)")
                if self.next_to_assign > self.end_height and not self.retry_queue.qsize():
                    time.sleep(0.2)
                    if self.next_to_assign > self.end_height and self.retry_queue.empty():
                        return
                    continue
                time.sleep(0.05)
                continue
            self.stats.note_thread_status(worker_index, "fetching", height)
            try:
                h, txids = self.client.fetch_block(height)
                with self.results_cv:
                    self.results[h] = txids
                    self.results_cv.notify_all()
            except RateLimited as e:
                self.mode_ctrl.report_blocked(getattr(e, "retry_after_s", None))
                self.retry_queue.put(height)
            except Exception as e:
                self.stats.note_error(f"height {height}: {e}")
                self.stats.note_error_type(type(e).__name__)
                self.logger.warning(f"Worker {worker_index} error at height {height}: {e}")
                time.sleep(1.0)
                self.retry_queue.put(height)

    def _finalize_current_file(self, blocks_to_write: list, next_file_start: int):
        path = write_final_parquet(self.output_dir, blocks_to_write)
        size = path.stat().st_size
        self.stats.note_file_finalized(size)
        self.wal.clear(next_file_start)
        with self.pending_lock:
            self.pending_blocks = []
        self.current_file_start = next_file_start
        self._block_count_since_exact_check = 0
        self.stats.set_current_file(self.current_file_start, 0, 0)
        self.logger.info(f"Finalized parquet file {path.name} ({size:,} bytes)")

    def force_finalize(self):
        """Triggered by the 'f' hotkey: flush whatever is pending right now
        into a real parquet file early, instead of waiting for the cap."""
        with self.pending_lock:
            blocks = list(self.pending_blocks)
        if blocks:
            self._finalize_current_file(blocks, blocks[-1][0] + 1)
            self.logger.info("Force-finalize completed (hotkey)")

    def _cheap_estimate_bytes(self, blocks: list) -> int:
        total = 0
        for _, txids in blocks:
            total += 24  # marker row overhead estimate
            total += len(txids) * 80  # ~64 hex chars + int + row overhead
        return total

    def process_ready_results(self):
        """Runs in the main/writer thread. Consumes results for
        self.next_expected in strict order, appends to WAL + pending file
        accumulator, rolls the file over at the 1 GiB cap."""
        processed_any = False
        while True:
            with self.results_cv:
                txids = self.results.pop(self.next_expected, None)
            if txids is None:
                break
            height = self.next_expected
            if self.store != "txids":
                if self.store == "roots":
                    n_tx = len(txids)
                    self.roots.append(height, merkle_root(txids))
                else:
                    n_tx = int(txids.get("nTx", 0))
                    self.headers.append(height, txids)
                self.ckpt.save({
                    "last_completed_height": height,
                    "current_file_start_height": self.current_file_start,
                    "blocks_done_cumulative": (height - self.start_height + 1),
                    "tx_total_cumulative": self.stats.tx_total + n_tx,
                    "bytes_written_total": self.stats.bytes_written_total,
                    "files_finalized": 0,
                })
                self.stats.note_block_done(height, n_tx)
                self.next_expected += 1
                processed_any = True
                continue
            self.wal.append_block(height, txids)
            with self.pending_lock:
                self.pending_blocks.append((height, txids))
                blocks_snapshot = list(self.pending_blocks)

            cheap_est = self._cheap_estimate_bytes(blocks_snapshot)
            self.stats.set_current_file(self.current_file_start, len(blocks_snapshot), cheap_est)

            if cheap_est >= self.file_cap_bytes * CHEAP_SIZE_CHECK_FRACTION:
                # Throttle: only run the expensive exact-size serialization every
                # EXACT_CHECK_INTERVAL blocks instead of every single block,
                # preventing the main (UI) thread from freezing near file boundaries.
                if self._block_count_since_exact_check >= EXACT_CHECK_INTERVAL:
                    exact = serialized_size_bytes(blocks_to_table(blocks_snapshot))
                    self._block_count_since_exact_check = 0
                else:
                    self._block_count_since_exact_check += 1
                    exact = cheap_est
                self.stats.set_current_file(self.current_file_start, len(blocks_snapshot), exact)
                if exact > self.file_cap_bytes and len(blocks_snapshot) > 1:
                    # roll the overflowing block into a fresh file
                    to_write = blocks_snapshot[:-1]
                    overflow_block = blocks_snapshot[-1]
                    self._finalize_current_file(to_write, overflow_block[0])
                    with self.pending_lock:
                        self.pending_blocks = [overflow_block]
                    self.wal.append_block(overflow_block[0], overflow_block[1])
                    self.stats.set_current_file(
                        self.current_file_start, 1,
                        self._cheap_estimate_bytes([overflow_block]))

            self.ckpt.save({
                "last_completed_height": height,
                "current_file_start_height": self.current_file_start,
                "blocks_done_cumulative": (height - self.start_height + 1),
                "tx_total_cumulative": self.stats.tx_total + len(txids),
                "bytes_written_total": self.stats.bytes_written_total,
                "files_finalized": self.stats.files_finalized,
            })

            self.stats.note_block_done(height, len(txids))
            self.next_expected += 1
            processed_any = True
        return processed_any

    def finished(self) -> bool:
        return self.next_expected > self.end_height

    def finalize_on_exit(self):
        """On graceful shutdown (or natural completion), write out whatever
        is left in the pending accumulator as a final (possibly small)
        parquet file so nothing sits only in the WAL."""
        with self.pending_lock:
            blocks = list(self.pending_blocks)
        if blocks:
            self._finalize_current_file(blocks, blocks[-1][0] + 1)


# ============================================================================
# UI
# ============================================================================

def human_bytes(n):
    if n is None:
        return "-"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def human_time(s):
    if s is None:
        return "-"
    s = int(s)
    d, s = divmod(s, 86400)
    h, s = divmod(s, 3600)
    m, s = divmod(s, 60)
    parts = []
    if d:
        parts.append(f"{d}d")
    if h or d:
        parts.append(f"{h}h")
    if m or h or d:
        parts.append(f"{m}m")
    parts.append(f"{s}s")
    return " ".join(parts)


def render_minimal(stats_snap, mode_snap, kb_snap, colors):
    pct = stats_snap["pct"]
    state = "PAUSED" if kb_snap.get("paused") else "RUNNING"
    rps = mode_snap.get("rps") or stats_snap.get("recent_bps_10s", 0.0) * 2
    net = ""
    if stats_snap.get("net_recv_mbps") is not None:
        net = f"   {stats_snap['net_recv_mbps']:.1f} MB/s"

    grid = Table.grid(expand=True)
    grid.add_column(justify="center")
    grid.add_row(Text("BITCOIN TXID COLLECTOR", style="bold"))
    grid.add_row(ProgressBar(total=100, completed=pct, width=None))
    grid.add_row(Text(f"{stats_snap['done_total']:,} / {stats_snap['total_target']:,} blocks"
                      f"   {pct:.1f}%", style="bold"))
    grid.add_row("")
    grid.add_row(Text(f"elapsed {human_time(stats_snap['elapsed_s'])}"
                      f"   ETA {human_time(stats_snap['eta_s'])}", style="bold yellow"))
    grid.add_row(Text(f"{stats_snap.get('recent_bps_10s', 0.0):.2f} blocks/s"
                      f"   {rps:.0f} req/s{net}", style="bold cyan"))
    grid.add_row(Text(f"{stats_snap.get('tx_total_session', 0):,} txids"
                      f"   {state}", style="dim"))
    return Panel(grid, border_style=colors["header"], padding=(1, 4))


def render_dashboard(stats_snap, mode_snap, thread_count, refresh_rate,
                      theme="dark", compact=False, verbose=False,
                      kb_snap=None, output_dir=None,
                      auto_restart_active=False, minimal=False) -> Layout:
    colors = THEMES.get(theme, THEMES["dark"])
    kb_snap = kb_snap or {"last_key": "-", "paused": False}
    if minimal:
        return render_minimal(stats_snap, mode_snap, kb_snap, colors)

    layout = Layout()
    layout.split_column(
        Layout(name="header", size=16),
        Layout(name="body"),
        *([] if compact else [Layout(name="extra", size=9)]),
        Layout(name="footer", size=3),
    )

    # ---- Header: title/progress bar (main) + controls legend ----
    layout["header"].split_row(Layout(name="header_main"), Layout(name="header_controls", size=34))

    pct = stats_snap["pct"]
    bar = ProgressBar(total=100, completed=pct, width=None)
    header_table = Table.grid(expand=True)
    header_table.add_column(justify="left")
    header_table.add_row(Align.center(Text("BITCOIN TXID COLLECTOR", style="bold")))
    header_table.add_row(bar)
    header_table.add_row(Align.center(Text(
        f"{stats_snap['done_total']:,} / {stats_snap['total_target']:,} blocks  ({pct:.4f}%)",
        style="bold")))
    header_table.add_row(Align.center(Text(
        f"Elapsed: {human_time(stats_snap['elapsed_s'])}  |  "
        f"ETA: {human_time(stats_snap['eta_s'])}",
        style="bold yellow")))
    layout["header_main"].update(Panel(header_table, border_style=colors["header"]))

    # ---- Corner controls panel ----
    ctl_tbl = Table.grid(padding=(0, 1))
    ctl_tbl.add_column(style="dim")
    ctl_tbl.add_column()
    state_style = "yellow" if kb_snap["paused"] else "green"
    state_text = "PAUSED" if kb_snap["paused"] else "RUNNING"
    ctl_tbl.add_row("State", Text(state_text, style=f"bold {state_style}"))
    ctl_tbl.add_row("Rate", f"{mode_snap['rate_multiplier']:.2f}x")
    ad_active = kb_snap.get("adaptive_active", False)
    ad_target = kb_snap.get("adaptive_target", 4.0)
    ad_label = f"adaptive ON ({ad_target:.1f})" if ad_active else "adaptive OFF"
    ad_style = "green" if ad_active else "dim"
    ctl_tbl.add_row("A", Text(ad_label, style=ad_style))
    ctl_tbl.add_row("SPACE", "pause / resume")
    ctl_tbl.add_row("+/- / Up/Dn", "rate up / down")
    ctl_tbl.add_row("L/R arrow", Text(f"adj. target ({kb_snap.get('adaptive_target', 4.0):.2f})" if kb_snap.get("adaptive_active") else "adj. target (adaptive OFF)", style="bold" if kb_snap.get("adaptive_active") else "dim"))
    ctl_tbl.add_row("R", Text("reset rate", style="bold"))
    ctl_tbl.add_row("V", "toggle verbose")
    ctl_tbl.add_row("F", "force finalize")
    ctl_tbl.add_row("P", Text("restart workers", style="bold"))
    ar_label = "auto-restart ON" if auto_restart_active else "auto-restart OFF"
    ar_style = "green" if auto_restart_active else "dim"
    ctl_tbl.add_row("L", Text(ar_label, style=ar_style))
    ctl_tbl.add_row("Ctrl+C", "graceful stop")
    ctl_tbl.add_row("Last key", kb_snap["last_key"][:16])
    layout["header_controls"].update(Panel(ctl_tbl, title="Controls", border_style=colors["controls"]))

    # ---- Body columns ----
    if compact:
        layout["body"].split_row(Layout(name="left"), Layout(name="right"))
        layout["left"].split_column(Layout(name="progress"), Layout(name="throughput"))
        layout["right"].split_column(Layout(name="network"), Layout(name="file"), Layout(name="system"))
    else:
        layout["body"].split_row(Layout(name="col1"), Layout(name="col2"), Layout(name="col3"))
        layout["col1"].split_column(Layout(name="progress"), Layout(name="throughput"))
        layout["col2"].split_column(Layout(name="network"), Layout(name="latency"), Layout(name="errors"))
        layout["col3"].split_column(Layout(name="file"), Layout(name="compression_io"), Layout(name="system"))

    prog_tbl = Table.grid(padding=(0, 1))
    prog_tbl.add_column(style="dim")
    prog_tbl.add_column(justify="right")
    prog_tbl.add_row("Blocks done (total)", f"{stats_snap['done_total']:,}")
    prog_tbl.add_row("Blocks remaining", f"{stats_snap['remaining']:,}")
    prog_tbl.add_row("Elapsed (this session)", human_time(stats_snap["elapsed_s"]))
    eta_str = human_time(stats_snap["eta_s"])
    prog_tbl.add_row("ETA (remaining)", eta_str.ljust(15))
    prog_tbl.add_row("Percent complete", f"{pct:.5f}%")
    finish_clock = "-"
    if stats_snap["eta_s"] is not None:
        finish_clock = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time() + stats_snap["eta_s"]))
    prog_tbl.add_row("Projected finish (clock)", finish_clock)
    layout["progress"].update(Panel(prog_tbl, title="Progress", border_style=colors["progress"]))

    thr_tbl = Table.grid(padding=(0, 1))
    thr_tbl.add_column(style="dim")
    thr_tbl.add_column(justify="right")
    thr_tbl.add_row("Blocks/sec (overall avg)", f"{stats_snap['overall_bps']:.3f}")
    thr_tbl.add_row("Blocks/sec (last 60s)", f"{stats_snap['recent_bps']:.3f}")
    thr_tbl.add_row("Blocks/sec (last 10s)", f"{stats_snap['recent_bps_10s']:.3f}")
    thr_tbl.add_row("Sec/block (overall avg)", f"{stats_snap['sec_per_block']:.3f}" if stats_snap["sec_per_block"] else "-")
    thr_tbl.add_row("Transactions collected", f"{stats_snap['tx_total']:,}")
    thr_tbl.add_row("Avg tx/block (recent)", f"{stats_snap['avg_tx_per_block_recent']:.1f}")
    thr_tbl.add_row("Avg tx/block (overall)", f"{stats_snap['avg_tx_per_block_overall']:.1f}")
    thr_tbl.add_row("Min tx seen in a block", f"{stats_snap['min_tx_in_block']}")
    thr_tbl.add_row("Max tx seen in a block", f"{stats_snap['max_tx_in_block']}")
    if stats_snap["sparkline"]:
        thr_tbl.add_row("Tx/block trend (recent)", stats_snap["sparkline"])
    layout["throughput"].update(Panel(thr_tbl, title="Throughput", border_style=colors["progress"]))

    net_tbl = Table.grid(padding=(0, 1))
    net_tbl.add_column(style="dim")
    net_tbl.add_column(justify="right")
    mode_color = "green" if mode_snap["mode"] == "multi" else "yellow"
    net_tbl.add_row("Mode", Text(mode_snap["mode"].upper(), style=f"bold {mode_color}"))
    net_tbl.add_row("Active worker slots", f"{mode_snap['allowed_workers']} / {thread_count}")
    net_tbl.add_row("Backoff stage", f"{mode_snap['backoff_stage']}")
    net_tbl.add_row("Retry multi in", human_time(mode_snap["retry_in_s"]) if mode_snap["retry_in_s"] else "-")
    net_tbl.add_row("Requests sent", f"{stats_snap['requests_sent']:,}")
    net_tbl.add_row("HTTP 429s hit", f"{stats_snap['requests_429']:,}")
    net_tbl.add_row("Requests retried", f"{stats_snap['requests_retried']:,}")
    net_tbl.add_row("Last mode event", mode_snap["last_event"][:40])
    if stats_snap["net_sent_mbps"] is not None:
        net_tbl.add_row("Net sent/recv (live)",
                         f"{stats_snap['net_sent_mbps']:.2f} / {stats_snap['net_recv_mbps']:.2f} MB/s")
    layout["network"].update(Panel(net_tbl, title="Network / Rate Limiting", border_style=colors["network"]))

    file_tbl = Table.grid(padding=(0, 1))
    file_tbl.add_column(style="dim")
    file_tbl.add_column(justify="right")
    est = stats_snap["current_file_est_bytes"]
    cap_pct = (est / DEFAULT_FILE_CAP_BYTES * 100) if DEFAULT_FILE_CAP_BYTES else 0
    file_tbl.add_row("Current file start height", f"{stats_snap['current_file_start_height']}")
    file_tbl.add_row("Blocks in current file", f"{stats_snap['current_file_block_count']:,}")
    file_tbl.add_row("Current file size (est.)", f"{human_bytes(est)}  ({cap_pct:.1f}% of cap)")
    file_tbl.add_row("Files finalized", f"{stats_snap['files_finalized']:,}")
    file_tbl.add_row("Total bytes written", human_bytes(stats_snap["bytes_written_total"]))
    file_tbl.add_row("Bytes written (session)", human_bytes(stats_snap["bytes_written_session"]))
    layout["file"].update(Panel(file_tbl, title="Output Files", border_style=colors["file"]))

    sys_tbl = Table.grid(padding=(0, 1))
    sys_tbl.add_column(style="dim")
    sys_tbl.add_column(justify="right")
    if HAVE_PSUTIL:
        proc = psutil.Process()
        sys_tbl.add_row("CPU usage (process)", f"{proc.cpu_percent():.1f}%")
        sys_tbl.add_row("Memory (RSS)", human_bytes(proc.memory_info().rss))
    else:
        sys_tbl.add_row("CPU/Memory", "psutil not installed")
    try:
        du = shutil.disk_usage(output_dir or ROOT_DIR)
        sys_tbl.add_row("Disk free (output drive)", human_bytes(du.free))
    except Exception:
        sys_tbl.add_row("Disk free", "-")
    sys_tbl.add_row("UI refresh rate", f"{refresh_rate:.0f} fps")
    sys_tbl.add_row("Wall clock", time.strftime("%Y-%m-%d %H:%M:%S"))
    layout["system"].update(Panel(sys_tbl, title="System", border_style=colors["system"]))

    if not compact:
        lat_tbl = Table.grid(padding=(0, 1))
        lat_tbl.add_column(style="dim")
        lat_tbl.add_column(justify="right")
        lat_tbl.add_row("Avg latency", f"{stats_snap['lat_avg']*1000:.0f} ms" if stats_snap["lat_avg"] is not None else "-")
        lat_tbl.add_row("Min / Max latency",
                         f"{stats_snap['lat_min']*1000:.0f} / {stats_snap['lat_max']*1000:.0f} ms"
                         if stats_snap["lat_min"] is not None else "-")
        lat_tbl.add_row("P95 latency", f"{stats_snap['lat_p95']*1000:.0f} ms" if stats_snap["lat_p95"] is not None else "-")
        lat_tbl.add_row("Last status code", str(stats_snap["last_status_code"] or "-"))
        layout["latency"].update(Panel(lat_tbl, title="API Latency", border_style=colors["latency"]))

        err_tbl = Table.grid(padding=(0, 1))
        err_tbl.add_column(style="dim")
        err_tbl.add_column(justify="right")
        if stats_snap["error_counts"]:
            for k, v in sorted(stats_snap["error_counts"].items(), key=lambda kv: -kv[1])[:6]:
                err_tbl.add_row(k, f"{v:,}")
        else:
            err_tbl.add_row("No errors yet", "")
        layout["errors"].update(Panel(err_tbl, title="Error Breakdown", border_style=colors["errors"]))

        comp_tbl = Table.grid(padding=(0, 1))
        comp_tbl.add_column(style="dim")
        comp_tbl.add_column(justify="right")
        cr = stats_snap["compression_ratio"]
        comp_tbl.add_row("Compression ratio", f"{cr:.2f}x" if cr else "-")
        comp_tbl.add_row("Raw estimate (uncompressed)", human_bytes(stats_snap["raw_estimate_bytes"]))
        comp_tbl.add_row("Disk write throughput", f"{stats_snap['disk_mbps']:.2f} MB/s")
        layout["compression_io"].update(Panel(comp_tbl, title="Compression / Disk I/O", border_style=colors["compression"]))

        # ---- Extra row: recent blocks log, per-thread table, session vs lifetime ----
        layout["extra"].split_row(Layout(name="recent"), Layout(name="threads"), Layout(name="session"))

        recent_tbl = Table(padding=(0, 1), expand=True, show_edge=False)
        recent_tbl.add_column("Height", style="dim")
        recent_tbl.add_column("Txs", justify="right")
        recent_tbl.add_column("When", justify="right")
        for height, tx_count, ts in list(stats_snap["recent_blocks_log"])[-6:][::-1]:
            recent_tbl.add_row(f"{height:,}", f"{tx_count:,}", time.strftime("%H:%M:%S", time.localtime(ts)))
        layout["recent"].update(Panel(recent_tbl, title="Recent Blocks", border_style=colors["recent"]))

        thread_tbl = Table(padding=(0, 1), expand=True, show_edge=False)
        thread_tbl.add_column("Thr", style="dim")
        thread_tbl.add_column("State")
        thread_tbl.add_column("Height", justify="right")
        for idx in range(thread_count):
            st = stats_snap["thread_status"].get(idx, {"state": "starting", "height": None})
            height_disp = f"{st['height']:,}" if st.get("height") else "-"
            state_style2 = "green" if st["state"] == "fetching" else ("yellow" if "idle" in st["state"] or "paused" in st["state"] else "dim")
            thread_tbl.add_row(str(idx), Text(st["state"], style=state_style2), height_disp)
        layout["threads"].update(Panel(thread_tbl, title="Worker Threads", border_style=colors["threads"]))

        sess_tbl = Table.grid(padding=(0, 1))
        sess_tbl.add_column(style="dim")
        sess_tbl.add_column(justify="right")
        sess_tbl.add_row("Blocks (session)", f"{stats_snap['blocks_done_session']:,}")
        sess_tbl.add_row("Blocks (lifetime)", f"{stats_snap['blocks_done_lifetime']:,}")
        sess_tbl.add_row("Txs (session)", f"{stats_snap['tx_total_session']:,}")
        sess_tbl.add_row("Txs (lifetime)", f"{stats_snap['tx_total_lifetime']:,}")
        layout["session"].update(Panel(sess_tbl, title="Session vs Lifetime", border_style=colors["session"]))

    # ---- Footer: last error, or verbose debug info if enabled ----
    if verbose:
        err_text = "-"
        if stats_snap["last_error"]:
            age = time.time() - stats_snap["last_error_time"]
            err_text = f"[{age:.0f}s ago] {stats_snap['last_error']}"
        debug_text = (f"status={stats_snap['last_status_code']}  "
                      f"lat_avg={stats_snap['lat_avg']*1000:.0f}ms  " if stats_snap["lat_avg"] else "")
        footer_text = f"{err_text}   |   VERBOSE: {debug_text}"
        layout["footer"].update(Panel(Text(footer_text, style="red" if stats_snap["last_error"] else "dim"),
                                       title="Last Error / Debug", border_style="red"))
    else:
        err_text = "-"
        if stats_snap["last_error"]:
            age = time.time() - stats_snap["last_error_time"]
            err_text = f"[{age:.0f}s ago] {stats_snap['last_error']}"
        layout["footer"].update(Panel(Text(err_text, style="red" if stats_snap["last_error"] else "dim"),
                                       title="Last Error", border_style="red"))
    return layout


def setup_logging(output_dir: Path, log_file_arg, verbose: bool) -> logging.Logger:
    logger = logging.getLogger("txid_collector")
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    logger.handlers.clear()
    log_path = Path(log_file_arg) if log_file_arg else (output_dir / LOG_FILENAME)
    handler = logging.handlers.RotatingFileHandler(
        log_path, maxBytes=5_000_000, backupCount=3, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(handler)
    logger.propagate = False
    logger.info("=== Session started ===")
    return logger


def export_stats_json(output_dir: Path, stats_snap: dict, mode_snap: dict, kb_snap: dict):
    """Atomic write of a machine-readable stats snapshot, so a second tool
    could tail it later if desired."""
    payload = {
        "timestamp": time.time(),
        "stats": stats_snap,
        "mode": mode_snap,
        "controls": kb_snap,
    }
    path = output_dir / STATS_JSON_FILENAME
    tmp = path.with_suffix(".json.tmp")
    try:
        tmp.write_text(json.dumps(payload, default=str, indent=2), encoding="utf-8")
        os.replace(tmp, path)
    except Exception:
        pass


def load_speed_profile(output_dir: Path) -> dict:
    try:
        return json.loads((output_dir / SPEED_PROFILE_FILENAME).read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_speed_profile(output_dir: Path, data: dict):
    path = output_dir / SPEED_PROFILE_FILENAME
    tmp = path.with_suffix(".json.tmp")
    data["updated"] = time.strftime("%Y-%m-%dT%H:%M:%S")
    tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def record_rate_failure(output_dir: Path, failed_rps: float, logger=None) -> float:
    prof = load_speed_profile(output_dir)
    failed_rps = max(0.5, float(failed_rps))
    known_bad = prof.get("known_bad_rps")
    if known_bad is None or failed_rps < known_bad:
        prof["known_bad_rps"] = failed_rps
    ceiling = round(failed_rps * RATE_FAILURE_SAFETY, 2)
    prev = prof.get("ceiling_rps")
    prof["ceiling_rps"] = min(ceiling, prev) if prev else ceiling
    prof.setdefault("history", []).append({
        "t": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "event": "throttled",
        "rps": failed_rps,
        "ceiling_rps": prof["ceiling_rps"],
    })
    prof["history"] = prof["history"][-100:]
    save_speed_profile(output_dir, prof)
    if logger:
        logger.warning("Throttled at ~%.1f req/s -> ceiling %.1f req/s persisted; "
                       "adaptive mode will not climb back there",
                       failed_rps, prof["ceiling_rps"])
    return prof["ceiling_rps"]


# ============================================================================
# main
# ============================================================================

def main():
    global DEFAULT_FILE_CAP_BYTES
    args = build_arg_parser().parse_args()
    DEFAULT_FILE_CAP_BYTES = args.file_cap_bytes  # used by the UI cap-% display
    if args.root_only:
        args.store = "roots"

    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        output_dir = ROOT_DIR / DEFAULT_OUTPUT_DIR_NAME
        legacy = ROOT_DIR / LEGACY_OUTPUT_DIR_NAME
        if not output_dir.exists() and legacy.exists():
            output_dir = legacy
            print(f"Using legacy output dir (pre-rename): {legacy}")
    if args.dry_run:
        output_dir = output_dir / "dryrun"
    output_dir.mkdir(parents=True, exist_ok=True)

    wal = WalManager(output_dir)
    ckpt = CheckpointManager(output_dir)

    # --- Resume logic: local files only, zero network requests ---
    existing_wal = WalManager.find_existing(output_dir)
    ckpt_data = ckpt.load()

    roots = None
    headers = None
    sink = None
    if args.store == "roots":
        roots = RootsWriter(output_dir)
        sink = roots
    elif args.store == "headers":
        headers = HeadersWriter(output_dir)
        sink = headers
    if sink is not None:
        WalManager.clear_stale(output_dir, keep=None)
        done = sink.last_height()
        current_file_start = (done + 1) if done is not None else args.start_height
        resume_height = current_file_start
        pending_blocks = []
    elif existing_wal is not None:
        WalManager.clear_stale(output_dir, keep=existing_wal)
        wal_start, wal_blocks = WalManager.parse(existing_wal)
        wal.path = existing_wal
        current_file_start = wal_start
        pending_blocks = wal_blocks
        resume_height = (wal_blocks[-1][0] + 1) if wal_blocks else wal_start
    else:
        current_file_start = ckpt_data.get("current_file_start_height", args.start_height)
        resume_height = current_file_start
        pending_blocks = []
        wal.start_new(current_file_start)

    resume_height = max(resume_height, args.start_height)
    prior_blocks_done = max(0, resume_height - args.start_height)
    prior_tx_total = ckpt_data.get("tx_total_cumulative", 0)
    prior_bytes_written = ckpt_data.get("bytes_written_total", 0)
    prior_files_finalized = ckpt_data.get("files_finalized", 0)

    if args.dry_run:
        args.end_height = min(args.end_height, resume_height + 2)
        print(f"Dry run: fetching blocks {resume_height}-{args.end_height} into {output_dir}")
    elif args.max_blocks is not None and args.max_blocks > 0:
        window_end = resume_height + args.max_blocks - 1
        # The 958052 default is stale (tip is past 975k). Below it, --max-blocks
        # used to clamp to an empty range and exit "Already complete" at once.
        if args.end_height < resume_height:
            args.end_height = window_end
        else:
            args.end_height = min(args.end_height, window_end)
        print(f"Capped run: blocks {resume_height}-{args.end_height} (--max-blocks {args.max_blocks})")

    if resume_height > args.end_height:
        print(f"Already complete: resume height {resume_height} > end height {args.end_height}.")
        return

    logger = setup_logging(output_dir, args.log_file, args.verbose)

    speed_profile = load_speed_profile(output_dir)
    if speed_profile.get("safe_rps"):
        seed = speed_profile["safe_rps"]
        if speed_profile.get("ceiling_rps"):
            seed = min(seed, speed_profile["ceiling_rps"])
        args.multi_rps = max(args.multi_rps, seed)

    rate_limiter = RateLimiter(args.multi_rps)
    mode_ctrl = ModeController(args.max_threads, args.multi_rps, args.single_rps, rate_limiter,
                               on_blocked=lambda rps: record_rate_failure(output_dir, rps, logger))
    if speed_profile.get("ceiling_rps"):
        mode_ctrl.set_ceiling(speed_profile["ceiling_rps"])
        logger.info("Learned ceiling applied: %.1f req/s (known_bad=%s) seed=%.1f",
                    speed_profile["ceiling_rps"], speed_profile.get("known_bad_rps"),
                    args.multi_rps)
    stats = Stats(args.start_height, args.end_height,
                  prior_blocks_done=prior_blocks_done,
                  prior_tx_total=prior_tx_total,
                  prior_bytes_written=prior_bytes_written,
                  prior_files_finalized=prior_files_finalized)
    client = MempoolClient(args.api_base, rate_limiter, stats=stats, verify_ntx=args.verify_ntx)
    client.payload = args.store

    pause_event = threading.Event()
    pause_event.set()  # start unpaused
    verbose_flag = threading.Event()
    if args.verbose:
        verbose_flag.set()
    force_finalize_event = threading.Event()
    adaptive_flag = threading.Event()
    restart_event = threading.Event()
    auto_restart_flag = threading.Event()

    coord = Coordinator(client, mode_ctrl, stats, wal, ckpt, output_dir,
                         args.file_cap_bytes, args.start_height, args.end_height,
                         resume_height, current_file_start, pending_blocks,
                         pause_event=pause_event, force_finalize_event=force_finalize_event,
                         logger=logger, roots=roots, headers=headers, store=args.store)

    threads = []
    for i in range(args.max_threads):
        t = threading.Thread(target=coord.worker_loop, args=(i,), daemon=True)
        t.start()
        threads.append(t)

    kb_ctrl = KeyboardController(rate_limiter, mode_ctrl, pause_event, verbose_flag,
                                  force_finalize_event, adaptive_flag, restart_event,
                                  logger, coord.stop_event,
                                  auto_restart_flag=auto_restart_flag)
    if HAVE_MSVCRT:
        kb_thread = threading.Thread(target=kb_ctrl.loop, daemon=True)
        kb_thread.start()
    else:
        logger.info("msvcrt not available (non-Windows) -- hotkeys disabled.")

    console = Console()
    frame_interval = 1.0 / max(args.refresh_rate, 1.0)

    internet_timeout = False
    last_stats_json = 0.0
    last_adaptive_check = 0.0
    last_adaptive_rate_check = 0.0
    last_auto_restart = 0.0     # monotonic time of last auto-restart

    # If the WAL replay loaded a huge pending_blocks that already exceeds the
    # file capacity, immediately finalize it as a parquet file BEFORE entering
    # the live UI loop. This prevents the first processed block from triggering
    # an expensive serialized_size_bytes() call on hundreds of thousands of
    # blocks on the main thread, which would freeze the UI instantly.
    if pending_blocks:
        startup_est = coord._cheap_estimate_bytes(pending_blocks)
        if startup_est >= coord.file_cap_bytes * CHEAP_SIZE_CHECK_FRACTION:
            logger.info(
                "Startup: WAL has %d blocks (est. %s) — finalizing before UI loop",
                len(pending_blocks), human_bytes(startup_est),
            )
            coord.finalize_on_exit()

    live_cm = (Live(console=console, screen=not args.no_fullscreen,
                   refresh_per_second=args.refresh_rate, transient=False)
               if not args.quiet else nullcontext())
    last_status_line = 0.0
    try:
        with live_cm as live:
            while not coord.finished():
                mode_ctrl.auto_ai = not adaptive_flag.is_set()
                mode_ctrl.tick()
                coord.process_ready_results()

                if force_finalize_event.is_set():
                    force_finalize_event.clear()
                    coord.force_finalize()

                if time.monotonic() - stats.last_success_time > 600:
                    internet_timeout = True
                    coord.stop_event.set()
                    break

                snap = stats.snapshot()
                # Fresh 10s bps — computed right now, not from the snapshot
                _now = time.monotonic()
                with stats._lock:
                    _w10 = [t for t in stats.block_done_times if _now - t <= 10.0]
                    _e10 = _now - stats.session_start
                snap["recent_bps_10s"] = len(_w10) / min(10.0, max(0.001, _e10))
                mode_snap = mode_ctrl.snapshot()
                kb_snap = kb_ctrl.snapshot()
                if not args.quiet:
                    live.update(render_dashboard(snap, mode_snap, args.max_threads, args.refresh_rate,
                                                  theme=args.theme, compact=args.compact,
                                                  verbose=verbose_flag.is_set(), kb_snap=kb_snap,
                                                  output_dir=output_dir,
                                                  auto_restart_active=auto_restart_flag.is_set(),
                                                  minimal=args.minimal))
                elif time.monotonic() - last_status_line >= 5.0:
                    last_status_line = time.monotonic()
                    print(f"[quiet] height {coord.next_expected - 1}/{args.end_height} "
                          f"tx={stats.tx_total} mode={mode_snap['mode']} "
                          f"workers={mode_snap['allowed_workers']} file={coord.current_file_start}",
                          flush=True)

                if args.stats_json_interval > 0 and time.monotonic() - last_stats_json >= args.stats_json_interval:
                    export_stats_json(output_dir, snap, mode_snap, kb_snap)
                    last_stats_json = time.monotonic()

                # ---- Worker-aware slowdown when idle (every ~4s) ----
                # Only when MOST workers idled, and floored: -0.05/2s used to
                # walk 1.00x down to 0.10x on ordinary latency jitter.
                now = time.monotonic()
                if now - last_adaptive_check >= 4.0:
                    last_adaptive_check = now
                    ts = snap.get("thread_status", {})
                    active = sum(1 for v in ts.values() if v.get("state") == "fetching")
                    total = args.max_threads
                    if (len(ts) >= total and total > 1 and active < total / 2
                            and mode_snap["mode"] == "multi"
                            and mode_snap["rate_multiplier"] > ADAPTIVE_IDLE_FLOOR_MULT):
                        mode_ctrl.bump_multiplier(-0.05)

                # ---- Adaptive rate control (A key) — targets blocks/10s ----
                # Damped + clamped + ceiling-limited. The old law set an
                # ABSOLUTE multiplier every 0.1s with no upper bound, so one dip
                # in the 10s average climbed until the API answered 429.
                if adaptive_flag.is_set() and not coord.stop_event.is_set():
                    if now - last_adaptive_rate_check >= ADAPTIVE_MIN_INTERVAL:
                        last_adaptive_rate_check = now
                        actual_bps_10s = snap.get("recent_bps_10s", 0.0)
                        target = kb_ctrl.adaptive_target
                        err = actual_bps_10s - target
                        if abs(err) > ADAPTIVE_DEADBAND_BPS:
                            cur = mode_snap["rate_multiplier"]
                            step = max(-ADAPTIVE_MAX_STEP,
                                       min(ADAPTIVE_MAX_STEP, -err * 0.25))
                            mode_ctrl.set_multiplier(cur + step)

                # ---- Auto-restart (L key) — detect single-thread backoff ----
                if auto_restart_flag.is_set() and not restart_event.is_set():
                    ts = snap.get("thread_status", {})
                    any_backoff = any(
                        v.get("state") == "idle (backoff)" for v in ts.values()
                    )
                    if any_backoff and now - last_auto_restart >= 30.0:
                        last_auto_restart = now
                        logger.info("Auto-restart triggered (worker in backoff)")
                        restart_event.set()

                # ---- Restart workers (P key / auto-restart) ----
                if restart_event.is_set():
                    restart_event.clear()
                    logger.info("Restarting workers via hotkey (P)")
                    coord.stop_event.set()
                    coord.finalize_on_exit()
                    for t in threads:
                        t.join(timeout=2)
                    # Clear stale heights that might have been queued mid-flight
                    while not coord.retry_queue.empty():
                        coord.retry_queue.get_nowait()
                    coord.results.clear()
                    # Sync expected height — we cleared any in-flight results
                    with coord.assign_lock:
                        coord.next_expected = coord.next_to_assign
                    # Reset backoff to clean multi-threaded state (preserve current multiplier)
                    with mode_ctrl._lock:
                        mode_ctrl.backoff_stage = 0
                        mode_ctrl.mode = "multi"
                        mode_ctrl.allowed_workers = args.max_threads
                    # Re-arm coordinator
                    coord.stop_event.clear()
                    # Restart worker threads
                    threads = []
                    for i in range(args.max_threads):
                        t = threading.Thread(target=coord.worker_loop, args=(i,), daemon=True)
                        t.start()
                        threads.append(t)
                    # Restart keyboard thread (old one exited when stop_event fired)
                    if HAVE_MSVCRT:
                        kb_thread = threading.Thread(target=kb_ctrl.loop, daemon=True)
                        kb_thread.start()
                    logger.info("Workers restarted")

                time.sleep(frame_interval)

            # drain any last stragglers, then final render
            mode_ctrl.tick()
            coord.process_ready_results()
            coord.finalize_on_exit()
            snap = stats.snapshot()
            mode_snap = mode_ctrl.snapshot()
            kb_snap = kb_ctrl.snapshot()
            if not args.quiet:
                live.update(render_dashboard(snap, mode_snap, args.max_threads, args.refresh_rate,
                                              theme=args.theme, compact=args.compact,
                                              verbose=verbose_flag.is_set(), kb_snap=kb_snap,
                                              output_dir=output_dir,
                                              minimal=args.minimal))
            export_stats_json(output_dir, snap, mode_snap, kb_snap)
    except KeyboardInterrupt:
        coord.stop_event.set()
        # Ctrl+C used to return without finalizing, stranding the whole open
        # file in the WAL (unreadable until the next start). Flush it here.
        try:
            coord.process_ready_results()
            coord.finalize_on_exit()
        except Exception as e:
            logger.warning(f"finalize on interrupt failed: {e}")
        logger.info("Interrupted by user (KeyboardInterrupt).")
        console.print("\n[yellow]Interrupted. Progress is safe in the WAL / checkpoint; "
                      "re-run the script to resume from the last completed block.[/yellow]")
        return

    coord.stop_event.set()
    for t in threads:
        t.join(timeout=2)

    if internet_timeout:
        logger.warning("No successful requests for 10 minutes -- possible internet outage. Stopping.")
        console.print(f"\n[yellow]No successful requests for 10 minutes — possible internet outage. "
                      f"Progress is safe in the WAL / checkpoint; "
                      f"re-run the script to resume from the last completed block.[/yellow]")
    else:
        logger.info(f"=== Session finished: blocks {args.start_height}-{args.end_height} complete ===")
        console.print(f"\n[green]Done.[/green] Collected txids for blocks "
                      f"{args.start_height:,}-{args.end_height:,} into {output_dir}")


if __name__ == "__main__":
    main()
