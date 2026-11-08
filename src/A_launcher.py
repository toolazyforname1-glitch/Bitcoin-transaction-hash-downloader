import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

import A_speed_probe
import A_txid_collector

SCRIPT_DIR = Path(__file__).resolve().parent
COLLECTOR = SCRIPT_DIR / "A_txid_collector.py"
DEFAULT_OUT = SCRIPT_DIR.parent / "data"
API_BASE = "https://mempool.space/api"

console = Console()


def clear():
    if console.is_terminal:
        console.clear()
    else:
        os.system("cls" if os.name == "nt" else "clear")


def getch():
    if os.name != "nt":
        return sys.stdin.read(1)
    import msvcrt
    ch = msvcrt.getch()
    if ch in (b"\x00", b"\xe0"):
        return {"H": "up", "P": "down"}.get(
            msvcrt.getch().decode(errors="ignore"), "")
    return ch.decode(errors="ignore")


def banner(title, subtitle=""):
    body = f"[bold]{title}[/]"
    if subtitle:
        body += f"\n[dim]{subtitle}[/]"
    console.print(Panel.fit(body, border_style="cyan", padding=(0, 2)))


def choose(title, options, default=0, help_text=""):
    idx = default
    width = max(len(o[0]) for o in options) + 6
    while True:
        clear()
        banner(title, help_text)
        for i, (label, _) in enumerate(options):
            if i == idx:
                console.print(f"  [bold reverse]  {label:<{width}}[/]")
            else:
                console.print(f"    [dim]{label}[/]")
        console.print("\n[dim]up/down move    Enter select    Esc cancel[/]")
        key = getch()
        if key == "up":
            idx = (idx - 1) % len(options)
        elif key == "down":
            idx = (idx + 1) % len(options)
        elif key in ("\r", "\n"):
            return options[idx][1]
        elif key == "\x1b":
            raise KeyboardInterrupt


def ask_int(title, default, lo, hi, help_text=""):
    while True:
        clear()
        banner(title, help_text)
        raw = console.input(f"  [bold]{title}[/] [[dim]{default}[/]]: ").strip()
        if not raw:
            return default
        try:
            val = int(raw.replace(",", "").replace("_", "").replace(" ", ""))
        except ValueError:
            console.print("  [red]not a number[/]")
            time.sleep(1.1)
            continue
        if lo <= val <= hi:
            return val
        console.print(f"  [red]must be between {lo:,} and {hi:,}[/]")
        time.sleep(1.1)


def ask_text(title, default, help_text=""):
    clear()
    banner(title, help_text)
    raw = console.input(f"  [bold]{title}[/] [[dim]{default}[/]]: ").strip().strip('"')
    return raw or str(default)


def read_json(path):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return {}


def resume_state():
    ck = read_json(DEFAULT_OUT / "checkpoint.json")
    return ck.get("last_completed_height"), ck.get("tx_total_cumulative", 0)


def tip_height():
    try:
        return int(requests.get(f"{API_BASE}/blocks/tip/height", timeout=8).text.strip())
    except Exception:
        return None


def parse_stamp(text):
    parts = text.strip().replace("-", "/").replace(" ", "/").split("/")
    parts = [p for p in parts if p != ""]
    if len(parts) != 5:
        return None
    try:
        year, month, day, hour, minute = (int(p) for p in parts)
    except ValueError:
        return None
    if len(parts[4]) == 1:
        minute *= 10
    if minute % 10 or not 0 <= minute <= 50:
        return None
    try:
        return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
    except ValueError:
        return None


def ask_stamp(title, default):
    while True:
        clear()
        banner(title, "YYYY/MM/DD/HH/MM  -  minutes in steps of 10  -  UTC")
        raw = console.input(f"  [bold]{title}[/] [[dim]{default}[/]]: ").strip()
        if not raw:
            return parse_stamp(default)
        parsed = parse_stamp(raw)
        if parsed:
            return parsed
        console.print("  [red]expected YYYY/MM/DD/HH/MM, minutes 00 10 20 30 40 50[/]")
        time.sleep(1.7)


def block_at_timestamp(ts):
    try:
        r = requests.get(f"{API_BASE}/v1/mining/blocks/timestamp/{int(ts)}", timeout=15)
        if r.status_code != 200:
            return None
        j = r.json()
        return int(j["height"]), int(j["timestamp"])
    except Exception:
        return None


def ask_date_range():
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y/%m/%d/00/00")
    month_ago = (now - timedelta(days=30)).strftime("%Y/%m/%d/00/00")
    for _ in range(3):
        t1 = ask_stamp("Start", month_ago)
        t2 = ask_stamp("End", today)
        if t2 <= t1:
            console.print("\n  [red]end must be after start[/]")
            time.sleep(1.6)
            continue
        clear()
        banner("Resolving to block heights", "asking mempool.space for the boundary blocks")
        with console.status("[cyan]looking up...[/]"):
            a = block_at_timestamp(t1.timestamp())
            b = block_at_timestamp(t2.timestamp())
        if not a or not b:
            console.print("\n  [red]could not resolve - mempool.space unreachable[/]")
            time.sleep(1.6)
            continue
        start_h = a[0] + (1 if a[1] < int(t1.timestamp()) else 0)
        end_h = b[0]
        if end_h < start_h:
            console.print("\n  [red]that range contains no blocks[/]")
            time.sleep(1.6)
            continue
        label = (f"{t1.strftime('%Y/%m/%d/%H:%M')} to "
                 f"{t2.strftime('%Y/%m/%d/%H:%M')} UTC")
        console.print(f"\n  [green]blocks {start_h:,} to {end_h:,}[/]"
                      f"  ({end_h - start_h + 1:,} blocks)")
        time.sleep(1.4)
        return label, start_h, end_h
    return None


def collect_params():
    prof = read_json(DEFAULT_OUT / "speed_profile.json")
    seed = prof.get("safe_rps")
    ceiling = prof.get("ceiling_rps")
    last, txs = resume_state()

    subtitle = f"collector: {COLLECTOR.name}"
    if last:
        subtitle += f"   last block: {last:,}   txids: {txs:,}"
    if ceiling:
        subtitle += f"   learned ceiling: {ceiling:.1f} req/s"
    clear()
    banner("Bitcoin txid collector - setup", subtitle)
    console.print()

    p = {"start": None, "max_blocks": None, "end": None, "window": None}
    p["store"] = choose(
        "What do you want to download?",
        [("Transaction hashes - every txid, in block order (~90 GB)", "txids"),
         ("Merkle roots only - one 32-byte hash per block (~70 MB)", "roots"),
         ("Block headers - 80 bytes per block, includes proof-of-work (~150 MB)", "headers")],
        help_text="Hashes are the full archive; roots and headers are compact summaries")
    p["mode"] = choose(
        "What kind of run?",
        [("Resume where it left off", "resume"),
         ("Recent blocks - last 1000 up to the tip", "recent"),
         ("Date range - pick dates and hours (UTC)", "dates"),
         ("Capped run - choose start height and block count", "capped"),
         ("Full history - height 1 to the tip", "full"),
         ("Dry run - 3 blocks into dryrun/", "dry")],
        help_text="Pick with up/down, confirm with Enter")

    if p["mode"] == "dates":
        got = ask_date_range()
        if got:
            p["window"], p["start"], p["end"] = got
        else:
            console.print("\n  [yellow]falling back to manual heights[/]")
            time.sleep(1.4)
            p["mode"] = "capped"

    if p["mode"] == "capped":
        p["start"] = ask_int("Start block height", 900000, 1, 20_000_000)
        p["max_blocks"] = ask_int("How many blocks", 500, 1, 5_000_000)
    elif p["mode"] in ("recent", "full"):
        tip = tip_height()
        if tip is None:
            console.print("\n  [red]mempool.space unreachable - enter the range manually[/]")
            time.sleep(1.6)
            p["start"] = ask_int("Start block height", 900000, 1, 20_000_000)
            p["max_blocks"] = ask_int("How many blocks", 1000, 1, 5_000_000)
            p["mode"] = "capped"
        elif p["mode"] == "recent":
            p["start"] = max(1, tip - 999)
            p["max_blocks"] = 1000
        else:
            p["end"] = tip

    p["threads"] = choose("Worker threads",
                          [(f"{n} threads", n) for n in (4, 8, 12, 16)], default=1)

    rate_default = int(round(seed)) if seed else 8
    p["rate"] = ask_int("Base request rate (req/s before AIMD)", rate_default, 1, 300,
                        help_text="AIMD adapts from here; the learned ceiling still caps it")

    if p["store"] == "txids":
        p["file_cap"] = choose("Max size per parquet file",
                               [("256 MB", 256 * 1024**2), ("512 MB", 512 * 1024**2),
                                ("1 GiB", 1024**3), ("2 GiB", 2 * 1024**3)], default=2)
    else:
        p["file_cap"] = 1024**3

    p["theme"] = choose("Theme", [("dark", "dark"), ("neon", "neon"), ("mono", "mono")])
    if p["store"] == "txids":
        p["verify"] = choose("Verify tx count against each block header?",
                             [("No - fastest", False),
                              ("Yes - one extra request per block", True)])
    else:
        p["verify"] = False
    p["dashboard"] = choose("Dashboard",
                            [("Minimal - progress, ETA and speed only", "minimal"),
                             ("Full - every panel", "full"),
                             ("Compact - two columns", "compact")])
    p["out"] = ask_text("Output folder", DEFAULT_OUT)
    return p


def build_args(p):
    a = []
    if p["mode"] == "dry":
        a.append("--dry-run")
    if p["start"] is not None:
        a += ["--start-height", str(p["start"])]
    if p["max_blocks"] is not None:
        a += ["--max-blocks", str(p["max_blocks"])]
    if p["end"] is not None:
        a += ["--end-height", str(p["end"])]
    a += ["--max-threads", str(p["threads"])]
    a += ["--multi-rps", str(p["rate"])]
    a += ["--file-cap-bytes", str(p["file_cap"])]
    a += ["--theme", p["theme"]]
    if p["verify"]:
        a.append("--verify-ntx")
    if p["dashboard"] == "compact":
        a.append("--compact")
    elif p["dashboard"] == "minimal":
        a.append("--minimal")
    a += ["--store", p["store"]]
    a += ["--output-dir", p["out"]]
    return a


AVERAGE_BYTES_PER_BLOCK = 90 * 1024


def estimate_window(p):
    if p["start"] is not None and p["end"] is not None:
        blocks = p["end"] - p["start"] + 1
    elif p["start"] is not None and p["max_blocks"] is not None:
        blocks = p["max_blocks"]
    elif p["mode"] == "full" and p["end"]:
        blocks = p["end"]
    else:
        return None
    seconds = 2 * blocks / max(1, p["rate"])
    per_block = {"txids": AVERAGE_BYTES_PER_BLOCK, "roots": 72, "headers": 180}[p["store"]]
    return blocks, seconds, blocks * per_block


def human_duration(seconds):
    if seconds < 90:
        return f"{seconds:.0f} seconds"
    minutes = seconds / 60
    if minutes < 90:
        return f"{minutes:.0f} minutes"
    hours = minutes / 60
    if hours < 48:
        return f"{hours:.1f} hours"
    return f"{hours / 24:.1f} days"


def summary_table(p, args):
    labels = {"resume": "resume where it left off",
              "recent": "recent blocks (last 1000)",
              "dates": "date range",
              "capped": "capped run",
              "full": "full history to the tip",
              "dry": "dry run"}
    t = Table.grid(padding=(0, 2))
    t.add_column(style="dim", justify="right")
    t.add_column()
    t.add_row("run", labels[p["mode"]])
    if p.get("window"):
        t.add_row("window", p["window"])
    if p["start"] is not None:
        t.add_row("start height", f"{p['start']:,}")
    if p["max_blocks"] is not None:
        t.add_row("blocks", f"{p['max_blocks']:,}")
    if p["end"] is not None:
        t.add_row("end height", f"{p['end']:,}")
    t.add_row("threads", str(p["threads"]))
    t.add_row("base rate", f"{p['rate']} req/s")
    t.add_row("file cap", f"{p['file_cap'] / 1024**2:.0f} MB")
    t.add_row("theme", p["theme"])
    t.add_row("verify", "yes" if p["verify"] else "no")
    t.add_row("layout", p["dashboard"])
    t.add_row("store", {"txids": "every txid",
                        "roots": "merkle roots only",
                        "headers": "block headers"}[p["store"]])
    t.add_row("output", p["out"])
    est = estimate_window(p)
    if est:
        blocks, seconds, nbytes = est
        t.add_row("blocks in range", f"{blocks:,}")
        t.add_row("estimated time", human_duration(seconds))
        t.add_row("estimated size", f"{nbytes / 1e9:.0f} GB" if nbytes >= 1e9
                                   else f"{nbytes / 1e6:.0f} MB")
    if p["rate"] > p["threads"] * 2:
        t.add_row("note", f"{p['threads']} threads sustain ~{p['threads'] * 2} req/s "
                          f"at 0.5s latency - raise threads or lower the rate")
    t.add_row("command", " ".join(args))
    return t


def main():
    argv = sys.argv[1:]
    if argv[:1] == ["--cli"]:
        sys.argv = ["A_txid_collector"] + argv[1:]
        return A_txid_collector.main() or 0
    if argv[:1] == ["--probe"]:
        sys.argv = ["A_speed_probe"] + argv[1:]
        return A_speed_probe.main() or 0

    while True:
        p = collect_params()
        args = build_args(p)
        clear()
        banner("Ready to launch")
        console.print(Panel(summary_table(p, args), border_style="green", padding=(0, 1)))
        action = choose("Launch now?",
                        [("Launch", "go"), ("Start over", "again"), ("Cancel", "quit")])
        if action == "quit":
            return 0
        if action == "again":
            continue
        clear()
        sys.argv = ["A_txid_collector"] + args
        return A_txid_collector.main() or 0


if __name__ == "__main__":
    try:
        code = main()
    except KeyboardInterrupt:
        console.print("\n[dim]cancelled[/]")
        code = 1
    sys.exit(code or 0)
