#!/usr/bin/env python3
"""Probe the fastest SUSTAINED request rate mempool.space tolerates.

Holds each rate for a full --dwell (a 429 can take minutes to appear), then
writes speed_profile.json: safe_rps, known_bad_rps and a hard ceiling_rps that
A_txid_collector.py loads and never exceeds.
"""
import argparse
import json
import os
import sys
import threading
import time
from pathlib import Path

import requests

if getattr(sys, "frozen", False):
    SCRIPT_DIR = Path(sys.executable).resolve().parent
    ROOT_DIR = SCRIPT_DIR
else:
    SCRIPT_DIR = Path(__file__).resolve().parent
    ROOT_DIR = SCRIPT_DIR.parent
PROFILE_NAME = "speed_profile.json"
TIMEOUT = 15


class RateLimiter:
    def __init__(self, rate_per_sec):
        self._lock = threading.Lock()
        self.rate = rate_per_sec
        self._last = time.monotonic()
        self._allowance = rate_per_sec

    def acquire(self):
        while True:
            with self._lock:
                now = time.monotonic()
                self._allowance += (now - self._last) * self.rate
                self._last = now
                if self._allowance > self.rate:
                    self._allowance = self.rate
                if self._allowance >= 1.0:
                    self._allowance -= 1.0
                    return
                needed = (1.0 - self._allowance) / self.rate
            time.sleep(max(needed, 0.005))


def fetch_block(session, api_base, height, limiter):
    limiter.acquire()
    r = session.get(f"{api_base}/block-height/{height}", timeout=TIMEOUT)
    if r.status_code != 200:
        return r.status_code, 0
    limiter.acquire()
    r2 = session.get(f"{api_base}/block/{r.text.strip()}/txids", timeout=TIMEOUT)
    if r2.status_code != 200:
        return r2.status_code, 0
    return 200, len(r2.json())


def probe_step(rps, dwell, threads, start_height, api_base, p95_ms):
    limiter = RateLimiter(rps)
    stop = threading.Event()
    lock = threading.Lock()
    lat = []
    st = {"req": 0, "ok": 0, "c429": 0, "c5xx": 0, "timeout": 0, "other": 0,
          "blocks": 0, "txids": 0}
    cursor = [start_height]

    def claim():
        with lock:
            h = cursor[0]
            cursor[0] += 1
            return h

    def worker():
        session = requests.Session()
        while not stop.is_set():
            height = claim()
            try:
                t0 = time.monotonic()
                status, ntx = fetch_block(session, api_base, height, limiter)
                dt = time.monotonic() - t0
            except requests.Timeout:
                with lock:
                    st["req"] += 1
                    st["timeout"] += 1
                continue
            except requests.ConnectionError:
                with lock:
                    st["req"] += 2
                    st["timeout"] += 1
                continue
            except Exception:
                with lock:
                    st["req"] += 1
                    st["other"] += 1
                continue
            with lock:
                st["req"] += 2
                if status == 429:
                    st["c429"] += 1
                elif status >= 500:
                    st["c5xx"] += 1
                elif status == 200:
                    lat.append(dt)
                    st["blocks"] += 1
                    st["txids"] += ntx
                else:
                    st["other"] += 1

    workers = [threading.Thread(target=worker, daemon=True) for _ in range(threads)]
    for w in workers:
        w.start()
    time.sleep(dwell)
    stop.set()
    for w in workers:
        w.join(timeout=20)

    p95 = None
    if lat:
        s = sorted(lat)
        p95 = s[min(len(s) - 1, int(0.95 * (len(s) - 1)))] * 1000
    hard_fail = st["c429"] + st["c5xx"] + st["timeout"] + st["other"]
    if st["blocks"] == 0 or hard_fail:
        verdict = "THROTTLED"
    elif p95 is not None and p95 > p95_ms:
        verdict = "DEGRADED"
    else:
        verdict = "CLEAN"
    return {
        "rps": rps, "dwell_s": dwell,
        "requests": st["req"], "blocks": st["blocks"], "txids": st["txids"],
        "effective_rps": round(st["req"] / dwell, 2),
        "c429": st["c429"], "c5xx": st["c5xx"], "timeouts": st["timeout"],
        "other": st["other"], "p95_ms": round(p95, 1) if p95 else None,
        "verdict": verdict,
    }


def load_profile(path):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_profile(path, prof):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    prof["updated"] = time.strftime("%Y-%m-%dT%H:%M:%S")
    tmp.write_text(json.dumps(prof, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def main():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--api-base", default="https://mempool.space/api")
    p.add_argument("--start-height", type=int, default=None,
                   help="default: tip - 2000")
    p.add_argument("--dwell", type=float, default=180.0,
                   help="seconds to hold each rate; 429s can be delayed by minutes")
    p.add_argument("--threads", type=int, default=8)
    p.add_argument("--start-rps", type=float, default=2.0)
    p.add_argument("--max-rps", type=float, default=64.0)
    p.add_argument("--factor", type=float, default=1.35)
    p.add_argument("--safety", type=float, default=0.70,
                   help="ceiling = failing rate * safety")
    p.add_argument("--cooldown", type=float, default=300.0)
    p.add_argument("--p95-ms", type=float, default=3000.0)
    p.add_argument("--max-steps", type=int, default=8)
    p.add_argument("--profile", default=str(ROOT_DIR / "data" / PROFILE_NAME))
    args = p.parse_args()

    if args.start_height is None:
        tip = int(requests.get(f"{args.api_base}/blocks/tip/height", timeout=TIMEOUT).text)
        args.start_height = max(1, tip - 2000)

    prof = load_profile(args.profile)
    known_bad = prof.get("known_bad_rps")
    ceiling = prof.get("ceiling_rps")
    safe = prof.get("safe_rps") or args.start_rps
    rps = args.start_rps
    if ceiling:
        rps = min(rps, ceiling)
        print(f"profile: safe={safe} known_bad={known_bad} ceiling={ceiling} -> start {rps:.1f}")

    print(f"probing {args.api_base} from height {args.start_height}, "
          f"dwell {args.dwell:.0f}s, {args.threads} threads")
    print(f"{'step':>4} {'req/s':>7} {'eff':>7} {'blocks':>7} {'429':>5} "
          f"{'5xx':>4} {'to':>3} {'p95ms':>7}  verdict")
    print("-" * 74)

    degraded = 0
    for step in range(1, args.max_steps + 1):
        if rps > args.max_rps:
            print(f"reached --max-rps {args.max_rps}; stopping")
            break
        if known_bad and rps >= known_bad * 0.95:
            print(f"{rps:.1f} req/s is at/above the known-bad rate {known_bad:.1f}; "
                  f"refusing to probe it")
            break

        res = probe_step(rps, args.dwell, args.threads, args.start_height,
                         args.api_base, args.p95_ms)
        print(f"{step:>4} {res['rps']:>7.1f} {res['effective_rps']:>7.1f} "
              f"{res['blocks']:>7} {res['c429']:>5} {res['c5xx']:>4} "
              f"{res['timeouts']:>3} "
              f"{(res['p95_ms'] or 0):>7.0f}  {res['verdict']}")

        prof.setdefault("history", []).append(
            {"t": time.strftime("%Y-%m-%dT%H:%M:%S"), **res})
        prof["history"] = prof["history"][-100:]

        if res["verdict"] == "CLEAN":
            degraded = 0
            safe = max(safe, rps)
            prof["safe_rps"] = safe
            save_profile(args.profile, prof)
            rps *= args.factor
        elif res["verdict"] == "DEGRADED" and degraded < 1:
            degraded += 1
            print(f"     p95 {res['p95_ms']:.0f}ms > {args.p95_ms:.0f}ms but no 429 — "
                  f"re-probing {rps:.1f} req/s once to confirm")
        else:
            known_bad = rps if known_bad is None else min(known_bad, rps)
            prof["known_bad_rps"] = known_bad
            prof["safe_rps"] = safe
            prof["ceiling_rps"] = min(prof.get("ceiling_rps") or 1e9,
                                      round(rps * args.safety, 2))
            save_profile(args.profile, prof)
            print(f"\nthrottled at {rps:.1f} req/s -> never probing at or above it again")
            print(f"ceiling set to {prof['ceiling_rps']:.1f} req/s "
                  f"({args.safety:.0%} of the failing rate)")
            if args.cooldown > 0:
                print(f"cooling down {args.cooldown:.0f}s before releasing the API")
                time.sleep(args.cooldown)
            break
    else:
        print(f"\nstopped after --max-steps; last clean rate {safe:.1f} req/s")

    print(f"\nprofile: {args.profile}")
    print(f"  safe_rps      {prof.get('safe_rps')}")
    print(f"  known_bad_rps {prof.get('known_bad_rps')}")
    print(f"  ceiling_rps   {prof.get('ceiling_rps')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
