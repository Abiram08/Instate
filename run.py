"""Instate one-shot run: the full demo, executed and self-checked.

Usage:
    python run.py            # full run (~60s): seed → webhook → tick → demo → …
    python run.py --fast     # same, demo with --pace 0 and fewer entities

Every beat asserts its own success and prints PASS/FAIL. Exit code is 0
only if all beats pass. Uses a scratch database in the temp dir — your
real data is never touched. No API keys, no network, ₹0.
"""

import hashlib
import hmac
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.request

if sys.platform == "win32":  # same crash class as §4.18: force UTF-8 prints
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

SECRET = "whsec_test_secret"
TMP = tempfile.gettempdir()
DB = os.path.join(TMP, "instate_run.db")
DB_URL = f"sqlite+aiosqlite:///{DB}"
PAYLOAD = {"event": "payment.failed", "id": "evt_run_001",
           "payload": {"payment": {"id": "pay_run1", "error_reason": "insufficient_funds",
                                   "amount": 49900}}}

ENV = {**os.environ, "INSTATE_DATABASE_URL": DB_URL,
       "PYTHONIOENCODING": "utf-8"}
BEATS: list[tuple[str, bool, str]] = []


def beat(name: str, ok: bool, detail: str = "") -> None:
    BEATS.append((name, ok, detail))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""), flush=True)
    if not ok:
        raise SystemExit(f"\nSTOPPED at '{name}': {detail}")


def cli(*args: str, timeout: int = 300) -> str:
    # Bytes, decoded as UTF-8 here: the child writes UTF-8 (cli forces it on
    # win32) but the parent locale is cp1252 — text=True would crash (§4.18).
    r = subprocess.run([sys.executable, "-m", "instate.surfaces.cli", *args],
                       capture_output=True, text=False, timeout=timeout,
                       env=ENV, cwd=os.path.dirname(os.path.abspath(__file__)))
    out = (r.stdout or b"").decode("utf-8", errors="replace")
    err = (r.stderr or b"").decode("utf-8", errors="replace")
    return out + err


def main() -> None:
    fast = "--fast" in sys.argv
    entities = "6" if fast else "10"
    pace = ["--pace", "0"] if fast else []

    if os.path.exists(DB):
        os.remove(DB)

    # 1 · seed
    out = cli("seed", "--entities", entities)
    beat("seed", "history events" in out, out.strip().splitlines()[-3].strip() if out else "")

    # 2 · verify
    out = cli("verify")
    beat("verify", "zero breaks" in out)

    # 3 · webhook: live server, signed body, exactly like production ingest
    body = json.dumps(PAYLOAD).encode()
    sig = hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()
    srv = subprocess.Popen(
        [sys.executable, "-m", "instate.surfaces.cli", "serve", "webhook", "--port", "8009"],
        env=ENV, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        for _ in range(60):
            try:
                urllib.request.urlopen("http://127.0.0.1:8009/health", timeout=2)
                break
            except OSError:
                time.sleep(1)
        req = urllib.request.Request(
            "http://127.0.0.1:8009/webhook", data=body,
            headers={"X-Razorpay-Signature": sig})
        with urllib.request.urlopen(req, timeout=30) as resp:
            ack, code = resp.read().decode(), resp.status
        beat("webhook", code == 200 and ack.startswith("captured #"), f"HTTP {code}: {ack}")

        # redelivery: inert, still 200, same bookmark
        with urllib.request.urlopen(req, timeout=30) as resp:
            ack2 = resp.read().decode()
        same_head = ack.split("head=")[1] == ack2.split("head=")[1]
        beat("redelivery", "duplicate" in ack2 and same_head, ack2)
    finally:
        srv.terminate()

    # 4 · tick → decision exists
    out = cli("worker", "tick")
    beat("tick", "failures decided" in out, out.strip().splitlines()[-1].strip())

    # 5 · timeline shows the trail
    out = cli("timeline", "pay_run1")
    beat("timeline", "PaymentFailed" in out and "FailureDiagnosed" in out)

    # 6 · explain opens decision 1 (fresh DB → insertion order)
    out = cli("explain", "1")
    beat("explain", "gate-1" in out and "model proposal" in out and "inputs_hash" in out)

    # 7 · measured comparison
    out = cli("demo", "--entities", entities, *pace)
    beat("demo", "net money recovered" in out and "0 breaks" in out,
         "escalated row: " + ("yes" if "escalated entities" in out else "NO"))

    # 8 · boot reconciliation (nothing dangling → quiet)
    out = cli("worker", "resume")
    beat("resume", "nothing dangling" in out or "reconciled" in out)

    # 9 · rebuild: zero drift
    out = cli("rebuild")
    beat("rebuild", "zero drift" in out)

    # 10 · replay: the product moment moves numbers
    out = cli("replay", "--set", "retry_spacing_24h=0")
    beat("replay", "verdict changes" in out and "0 verdict changes" not in out,
         [ln.strip() for ln in out.splitlines() if "verdict changes" in ln][0])

    # 11 · prod readiness
    out = cli("prod")
    beat("prod", "Production gaps closed" in out)

    print(f"\nALL {len(BEATS)} BEATS PASS  (db: {DB})")


if __name__ == "__main__":
    main()
