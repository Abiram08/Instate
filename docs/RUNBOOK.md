# Instate — Runbook: run it and prove it works

## 0 · The one command

```powershell
cd "Z:\Projects\Better Projects\FiMem"
pip install -e ".[dev]"   # once; Python >= 3.12
python run.py              # full run (~90s) — 12 beats, each self-checked
python run.py --fast       # same beats, smaller batch (~60s)
```

`run.py` uses a scratch database in your temp dir, starts a real webhook
server, signs and posts a real failure, ticks, explains, compares, rebuilds,
replays, and checks prod — printing `PASS`/`FAIL` per beat and exiting nonzero
on the first failure. Expected ending: `ALL 12 BEATS PASS`. No API keys, no
network, ₹0.

The rest of this file is the same run done by hand — use it when a beat
fails and you need to see the machinery, or when you're recording and need
each command separately.

One file, zero assumed knowledge. Start with `python run.py` above; every
command below was executed against a scratch database, and the **expected
outputs are pasted from real runs**. If your output matches, it works. If it
doesn't, jump to Troubleshooting.

## 0 · The dataset question (read this first)

There is **no real dataset** — no CSV, no dump, no fixtures directory. History
is generated deterministically by `instate/seed/generate.py` (seeded RNG, seed
42): subscriptions with failures, retries, promises, escalations, plus checkout
consumers and L3 precedent cases. Same seed → byte-identical history, which is
what makes the baseline-vs-agent comparison fair instead of rigged.

Real data enters through exactly one door: the **webhook** (`POST /webhook` —
`instate/surfaces/webhook.py`). A Razorpay-shaped `payment.failed` body,
HMAC-signed, appends one `PaymentFailed` event to L0. That is the production
ingest path, and the demo exercises it for real (steps 4–5). There is no batch
CSV importer — if you need one, it would be a ~30-line loader over
`core/ledger.record_event`, and it doesn't exist yet.

## 1 · Install

```powershell
cd "Z:\Projects\Better Projects\FiMem"
pip install -e ".[dev]"          # Python >= 3.12
instate --help                   # must list commands; if it tracebacks with
                                 # "No module named 'instate.cli'", reinstall:
                                 # pip install -e . --no-deps --force-reinstall
```

Use a scratch database so rehearsals never pollute anything:

```powershell
$env:INSTATE_DATABASE_URL="sqlite+aiosqlite:///C:/Users/Nella/AppData/Local/Temp/opencode/runbook.db"
$env:INSTATE_ENCRYPTION_KEY = (python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
```

No API keys. No accounts. Total cost ₹0. (`GEMINI_API_KEY` and Razorpay test
keys unlock only the two optional flexes — see §8.)

## 2 · Seed the world

```powershell
instate seed --entities 10
```

Expect:

```
│ merchant             │ 690da99d-… │
│ history events       │         59 │
│ entities             │         10 │
│ checkout consumers   │          2 │
│ precedent cases (L3) │          7 │
```

## 3 · Verify the chains

```powershell
instate verify
```

Expect: every entity `✓ verified`, ending in `✓ 12 entities verified, zero
breaks.` Any `✗ BROKEN` here means the ledger code regressed — stop and
investigate, don't continue.

## 4 · Fire a failure at the webhook (the real ingest path)

Terminal 2 (leave running):

```powershell
instate serve webhook   # → http://127.0.0.1:8000/webhook (HMAC verified)
```

Recording terminal. Save this as `payload.json` (UTF-8, no BOM — don't touch
it after):

```json
{"event":"payment.failed","id":"evt_test_001","payload":{"payment":{"id":"pay_9A1B","error_reason":"insufficient_funds","amount":49900}}}
```

Sign the **raw file bytes** and send:

```powershell
$secret = "whsec_test_secret"
$bytes = [IO.File]::ReadAllBytes("$pwd/payload.json")
$hmac = [System.Security.Cryptography.HMACSHA256]::new([Text.Encoding]::UTF8.GetBytes($secret))
$sig = ($hmac.ComputeHash($bytes) | ForEach-Object { $_.ToString("x2") }) -join ""
curl -X POST http://127.0.0.1:8000/webhook -H "X-Razorpay-Signature: $sig" --data "@payload.json"
```

Expect: `captured #7 head=3f9a…` (HTTP 200) — a bookmark naming the exact
ledger position this delivery owns: event id + chain head. Wrong/absent
signature → `401 invalid or missing signature`. Resend the same body →
`200 duplicate — already captured #7 head=3f9a…` with the *identical* head
(exactly-once; redeliveries are inert but still name their position).

## 5 · Tick, then read the trail

The webhook only appends. The worker step decides:

```powershell
instate worker tick
# ✓ tick complete — 1/1 failures decided (scripted model (reproducible))

instate timeline pay_9A1B
#   PaymentFailed      ₹499 · code=insufficient_funds
#   FailureDiagnosed   code=insufficient_funds · cause=insufficient_funds
#   RetryScheduled     due <48h out>

# Pinned snapshot — what was visible at noon, ignoring everything after:
instate timeline pay_9A1B --as-of 2026-09-05T12:00:00+00:00
```

## 6 · Open the decision

```powershell
instate explain 1
#   root cause      insufficient_funds
#   gate-1          retry_ceiling_7d: 0/3 → ALLOW; retry_spacing_24h: 0/1 → ALLOW
#   model proposal  RETRY_SCHEDULED · T_PLUS_48H · confidence 0.90
#   gate-2          retry_ceiling_7d: 0/3 → ALLOW; retry_spacing_24h: 0/1 → ALLOW
#   executed        RETRY_SCHEDULED
#   inputs_hash     779f…799d   (reproducible — same inputs, same output)
```

(Decision 1 is `pay_9A1B` on a fresh DB — insertion order. On a rehearsed DB,
confirm the id via `timeline` first.)

## 7 · Run the measured comparison

```powershell
instate demo --entities 10          # animated, ~20s
instate demo --entities 10 --pace 0 # same run, no animation (CI)
```

Expect: Live view (header / pipeline / scoreboard), then the still tables —
`net money recovered ₹13,492 vs ₹14,490`, `attempted recovery rate 62% vs 88%`,
`compliance violations 1 vs 0`, `escalated entities 2 vs 4`, `chain verification
0 breaks`, closing with `✓ 17% of decisions resolved with zero LLM calls`.
Exact rupees shift with history; the *shape* (instate ≥ baseline, violations 0)
is the assertion.

## 8 · The two optional flexes (keys, free, never for the keeper take)

```powershell
$env:GEMINI_API_KEY="…"                          # Google AI Studio free tier
instate worker tick --llm                        # real model, failover → policy default

$env:RAZORPAY_KEY_ID="rzp_test_…"
$env:RAZORPAY_KEY_SECRET="…"
instate demo --live                              # real test-mode gateway, BOTH arms
                                                 # (fair by construction; numbers vary)
```

## 9 · Full self-check (one command per layer)

```powershell
python -m pytest -q        # 364 passed, 1 skipped (~75s)
ruff check instate tests   # All checks passed!
```

## Troubleshooting (every failure seen in practice)

| Symptom | Cause | Fix |
|---|---|---|
| `instate` tracebacks `No module named 'instate.cli'` | stale installed entry point | `pip install -e . --no-deps --force-reinstall` |
| webhook → `401` | signature not over raw bytes | sign with `[IO.File]::ReadAllBytes`, not `Get-Content`; don't re-save the file |
| `explain N` → `not found` | no decision yet — webhook only appends | run `instate worker tick` first |
| tick → `LookupError: no policy rows for entity_type='payment'` | fixed: `seed` now seeds both `subscription` + `payment` | upgrade + re-seed |
| `rebuild` → `DRIFT DETECTED` on first run ever | stale L1 (fixed: due-scheduled outcomes now refold) | second run is clean; on current code first run is clean too |
| `replay` → `no decisions to replay` | nothing decided yet, or replay already consumed (one-shot per DB: it bumps policy version) | `worker tick` / `demo` first; rehearse replay on scratch, record on fresh |
| `replay --set retry_ceiling_7d=2` → `0 verdict changes` | nothing in this history sits at exactly 2/3 (correct, not a bug) | use `--set retry_spacing_24h=0` for the guaranteed product beat |
| `worker --resume` → `No such option` | it's a subcommand | `instate worker resume` |
| `prod` encryption row → `no key set` | no Fernet key in env | set `INSTATE_ENCRYPTION_KEY` (step 1) before recording |
