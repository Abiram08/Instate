# Demo Video — Instate (Track 03: AI Revenue Recovery)
**Length: 5:00 — start at payment failure, end at `instate prod`**

> **Goal:** Prove the bar — *measured money recovered, compliant escalation, stopping rules, audit trail* — with a terminal that makes the memory tangible. No web app, no slides as proof. The demo is one `Live` view (header / pipeline / scoreboard), then a still comparison table with a delta column.

## Setup from zero (do once, before pre-flight)

```powershell
# 1 · Python 3.12+, then install (editable while developing):
cd "Z:\Projects\Better Projects\FiMem"
pip install -e ".[dev]"

# 2 · the `instate` command must print the command list (not a traceback):
instate --help
# seed → timeline → verify → explain → demo → worker → …
```

**API keys: none required for the keeper take.** The recording path is fully
offline — scripted model, stand-in gateway, test HMAC secret. Keys exist only
for the two optional flexes, and both are free:

| Key | Unlocks | Needed for recording? | Cost |
|---|---|---|---|
| none | everything in this script | — | ₹0 |
| `GEMINI_API_KEY` | `worker tick --llm` (real model instead of scripted) | No | Google AI Studio free tier covers it |
| `RAZORPAY_KEY_ID` / `RAZORPAY_KEY_SECRET` (test mode) | `demo --live` (real test-mode gateway on both arms) | No — and don't record with it (numbers vary) | free, test mode moves no money |
| `whsec_test_secret` | signing the demo webhook bodies | It's a placeholder default, not a real secret — never a live secret on camera | — |

> Total cost of the demo video: **₹0 and no accounts**. If a judge asks "what
> does it cost to run", the answer is the `cost / decision` row in the final
> table (~$0.002/decision input tokens on the scripted profile; production
> cost = your provider's per-1k rate × ~1k bounded tokens + zero for every
> decision the gates resolve before the model).

## Timing budget — the 5:00 game

| Beat | Time | What happens | Why it fits |
|---|---|---|---|
| Hook | 0:00-0:25 (25s) | `instate` banner + thesis | Memorized 4-sentence monologue, no typing |
| Failure | 0:25-1:00 (35s) | `seed`, `verify`, signed webhook, `timeline` | Webhook returns instantly; timeline is one screen |
| Decision | 1:00-1:45 (45s) | `explain 1`, read the evidence lines | Still output — talk at your pace |
| Hard-decline | 1:45-2:30 (45s) | second signed failure, `timeline pay_dead` | Same ritual, no new concepts to type |
| Measured proof | 2:30-3:15 (45s) | `instate demo --entities 10` (~20s animated) + read the table | 6-entity batch = ~2s compute, ~20s at pace 0.45 — leaves ~25s to read the table |
| Reconcile | 3:15-3:45 (30s) | `worker resume`, point at dim→green | Pre-staged dangling intent, instant |
| Audit + replay | 3:45-4:10 (25s) | `rebuild`, `replay`, open console | Both commands return in seconds |
| Prod close | 4:10-5:00 (50s) | `instate prod`, 12 rows, final line | Still table + memorized close |

Total: 300s. Slack lives in the demo beat (~25s of table-reading you can
compress) and the close (cut the 12 rows to the 4 that matter if you're behind:
snapshots, RLS, HITL-learns, canary-by-replay). Rules: record the keeper with
`--entities 10` (measured ~20s animated); if any beat overruns, trim words, not
commands — every command above returns in seconds except the demo, and the demo
is timed. One full timed dry run before the keeper, stopwatch in hand.

---

## Pre-flight (2 min before record)

```powershell
cd "Z:\Projects\Better Projects\FiMem"
Remove-Item instate.db -ErrorAction SilentlyContinue
$env:INSTATE_DATABASE_URL="sqlite+aiosqlite:///instate.db"
# Fernet key so the `prod` encryption row reads ok, not "no key set":
$env:INSTATE_ENCRYPTION_KEY = (python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
# clean terminal: 140x36 minimum (the Live layout needs width), font 14 (Cascadia Code)
# OBS: capture terminal only, 1080p, 30fps, mic test
# Chrome ready: http://127.0.0.1:8002/ (closed, will open at 3:30)
```

> Rehearse on a scratch DB, record on a fresh one. `replay` is one-shot per
> database (it bumps the policy version), and `verify`/`timeline` counts depend
> on insertion order — a rehearsed DB gives different numbers than a fresh one.

Flags that matter:
- `instate demo --entities 10` — animated run (0.45s per stage reveal). Default.
  **Record with this.** Fully offline, byte-reproducible (seed 42).
- `instate demo --entities 10 --pace 0` — same run, no animation (CI / flaky terminal).
- `instate demo --live` — swaps the stand-in gateway for the real Razorpay
  **test-mode** gateway on *both* arms (same gateway, still fair — only the
  memory layer differs). Needs `RAZORPAY_KEY_ID` / `RAZORPAY_KEY_SECRET` in env.
  Numbers vary run to run and it needs network — **do not record with this**;
  use it only for a live Q&A flex, never as the keeper take. Without keys it
  falls back to stand-in and says so out loud.
- Never claim live money moved unless `--live` actually ran — and even then,
  only test-mode, never real captures.

**The game, in one paragraph:** nobody calls an API to *cause* a failure —
Razorpay pushes `payment.failed` to *you*. Receiving needs zero keys. The demo
therefore has two independent beats: (1) a signed failure lands on your local
webhook and is decided (needs no keys, no network); (2) the batch comparison
runs offline on the stand-in gateway (reproducible). Keys enter only if you
choose `--live` for beat 2. If a judge asks "is this live", the answer is:
"the ledger path is real — HMAC-verified webhook to decision; the gateway
behind the comparison is a deterministic stand-in unless I pass `--live`."

Payload on disk `payload.json` (the only input the system needs):
```json
{"event":"payment.failed","id":"evt_test_001","payload":{"payment":{"id":"pay_9A1B","error_reason":"insufficient_funds","amount":49900}}}
```

---

## Shot list — copy-paste commands + exactly what to say

### 0:00-0:25 — Hook (problem, no code yet)
**Screen:** `instate` bare → banner

```powershell
instate
```

**Say:**
> "Revenue recovery agents are built as single-shot pipelines. Snapshot in, decision out. They break at the second attempt. Instate is the memory layer that answers three questions every human analyst asks: what have we tried, have we hit our limit, has a case like this resolved before. Authority decreases as uncertainty increases — a stopping rule is answered by an integer, never by cosine distance."

### 0:25-1:00 — Seed + the failure (ledger-first)

```powershell
instate seed --entities 10
instate verify
```

**Say:**
> "Ten entities plus checkout consumers, precedent cases built — including backup-instrument recoveries and WhatsApp-first contacts, so the demo shows what the product actually does. Every chain verified — tamper-evident. Now a Razorpay-shaped failure lands on the local webhook."

The webhook rejects unsigned bodies (401) — the old `Signature: demo` shortcut
never returned 200. Start the receiver in a **second terminal**, then send a
properly HMAC-signed body from the recording terminal:

```powershell
# terminal 2 (before recording):
instate serve webhook   # → http://127.0.0.1:8000/webhook (HMAC verified)
```

```powershell
# recording terminal — payload.json is the only input the system needs:
# {"event":"payment.failed","id":"evt_test_001","payload":{"payment":{"id":"pay_9A1B","error_reason":"insufficient_funds","amount":49900}}}
$secret = "whsec_test_secret"   # test secret; never a live one on camera
$bytes = [IO.File]::ReadAllBytes("$pwd/payload.json")   # byte-exact: no BOM/encoding trap
$hmac = [System.Security.Cryptography.HMACSHA256]::new([Text.Encoding]::UTF8.GetBytes($secret))
$sig = ($hmac.ComputeHash($bytes) | ForEach-Object { $_.ToString("x2") }) -join ""
curl -X POST http://127.0.0.1:8000/webhook -H "X-Razorpay-Signature: $sig" --data "@payload.json"
instate worker tick
instate timeline pay_9A1B
```

> `worker tick` is one worker step (diagnose → gate → reason → execute). The
> webhook only appends — without the tick there is no decision and `explain`
> has nothing to open. Say exactly that; it's the ledger-first thesis in one line.

> Byte-exactness is the whole trick: the signature is over the raw file bytes,
> so sign with `ReadAllBytes`, not `Get-Content` (which re-encodes and breaks
> the HMAC → 401 on camera). Save `payload.json` once, verify the 200 in
> pre-flight, don't touch the file after.

**Point to:** the vertical chronological list — red `PaymentFailed`, then `FailureDiagnosed`, blue `RetryScheduled`.

**Say:**
> "Ledger-first: verify HMAC → dedupe → append → 200. The webhook returns before any gate or model runs. The tick decides afterwards. Late events append, they never rewrite a decision — bi-temporal."

**Two ways to trigger the failure (pick one in pre-flight, don't improvise):**
- **(a) Replayed test-mode delivery (recommended).** The snippet above — a real
  `payment.failed` body shape, HMAC-signed with the test secret. Offline-safe,
  identical every take. This is the keeper path.
- **(b) Real dashboard failure (flex only).** In the Razorpay dashboard
  (test mode), fail a test payment (card `4000000000000002`) with the webhook
  URL pointed at your machine via a tunnel. Same ledger path, but network +
  dashboard + tunnel can all flake mid-take — keep it for Q&A, not the recording.

### 1:00-1:45 — The decision, opened

```powershell
instate explain 1
```

**Point to:** the evidence lines — `gate-1: retry_ceiling_7d 0/3 → ALLOW; retry_spacing_24h 0/1 → ALLOW`, `model proposal: RETRY_SCHEDULED · T_PLUS_48H · confidence 0.90`, `gate-2` re-checking both, `executed: RETRY_SCHEDULED`, `inputs_hash` with the reproducibility note. (Decision 1 is `pay_9A1B` on a fresh DB — insertion order. If your rehearsal DB differs, re-check with `timeline pay_9A1B`, never guess the id on camera.)

**Say:**
> "Gate-1 checked the ceiling before the model — zero tokens. The model proposed a payday-aligned retry, Gate-2 checked the concrete timing against contact caps and DNC. Nothing the model emits reaches Razorpay unverified. That's the answer to 'what if it hallucinates' — it cannot matter."

### 1:45-2:30 — Hard-decline + at-ceiling (thesis, 0 LLM)

```powershell
instate timeline sub_004
```

`sub_004` is at-ceiling (3 retries in 7d). During the live demo its pipeline panel will show `GATE-1 … DENY → escalated` with every later stage dimmed and `(skipped — gate-1 denied, 0 tokens)`.

Switch (same signing ritual — save `card.json`, sign, send):

```powershell
# {"event":"payment.failed","id":"evt_card_001","payload":{"payment":{"id":"pay_dead","error_reason":"CARD_EXPIRED"}}}
$bytes = [IO.File]::ReadAllBytes("$pwd/card.json")
$hmac = [System.Security.Cryptography.HMACSHA256]::new([Text.Encoding]::UTF8.GetBytes($secret))
$sig = ($hmac.ComputeHash($bytes) | ForEach-Object { $_.ToString("x2") }) -join ""
curl -X POST http://127.0.0.1:8000/webhook -H "X-Razorpay-Signature: $sig" --data "@card.json"
instate worker tick
instate timeline pay_dead
```

**Say:**
> "A card_expired retry without a new method is denied until `PaymentMethodChanged` lands — hard declines are payment-method situations, not retry situations. But a stored *backup* instrument is a different story: `RETRY_BACKUP_METHOD` charges it with zero customer action, still inside the same retry budget. That's the FlyCode move, gated instead of vibes."

### 2:30-3:15 — Measured proof (one batch, two agents, live)

```powershell
instate demo --entities 10
```

**Narrate the three regions as they run** (don't talk over every stage — let two entities breathe, then speak):
> "Top is where we are in the batch. Middle is one entity walking the pipeline — intake, diagnose, gate-1, one model call, gate-2, execute. Green allowed, red denied, yellow human, dim skipped. Bottom is both agents scoring live — the gap widens in front of you."

**When the still table lands, read it out loud:**
> "Same seed, fair by construction. Net recovered, attempted-recovery-rate — recovered over *attempted*, not over every failure including ones we never touched — duplicate retries avoided, violations, escalated entities on both sides, share resolved with zero LLM calls, cost per decision, chain verification. The baseline retried past the ceiling; we escalated."

**Timing (measured, not assumed):** the 6-entity batch costs ~2s of compute;
animated at `--pace 0.45` it runs ~20s. A 30-entity batch stays inside ~80s —
no fast-forward cut needed. If you add entities and it runs long, show the first
6–8 at full pace, then cut visibly to the final scoreboard — never silently.

**Say the token line:**
> "Context is bounded: digest + top-3 precedents, ~1k tokens flat whether the entity has 3 events or 300 — measured, not asserted. The stateless baseline re-derives the full history every time."

Verify one chain tersely on camera:

```powershell
instate verify sub_004
```

> "N events, N hashes checked — intact, no breaks." (Read the counts off the
> screen — `sub_004` carries 8 on a fresh seed, but say what it says.)

### 3:15-3:45 — Kill mid-action, reconcile (failure beat, same visual language)

Rehearse, don't ad-lib. Before recording, leave one intent dangling (kill the
worker between `ActionIntended` and the gateway call, or seed one via a dry run).
On camera:

```powershell
instate worker resume
```

**Point to, in order:** dim `○ sub_4471 querying gateway by idempotency_key …`,
then green `● ActionCompleted written (lookup)` — same green/dim contract as the
pipeline tracker. A `yellow` line means the gateway is still unreachable: the
intent stands and is safe to re-run, never doubled.

**Say:**
> "The intent is committed before anything touches the gateway. Kill the process
> mid-action, restart, and the worker reconciles by idempotency key — found it,
> receipt written, money accounted for exactly once."

### 3:45-4:10 — Audit + replay (product moment)

```powershell
instate rebuild
instate replay --set retry_spacing_24h=0
```

**Say (read the numbers off the screen — on a fresh DB expect ~7 verdict
changes, ~−₹2,000 projected, ~3 doomed attempts avoided):**
> "`instate rebuild` drops L1, replays L0, diffs — zero drift. `instate replay` is the product moment: re-decide history at original decision time. Pausing all retries for 24 hours would have blocked this much recovery and saved these doomed attempts — a question every collections team has and none can answer. With snapshots it's sublinear."

> Do NOT use `--set retry_ceiling_7d=2` here — on this history nothing sits at
> exactly 2/3, so it replays 8 decisions with 0 changes and the beat dies.
> The spacing-zero override binds on every retry decision by construction.
> And `replay` is one-shot per DB (it bumps the policy version) — if you
> rehearsed it on the keeper DB, start over with a fresh `instate.db`.

Open console:
```powershell
instate serve console   # → http://127.0.0.1:8002/
```

**Say while scrolling:**
> "Read-only memory wall — see what your agent remembers. Every entity, every gate."

### 4:10-5:00 — Prod (close)

> `instate prod` is a readiness checklist, not a deploy — say that if asked.
> Deploying is below; the video closes on readiness, not on infra.

```powershell
instate prod
```

**Read the 12 rows:**

> "L1 snapshots, cold archive with chain anchors, Vault rotation, Fernet at rest, RLS, standalone verifier, FailoverReasoner, HITL queue that writes HumanResolved back to L0 so L3 learns, canary rollout validated by replay, k-threshold privacy. Demo is lean, prod is built. `pipx install instate && instate init` — the Meniscus-grade wizard is `instate init`."

**Final line, look at camera:**
> "Instate is the product. The recovery agent is the first proof the layer works — not the only thing it's for."

---

## Pre-flight checklist (recording, not code)

- [ ] Terminal ≥140×36, theme checked at recording resolution: green/red/yellow
      all legible, dim-gray pending states still visible (some dark themes wash
      them out — test, don't assume).
- [ ] One real Razorpay test-mode event captured separately, still in the cut
      before the batch segment — it answers "is this live".
- [ ] Full 5-minute dry run, timed, stopwatch in hand — before the keeper take.
- [ ] Cost/decision caveat memorized: "input-token cost only — output is a fixed
      ~60 tokens, so the delta is the story."
- [ ] Dangling intent pre-staged for the `worker resume` beat.
- [ ] `instate demo --pace 0` verified as fallback if the terminal misbehaves.

## Deploy (not on camera — for the judge who asks "how do I run this")

`instate prod` proves readiness; running it is three processes against one DB:

```powershell
# 1 · database — SQLite file for the demo, Postgres 16 + pgvector for prod:
$env:INSTATE_DATABASE_URL="sqlite+aiosqlite:///instate.db"          # demo
# $env:INSTATE_DATABASE_URL="postgresql+asyncpg://user:pass@host/db" # prod (+ RLS DDL from instate.core.tenant)

# 2 · webhook receiver (ledger-first ingress):
instate serve webhook --host 127.0.0.1 --port 8000
# secret via INSTATE_WEBHOOK_SECRET (vault-backed); test default is whsec_test_secret

# 3 · worker (tick loop: diagnose → gate → reason → execute → reconcile):
instate worker resume   # boot reconciliation, once per deploy/restart
instate worker tick     # one step; run on a schedule (cron/systemd timer), not as a daemon
```

Secrets live in the vault (`instate init` writes `~/.instate/config.json`,
mode 0600): `RAZORPAY_KEY_ID/SECRET` (test-mode only), `INSTATE_WEBHOOK_SECRET`,
`INSTATE_ENCRYPTION_KEY` (Fernet at rest). No keys on the command line, no keys
on camera. The console (`instate serve console`) is read-only and safe to
expose; the webhook port is the only ingress Razorpay needs.

* **Thesis:** Authority decreases as uncertainty increases.
* **L3 never gates:** precedent returning `[]` is a degradation, not an outage.
* **Per-entity chain:** no global write contention (`instate verify <entity>` is self-contained).
* **TOCTOU closed:** per-entity lock from Gate-1 → intent-write (row lock on PG, app lock everywhere — PG path proven by test when `INSTATE_TEST_PG_DSN` is set).
* **Fair baseline:** same model, same data, no memory — a rigged comparison discounts everything.
* **Attempted-rate:** recovered ÷ attempted, never ÷ all failures. Say the denominator.
* **Mode honesty:** stand-in by default, `--live` only with test-mode keys, never "live money" otherwise.

## Appendix — what I built (the 60-second system tour)

Say this if a judge asks "so what is the system", or over the first seconds of
the demo while entities walk. Memorize the two halves: the flow, then the core.

**The flow — six stages, one rule (gates before and after the model):**

> "Every failure walks six stages. **Intake** verifies and dedupes — the same
> webhook twice is one event. **Diagnose** maps the Razorpay code to a root
> cause from a table, not from the model. **Gate-1** checks stopping rules —
> retry ceiling, spacing, contact caps — before a single token is spent.
> **Reason** is the only model call, and it gets a bounded digest plus three
> precedents, never the raw history. **Gate-2** re-checks the proposal against
> the concrete timing — contact caps, DNC. **Execute** commits the intent first,
> then touches the gateway with an idempotency key, so a crash mid-action
> reconciles instead of double-charging. The model proposes; it never decides."

**The memory core — three layers, and why it's not a vector DB:**

> "L0 is the ledger: every event hash-chained per entity, append-only,
> bi-temporal — late events append, nothing rewrites. L1 is derived state,
> rebuilt from L0 any time and diffed for drift. L3 is precedent: past cases
> with outcomes, so 'has a case like this resolved before' is a lookup, not a
> vibe. Policy itself is versioned data, which is why replay works — re-decide
> history at original decision time under any counterfactual ceiling."

**HydraDB positioning (if they name it — they will):**

> "HydraDB is graph-native recall — relationships, temporal states, audit
> chains of evidence for finance. It answers *what do we know*. Instate is the
> layer above it: authority. Which attempt is legal right now, who approved it,
> what stops the next one. Their finance page lists the same wounds we treat —
> missing audit trails, flattened temporal states, stateless retrieval — we
> just treat them at decision time instead of retrieval time: hash-chained
> ledger instead of versioned documents, gates instead of reranking, replay
> instead of point-in-time search. If you already have HydraDB, Instate is what
> stops its agent from retrying past the ceiling. Different layer, same war."

## What NOT to show
* No merchant web app (zero rubric value).
* No live Razorpay keys on screen — test keys only (`rzp_test_xxx`, `whsec_test_secret`).
* No `payment.captured` — only `payment.failed` / `subscription.charged.failed`.
* No `demo --ab` numbers as anything but scripted lift (the table says so itself).

## Backup — if live webhook flakes (it won't)

You already seeded `payload.json` — the whole flow is replayable offline:
`instate timeline` / `verify` / `explain` work on the seeded ledger without any network.
`instate demo --pace 0` runs the full comparison with zero animation if the terminal misbehaves.

---

## File references for Q&A

* Flow: `docs/architecture.md` §6, `instate/agent/decide.py` (`process_failure`)
* Gates: `instate/core/gate.py` (`evaluate` / `check_proposal`), hard-decline in `projection.has_new_method_since_last_failure`
* Outbox: `instate/agent/execute.py` (idempotency key = `source_event_id`)
* Backup route: `instate/adapters/razorpay.py` (`RETRY_BACKUP_METHOD` branch), `instate/agent/execute.py` (`via: backup`)
* Live demo: `instate/surfaces/live_demo.py` (layout, stage renderer, scoreboard, delta table)
* Chain: `instate/core/ledger.py` (`compute_event_hash` / `verify_chain`) + `instate/verify/standalone.py`
* Prod gaps: `docs/architecture.md:520` (all ✅ via `instate prod`)
