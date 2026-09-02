# Demo Video — Instate (Track 03: AI Revenue Recovery)
**Length: 5:00 — start at payment failure, end at `instate prod`**

> **Goal:** Prove the bar — *measured money recovered, compliant escalation, stopping rules, audit trail* — with a terminal that makes the memory tangible. No web app, no slides as proof.

---

## Pre-flight (2 min before record)

```powershell
cd "Z:\Projects\Better Projects\FiMem"
Remove-Item instate.db -ErrorAction SilentlyContinue
$env:INSTATE_DATABASE_URL="sqlite+aiosqlite:///instate.db"
# clean terminal: 120x32, font 14 (Cascadia Code), hide taskbar
# OBS: capture terminal only, 1080p, 30fps, mic test
# Chrome ready: http://127.0.0.1:8002/ (closed, will open at 3:30)
```

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
> "Ten entities, twelve with checkouts, precedent cases built. Every chain verified — tamper-evident. Now a real Razorpay test-mode failure fires."

```powershell
curl -X POST http://127.0.0.1:8000/webhook -H "X-Razorpay-Signature: demo" --data @payload.json
instate timeline pay_9A1B
```

**Point to:** red `PaymentFailed(cause=insufficient_funds)` → blue `RetryScheduled`, panel `status RETRY_SCHEDULED, retries·7d 0`, footer `✓ chain verified — 2 events, zero breaks`.

**Say:**
> "Ledger-first: verify HMAC → dedupe → append → 200. The webhook returns before any gate or model runs. Late events append, they never rewrite a decision — bi-temporal."

### 1:00-1:45 — The decision, opened

Pick `d1` from timeline's `decision` column:

```powershell
instate explain 1
```

**Point to:** gate-1 table `retry_ceiling_7d 0/3 ALLOW` green, gate-2 `contact_freq 0/2 ALLOW`, proposal `{action: RETRY_SCHEDULED, timing: T_PLUS_48H, confidence: 0.85}`.

**Say:**
> "Gate-1 checked the ceiling before the model — zero tokens. The model proposed a payday-aligned retry, Gate-2 checked the concrete timing against contact caps and DNC. Nothing the model emits reaches Razorpay unverified. That's the answer to 'what if it hallucinates' — it cannot matter."

### 1:45-2:30 — Hard-decline + at-ceiling (thesis, 0 LLM)

```powershell
instate timeline sub_004
instate explain 2
```

`sub_004` is at-ceiling (3 retries in 7d). `explain 2` → `retry_ceiling_7d 3/3 DENY` red → `executed ESCALATE_HUMAN`.

Switch:

```powershell
curl -X POST http://127.0.0.1:8000/webhook -H "X-Razorpay-Signature: demo" --data '{"event":"payment.failed","id":"evt_card_001","payload":{"payment":{"id":"pay_dead","error_reason":"CARD_EXPIRED"}}}'
instate timeline pay_dead
```

**Say:**
> "A card_expired retry without a new method is denied until `PaymentMethodChanged` lands — hard declines are payment-method situations, not retry situations. Scheduled retries stay queued until the method changes. That's the Stripe lesson, mechanically."

### 2:30-3:15 — Measured proof (one batch, two agents)

```powershell
instate demo --entities 10
```

**Read the table out loud:**

> "Same seed, same scripted model, same realistic gateway — fair by construction. Net recovered ₹1,996 vs ₹499. Retry-ceiling violations 0 vs 1. Seventeen percent resolved with zero LLM calls — gates fired before the model. Hash chain verified. The baseline retried a 4th time past the ceiling; we escalated."

**Say the token line:**
> "Context is bounded: digest + top-3 precedents, ~1k tokens flat whether the entity has 3 events or 300. The stateless baseline re-derives the full history — 5 to 15k tokens."

### 3:15-4:10 — Audit + replay (product moment)

```powershell
instate verify
instate rebuild
instate replay --set retry_ceiling_7d=2
```

**Say:**
> "`instate rebuild` drops L1, replays L0, diffs — zero drift. `instate replay` is the product moment: re-decide history at original decision time. Tightening the ceiling from 3 to 2 would have cost ₹X and avoided Y doomed attempts — a question every collections team has and none can answer. With snapshots it's sublinear."

Open console:
```powershell
instate serve console   # → http://127.0.0.1:8002/
```

**Say while scrolling:**
> "Read-only memory wall — see what your agent remembers. Every entity, every gate."

### 4:10-5:00 — Prod (close)

```powershell
instate prod
```

**Read the 12 rows:**

> "L1 snapshots, cold archive with chain anchors, Vault rotation, Fernet at rest, RLS, standalone verifier, FailoverReasoner, HITL queue that writes HumanResolved back to L0 so L3 learns, canary rollout validated by replay, k-threshold privacy. Demo is lean, prod is built. `pipx install instate && instate init` — the Meniscus-grade wizard is `instate init`."

**Final line, look at camera:**
> "Instate is the product. The recovery agent is the first proof the layer works — not the only thing it's for."

---

## Narration cheat-sheet (if you blank)

* **Thesis:** Authority decreases as uncertainty increases.
* **L3 never gates:** precedent returning `[]` is a degradation, not an outage.
* **Per-entity chain:** no global write contention (`instate verify <entity>` is self-contained).
* **TOCTOU closed:** `SELECT … FOR UPDATE` from Gate-1 → intent-write.
* **Fair baseline:** same model, same data, no memory — a rigged comparison discounts everything.

## What NOT to show

* No merchant web app (zero rubric value).
* No live Razorpay keys on screen — test keys only (`rzp_test_xxx`, `whsec_test_secret`).
* No `payment.captured` — only `payment.failed` / `subscription.charged.failed`.

## Backup — if live webhook flakes (it won't)

You already seeded `payload.json` — the whole flow is replayable offline:
`instate timeline` / `verify` / `explain` work on the seeded ledger without any network.

---

## File references for Q&A

* Flow: `docs/architecture.md:230`, `instate/agent/decide.py:28`
* Gates: `instate/core/gate.py:230`, `instate/core/projection.py:300`, `instate/core/projection.py:360` (hard-decline)
* Outbox: `instate/agent/execute.py:62` (idempotency = source_event_id)
* Chain: `instate/core/ledger.py:44` + `instate/verify/standalone.py:12`
* Prod gaps: `docs/architecture.md:520` (all ✅ via `instate prod`)
