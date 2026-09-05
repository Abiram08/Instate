# SHOOT.md — the demo video, start to finish

**One file. Follow top to bottom, don't improvise.** Total: 5:00.
Full narration lives in `docs/demo-script.md`; this file is the execution —
exact commands, expected outputs, camera notes, timing. If anything on screen
differs from "expect", stop and fix it before continuing.

`python run.py --fast` rehearses all of this headless first. Shoot only after
it prints `ALL 12 BEATS PASS`.

---

## 0 · Setup (day before, 10 min)

```powershell
cd "Z:\Projects\Better Projects\FiMem"
pip install -e ".[dev]"          # Python >= 3.12
instate --help                   # must list commands, not traceback
```

Terminal: ≥140×36, font 14 (Cascadia Code). OBS: terminal only, 1080p, 30fps,
mic tested. Chrome ready at `http://127.0.0.1:8002/` (closed until 3:45).
Theme check at recording resolution: green/red/yellow legible, dim-gray
pending states still visible.

Fresh DB for the keeper (replay is one-shot per DB; rehearsals use scratch):

```powershell
$env:INSTATE_DATABASE_URL="sqlite+aiosqlite:///instate.db"
$env:INSTATE_ENCRYPTION_KEY = (python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
Remove-Item instate.db -ErrorAction SilentlyContinue
```

Payload files (save once, never touch again — the HMAC is over raw bytes).
Ship with the repo as `demo/payload.json` and `demo/card.json`; copy to temp
for signing so the originals stay pristine:

```powershell
Copy-Item demo/payload.json, demo/card.json C:/Users/Nella/AppData/Local/Temp/opencode/
```

`payload.json`:

`payload.json`:
```json
{"event":"payment.failed","id":"evt_test_001","payload":{"payment":{"id":"pay_9A1B","error_reason":"insufficient_funds","amount":49900}}}
```

`card.json`:
```json
{"event":"payment.failed","id":"evt_card_001","payload":{"payment":{"id":"pay_dead","error_reason":"CARD_EXPIRED"}}}
```

Dangling intent pre-staged for the 3:15 beat (kill a worker between
`ActionIntended` and the gateway call during rehearsal, or seed one — verify
with `instate worker resume` on scratch that it prints a `●` line, then
re-create it on the keeper DB).

---

## 1 · 0:00–0:25 — Hook (no typing)

```powershell
instate
```

Say (memorized): *"Revenue recovery agents are built as single-shot pipelines.
Snapshot in, decision out. They break at the second attempt. Instate is the
memory layer that answers three questions every human analyst asks: what have
we tried, have we hit our limit, has a case like this resolved before."*

---

## 2 · 0:25–1:00 — Seed + the failure (Terminal 2 serves the webhook)

Terminal 2 (start before recording):
```powershell
instate serve webhook
```
Expect: `webhook receiver → http://127.0.0.1:8000/webhook (HMAC verified)`.

Recording terminal:
```powershell
instate seed --entities 10
instate verify
```
Expect: `memory seeded` table, then `✓ 12 entities verified, zero breaks`.

```powershell
$secret = "whsec_test_secret"
$bytes = [IO.File]::ReadAllBytes("$pwd/payload.json")
$hmac = [System.Security.Cryptography.HMACSHA256]::new([Text.Encoding]::UTF8.GetBytes($secret))
$sig = ($hmac.ComputeHash($bytes) | ForEach-Object { $_.ToString("x2") }) -join ""
curl -X POST http://127.0.0.1:8000/webhook -H "X-Razorpay-Signature: $sig" --data "@payload.json"
instate worker tick
instate timeline pay_9A1B
```
Expect: `captured #N head=…`, `1/1 failures decided`, then red
`PaymentFailed` → `FailureDiagnosed` → blue `RetryScheduled`.

Say: *"Ledger-first: verify HMAC, dedupe, append, 200 — before any gate or
model runs. The tick decides afterwards."*

---

## 3 · 1:00–1:45 — Open the decision

```powershell
instate explain 1
```
Expect: `retry_ceiling_7d 0/3 → ALLOW`, `RETRY_SCHEDULED · T_PLUS_48H ·
confidence 0.90`, `executed: RETRY_SCHEDULED`, `inputs_hash`.

Say: *"Gate-1 checked the ceiling before the model — zero tokens. Gate-2
checked the timing against contact caps. Nothing the model emits reaches
Razorpay unverified."*

---

## 4 · 1:45–2:30 — Hard decline

```powershell
$bytes = [IO.File]::ReadAllBytes("$pwd/card.json")
$hmac = [System.Security.Cryptography.HMACSHA256]::new([Text.Encoding]::UTF8.GetBytes($secret))
$sig = ($hmac.ComputeHash($bytes) | ForEach-Object { $_.ToString("x2") }) -join ""
curl -X POST http://127.0.0.1:8000/webhook -H "X-Razorpay-Signature: $sig" --data "@card.json"
instate worker tick
instate timeline pay_dead
```
Expect: `PaymentFailed (CARD_EXPIRED)` → link sent, contact made — a payment
link, not a doomed retry.

Say: *"A dead card is a payment-method situation, not a retry situation."*

---

## 5 · 2:30–3:15 — Measured proof (~20s animated + table read)

```powershell
instate demo --entities 10
```
Narrate the three regions once, then let two entities breathe in silence.
When the still table lands, read it: net recovered, attempted-rate (say the
denominator), duplicates avoided, violations, escalated both sides, zero-LLM
share, cost/decision, chain verification.

```powershell
instate verify sub_004
```
Read the counts off the screen: *"N events, N hashes — intact, no breaks."*

---

## 6 · 3:15–3:45 — Kill mid-action, reconcile

```powershell
instate worker resume
```
Point: dim `○ … querying gateway by idempotency_key`, then green
`● ActionCompleted written`. Say: *"Intent committed before the gateway call.
Kill the process, restart, reconcile by idempotency key — exactly once."*

---

## 7 · 3:45–4:10 — Audit + replay (product moment)

```powershell
instate rebuild
instate replay --set retry_spacing_24h=0
```
Expect: `zero drift`, then verdict changes with −₹ projected (fresh DB: ~6–7
changes, ~−₹2,000). Say: *"Pausing retries for 24 hours would have blocked
this much and saved these doomed attempts — re-decided at original decision
time. No collections team can answer that today."*

```powershell
instate serve console   # → http://127.0.0.1:8002/ (Terminal 2 is busy — use a third, or stop the webhook first)
```
Scroll once. Say: *"Read-only memory wall. Every entity, every gate."*

> Terminal conflict: the webhook server owns Terminal 2. Either stop it with
> Ctrl+C after beat 4 (webhooks are done) and reuse the terminal, or open a
> third. Decide in rehearsal, not on camera.

---

## 8 · 4:10–5:00 — Prod close

```powershell
instate prod
```
Read 4 rows if behind (snapshots, RLS, HITL-learns, canary-by-replay), all 12
if ahead. Final line, camera: *"Instate is the product. The recovery agent is
the first proof the layer works — not the only thing it's for."*

---

## Appendix A · Razorpay relevance (what makes this a Razorpay demo, not generic)

Checked against `razorpay.com/docs/webhooks/payments` (Sept 2026):

- **Real payload shape in, not just demo shape.** Our extractor accepts the
  actual dashboard delivery: `payload.payment.entity.{id, amount, status,
  method, order_id, error_code, error_description, error_source, error_step,
  error_reason}` plus `account_id`/`created_at`. A real `payment.failed`
  from test mode lands on our webhook today and captures with method,
  order, and error context on the ledger (pinned by test).
- **Real failure vocabulary.** `error_code` (`BAD_REQUEST_ERROR`),
  `error_description`, `error_source` (`bank`), `error_step`
  (`payment_authorization`) are first-class ledger fields, not dropped.
  Unknown reasons route to `UNKNOWN → escalate to human` — the only honest
  default for money.
- **Payment Links are the real API.** `SEND_PAYMENT_LINK` / recovery links
  post to `/payment_links` with the entity as `reference_id` — the same
  endpoint a merchant integration uses. Backup-method retries hit the real
  retry path with the stored instrument flagged.
- **Test-mode path is documented, not hand-waved.** Dashboard → Test Mode →
  Settings → API Keys (`rzp_test_…`) → webhooks signed with your webhook
  secret → `instate demo --live` runs both arms against test mode. Declines
  on demand: `failure@razorpay` UPI, test cards from
  `razorpay.com/docs/payments/payments/test-card-details`. Judge Q&A only —
  never the keeper take (numbers vary).

## Appendix B · If it goes wrong on camera

| Moment | Failure | Recovery line |
|---|---|---|
| Webhook | `401` | "Signature is over raw bytes — let me re-sign." (You rehearsed this; it won't happen.) |
| `explain 1` | wrong entity | Read the id from `timeline` first — never guess ids on camera. |
| `demo` | terminal glitch | `instate demo --entities 10 --pace 0` — same numbers, no animation. Say so out loud. |
| `replay` | `0 verdict changes` | You rehearsed on the keeper DB. Fresh `instate.db`, start over — say "fresh ledger, honest replay." |
| Running long | — | Trim words, never commands. Cut prod to 4 rows. |
