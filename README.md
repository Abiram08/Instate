# Instate — a revenue-recovery agent I built

I built an **agent that chases failed payments and gets the money back**.
A payment dies on Razorpay, my agent's webhook catches it, figures out why it
failed, checks whether it's even allowed to try again, picks the smartest next
move, executes it — and remembers all of it, hash-chained, so I can prove a
year later why every rupee moved. That's the whole project. Everything below
is real, runnable, and tested (376 tests).

## How it flows — one picture

It starts with one real event: **a payment fails**, and the webhook it fires
is the only input the system needs.

```
 ① PAYMENT FAILS (test card, test mode)
        │  payment.failed
        ▼
 ┌──────────────────────────────┐
 │ receiver — ledger-first      │
 │ verify sig → dedupe →        │
 │ append → 200 ok              │
 └──────────────┬───────────────┘
                │ tick drains
                ▼
 ┌──────────────────────────────┐
 │ recovery agent — gated       │
 │ diagnose → gate-1 → reason   │
 │ → gate-2 → execute           │
 └──────┬───────────────┬───────┘
        │ reads         │ writes
        ▼               ▼
 ┌──────────────────────────────┐
 │ memory: ledger → projection  │
 │ policy (the gate) ·          │
 │ precedent (advisory) ·       │
 │ decisions                    │
 └──────────────┬───────────────┘
                │ only approved actions
                ▼
 ┌──────────────────────────────┐
 │ Razorpay API (idempotent)    │
 │ intent → call → commit →     │
 │ reconcile                    │
 └──────────────────────────────┘
```

Every box knows what it may do:

| Piece | Guarantee | May it gate an action? |
|---|---|---|
| ledger | immutable, hash-chained | it IS the truth |
| projection | exact, derived | yes |
| policy | exact, versioned | it is the gate |
| precedent | probabilistic | **never — advisory only** |

## Watch it work (90 seconds)

```powershell
pip install -e ".[dev]"   # once; Python >= 3.12
python run.py             # 12 beats, self-checked — ALL 12 BEATS PASS
```

That *is* the agent running: seed → verify → live webhook server → signed
failure posted → `captured #N head=…` → repost comes back `duplicate` with the
identical head → tick decides → timeline → explain → twin-agent comparison →
reconcile → rebuild (zero drift) → replay moves numbers → prod green. Scratch
DB, no keys, no network, ₹0.

Then drive it yourself:

```powershell
$env:INSTATE_DATABASE_URL="sqlite+aiosqlite:///instate.db"
instate seed --entities 10
instate verify                    # 12 chains, zero breaks
instate serve webhook             # second terminal
# sign + post demo/payload.json, then:
instate worker tick               # webhook appends; tick decides
instate timeline pay_9A1B         # failed → diagnosed → scheduled
instate explain 1                 # gates, proposal, inputs_hash
instate demo --entities 10        # one batch, two agents, watch the gap
instate worker resume             # reconcile by idempotency key
instate rebuild                   # zero drift
instate replay --set retry_spacing_24h=0
instate prod                      # 12 checks green
```

## What it proves (measured, not asserted)

Same failures, same model, same gateway — left twin has no memory, right is
Instate. The left one retried past the ceiling. Ours escalated:

```
net money recovered            ₹13,492        ₹14,490
attempted recovery rate        62% (5/8)      88% (7/8)
retry-ceiling violations       1              0
% decisions with zero LLM      0%             17%
hash chain verified            yes            yes
```

## How I broke it (and fixed it)

Attacked on purpose — 20 duplicate webhooks at once, 4 workers at once, a
gateway exploding mid-charge — four real bugs died in `tests/test_adversarial.py`:
one event per delivery, one decision per failure, one charge per intent, every
rerun idempotent. Older kills from the build: false tamper alarms (walked chains wrong),
a shadowing bug that silenced do-not-call, a webhook that couldn't parse real
Razorpay payloads.

```
python -m pytest -q        # 376 passed, 1 skipped
ruff check instate tests   # clean
```

## Where things live

```
instate/agent/     decide · execute · reconcile · diagnose   (the agent)
instate/core/      ledger · projection · policy · gate       (the memory)
instate/adapters/  razorpay test-mode · gemini · failover
instate/replay/    memory-less twin · comparison · replay
instate/surfaces/  webhook · cli · live demo · console
demo/              sample failures
```

---

*I built an agent that gets money back — and remembers why every rupee moved.*
