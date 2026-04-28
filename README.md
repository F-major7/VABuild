# PizzaTime — Autonomous Pizza Ordering Agent

An AI agent that calls a pizza restaurant, navigates the phone menu automatically, and places a delivery order through a natural conversation with the employee.

**Stack**: Python 3.11 | FastAPI | Twilio | Deepgram | OpenAI GPT-4o | ElevenLabs | Pipecat 1.0 | Prometheus

---

## Executive Summary

PizzaTime is a production voice agent that handles the full arc of a pizza order call — from dialing the restaurant to receiving an order confirmation number. It operates in two phases: an IVR phase where GPT-4o navigates the automated phone menu using function calling, and a human phase where a Pipecat pipeline carries a real-time conversation with the restaurant employee. Every call is logged as a structured JSON file with a full event timeline. The system exposes a REST API, Prometheus metrics, and runs in Docker.

---

## Problem

Placing a food delivery order by phone involves two hard sub-problems:

1. **IVR navigation** — phone menus vary across restaurants and change over time. A hardcoded keyword matcher fails in production.
2. **Human conversation** — once an employee picks up, the agent must handle substitutions, pricing, budget constraints, and confirmation numbers — not just read from a script.

Both require real judgment, not pattern matching.

---

## Architecture

```
POST /call
    │
    ▼
TwilioClient — dials restaurant, returns call_sid
    │
    ▼  (Twilio connects, opens bidirectional audio stream at mulaw 8kHz)
WS /media-stream
    │
    ├────────────────────────────────────────────┐
    │                IVR PHASE                   │
    │                                            │
    │  Restaurant audio (mulaw 8kHz)             │
    │          │                                 │
    │  DeepgramClient  ← raw WebSocket           │
    │          │  transcript                     │
    │  LlmIvrDriver (GPT-4o + 3 tools)          │
    │          │                                 │
    │  ┌───────┴────────┬───────────────┐        │
    │  press_digit   speak_value    enter_hold   │
    │  (DTMF inject) (ElevenLabs TTS)  (signal)  │
    │                                            │
    └────────────────────────────────────────────┘
                       │ hold detected
                       ▼
    ├────────────────────────────────────────────┐
    │              HUMAN PHASE                   │
    │                                            │
    │  FastAPIWebsocketTransport (input)         │
    │          │                                 │
    │  DeepgramSTTService                        │
    │          │  transcript                     │
    │  LLMContextAggregatorPair                  │
    │          │                                 │
    │  OpenAILLMService (GPT-4o)                │
    │          │  text / tool call               │
    │  ElevenLabsTTSService (pcm_16000)          │
    │          │  audio frames                   │
    │  TwilioFrameSerializer → mulaw 8kHz        │
    │          │                                 │
    │  FastAPIWebsocketTransport (output)        │
    │                                            │
    │  └── complete_order() ─────────────────── ┤
    │                                            │
    └────────────────────────────────────────────┘
                       │
                       ▼
        logs/YYYY-MM-DD_HHMM_outcome_id.json
```

---

## How It Works

### IVR Phase

Restaurant audio flows from Twilio → Deepgram (raw WebSocket, mulaw 8kHz) → transcript. Each transcript goes to GPT-4o with the agent's current state and three function tools:

| Tool | When called | What happens |
|------|-------------|--------------|
| `press_digit(digit)` | Menu prompt to press a number | DTMF injected into the call |
| `speak_value(text)` | Prompt asking for name, zip, confirmation | ElevenLabs TTS audio injected into the call |
| `enter_hold()` | Explicit hold language heard | Signals handoff to human phase |

The LLM receives its current position in the flow (WELCOME → NAME → PHONE → ZIP → CONFIRM → HOLD) with every transcript, so it knows what to expect at each step.

### Human Phase

Once on hold, a Pipecat pipeline takes over the WebSocket:

```
Restaurant audio → Deepgram STT → GPT-4o → ElevenLabs TTS → Twilio
```

GPT-4o follows a system prompt encoding the full order — pizza, sides, drink, budget, substitution rules, and special instructions. It orders one item at a time, asks for exact prices if given ranges, and must call `complete_order()` before ending the call.

### Call Logs

Every call writes a JSON file to `logs/` regardless of outcome:

```
logs/2026-04-28_1921_completed_A7X3K1.json
     ├── summary: outcome, customer, prices, delivery time, order number
     └── events:  full timestamped event timeline
```

---

## Problems Encountered and How We Fixed Them

### 1. IVR called `enter_hold()` on a menu option
**Symptom**: Agent skipped the entire IVR and jumped straight to human phase on the first prompt.

**Root cause**: The system prompt described `enter_hold()` as "connecting to a team member" — which placing an order also involves. The LLM couldn't distinguish "press 1 to place an order" from a hold/transfer prompt.

**Fix**: Rewrote the system prompt to be state-aware. The LLM now receives its current state with every transcript. `enter_hold()` is explicitly restricted to hold language ("please hold", "one moment", "stay on the line"). Menu options can never trigger it.

---

### 2. Human phase TTS generating audio but silent on the phone
**Symptom**: Pipecat logs confirmed audio was being generated, but nothing was heard on the call.

**Root cause**: `ElevenLabsTTSService` was set to `output_format="ulaw_8000"`, so ElevenLabs returned pre-encoded mulaw audio. `TwilioFrameSerializer` then encoded it to mulaw a second time. Double mulaw encoding produces silence.

**Fix**: Changed to `output_format="pcm_16000"`. ElevenLabs returns PCM, the serializer encodes to mulaw once.

---

### 3. IVR TTS silently crashing — no error in logs
**Symptom**: Call log showed `ivr_transition` with `action_type: tts` but no `tts_sent` event after it. Agent stopped responding and the call dropped.

**Root cause**: `synthesize_mulaw_8khz()` was raising an unhandled exception. Without a try/except in `_handle_ivr_prompt`, the exception propagated to the asyncio task and terminated it silently. The Twilio WebSocket stayed open, so the call appeared alive but the agent had stopped.

**Fix**: Wrapped TTS synthesis and WebSocket send in try/except. Errors are now logged with the full exception message. The task no longer dies on a single TTS failure.

---

### 4. ElevenLabs 401 — misdiagnosed as invalid API key
**Symptom**: After adding error logging, every TTS call failed with `401 Unauthorized`.

**Misdiagnosis**: Tested the wrong ElevenLabs endpoint (`/v1/user`) which also returned 401 for a different reason (missing `user_read` scope). This led to incorrect conclusions about the key and voice ID.

**Root cause**: The ElevenLabs account had exhausted its free tier (10,000 characters/month). ElevenLabs returns HTTP `401` for quota exceeded — the same status code as an invalid key.

**Fix**: Fetched the full response body instead of reading only the status code. The body contained `"status": "quota_exceeded"`. Solution: add credits to the account.

---

### 5. `response.content` accessed after httpx connection closed
**Symptom**: Potential for empty audio bytes returned from ElevenLabs on the IVR TTS path.

**Root cause**: `return response.content` was placed outside the `async with httpx.AsyncClient()` block (moved there to wrap timing metrics). For streaming HTTP responses (ElevenLabs `/stream` endpoint), the body buffer may not be guaranteed after the connection closes.

**Fix**: Read body inside the block (`audio = response.content`), return after.

---

## Performance

| Stage | Latency |
|-------|---------|
| IVR LLM decision (GPT-4o) | ~1–3s per transcript |
| ElevenLabs TTS synthesis | ~300ms |
| Full IVR navigation | ~30–60s |
| Human phase — per turn (STT → LLM → TTS) | ~2–4s |

---

## Metrics

Prometheus at `GET /metrics`:

| Metric | Description |
|--------|-------------|
| `pizza_agent_calls_total` | Call count by outcome |
| `pizza_agent_ivr_duration_seconds` | Time in IVR phase |
| `pizza_agent_human_phase_duration_seconds` | Time in human conversation |
| `pizza_agent_llm_response_latency_seconds` | LLM response time |
| `pizza_agent_tts_latency_seconds` | TTS synthesis time |
| `pizza_agent_ivr_retries_total` | IVR retries by state |

---

## Setup

```bash
git clone <repo>
cd PizzaTime
cp voice_agent/.env.example voice_agent/.env
# fill in credentials
docker-compose up
```

---

## Usage

```bash
# Place a call
curl -X POST http://localhost:8000/call \
  -H "Content-Type: application/json" \
  -d '{
    "customer_name": "Jordan Mitchell",
    "phone_number": "5551234567",
    "delivery_address": "4821 Elm Street, Apt 3B, Austin TX 78745",
    "pizza": {
      "size": "large",
      "crust": "thin",
      "toppings": ["pepperoni", "mushrooms"],
      "acceptable_topping_subs": ["sausage", "olives"],
      "no_go_toppings": ["anchovies"]
    },
    "side": { "first_choice": "breadsticks", "backup_options": ["garlic bread"], "if_all_unavailable": "skip" },
    "drink": { "first_choice": "Coke", "alternatives": ["Pepsi"], "skip_if_over_budget": true },
    "budget_max": 45.00,
    "special_instructions": "Ring doorbell, dont knock, baby sleeping"
  }'

# Poll for status
curl http://localhost:8000/call/<call_sid>

# Metrics
curl http://localhost:8000/metrics
```

---

## Caveats

**Twilio trial accounts** can only call phone numbers that have been verified in the Twilio console. Any unverified number fails. A paid Twilio account removes this restriction.

**ElevenLabs free tier** provides 10,000 characters per month. Active development and testing exhausts this quickly — we hit the limit during this build. The API returns HTTP 401 for quota exceeded, the same status code as an invalid key, which caused a debugging detour. Sustained use requires a paid plan or additional credits.

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| API server | FastAPI + uvicorn |
| Telephony | Twilio (outbound calls, bidirectional media streams, DTMF) |
| STT — IVR phase | Deepgram nova-2-phonecall (raw WebSocket, mulaw 8kHz) |
| STT — Human phase | Deepgram via Pipecat `DeepgramSTTService` |
| LLM | OpenAI GPT-4o |
| TTS — IVR phase | ElevenLabs HTTP API (ulaw_8000, direct WebSocket injection) |
| TTS — Human phase | ElevenLabs via Pipecat `ElevenLabsTTSService` (pcm_16000) |
| Pipeline | Pipecat 1.0 |
| Metrics | Prometheus |
| Runtime | Python 3.11, Docker |
