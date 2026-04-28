from __future__ import annotations

from prometheus_client import Counter, Histogram

calls_total = Counter(
    "pizza_agent_calls_total",
    "Total outbound calls by outcome.",
    ["outcome"],
)

ivr_duration = Histogram(
    "pizza_agent_ivr_duration_seconds",
    "Time from media stream start to HOLD transition.",
    buckets=[1, 2, 5, 10, 20, 30, 60, 120],
)

human_phase_duration = Histogram(
    "pizza_agent_human_phase_duration_seconds",
    "Time from human phase start to complete_order.",
    buckets=[10, 30, 60, 120, 180, 300, 600],
)

llm_response_latency = Histogram(
    "pizza_agent_llm_response_latency_seconds",
    "OpenAI response latency during IVR phase.",
    buckets=[0.1, 0.25, 0.5, 1.0, 2.0, 5.0],
)

tts_latency = Histogram(
    "pizza_agent_tts_latency_seconds",
    "ElevenLabs TTS synthesis latency.",
    buckets=[0.1, 0.25, 0.5, 1.0, 2.0, 5.0],
)

ivr_retries_total = Counter(
    "pizza_agent_ivr_retries_total",
    "IVR reprompts by state.",
    ["state"],
)
