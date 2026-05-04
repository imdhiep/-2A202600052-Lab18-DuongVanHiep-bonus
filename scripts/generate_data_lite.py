"""Generate synthetic LLM-observability records into Bronze (lightweight path).

Default 200K rows — small enough that all 4 notebooks finish in under a minute
on a laptop. Override with `python generate_data_lite.py 1000000`.

Realism choices that matter for NB4:
  - Timestamps spread across 7 days so Gold has 7 × 3 = 21 rows (interesting).
  - ~5% of `request_id`s are duplicated (retry pattern) so Silver dedup is
    actually observable: Silver row count < Bronze row count.
  - Latency is not unbounded — capped to a sensible range so p95 looks like
    a real LLM service, not a stuck pipeline.
"""
from __future__ import annotations

import json
import random
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import polars as pl
from deltalake import write_deltalake

from lakehouse import path, reset


# Per-model latency mean/std (ms). Loosely based on real product behavior
# but synthetic — do not cite as benchmarks.
LATENCY_PROFILES = {
    "claude-haiku-4-5":   (450,  150),
    "claude-sonnet-4-6":  (1100, 350),
    "claude-opus-4-7":    (2400, 700),
}

DUP_RATE = 0.05  # ~5% of request_ids reappear (retry pattern)
DAYS_SPAN = 7    # timestamps spread across this many UTC days


def _sample_latency(model: str, completion_tokens: int) -> int:
    base, jitter = LATENCY_PROFILES[model]
    # Output tokens dominate latency for LLMs (autoregressive decoding).
    per_token = base / 800.0  # so ~800 tokens ≈ `base` ms
    ms = per_token * completion_tokens + random.gauss(0, jitter)
    return max(50, int(ms))


def main(n_rows: int = 200_000) -> None:
    random.seed(42)
    start = datetime(2026, 4, 1, tzinfo=timezone.utc)
    span_seconds = DAYS_SPAN * 24 * 3600
    models = ["claude-sonnet-4-6"] * 6 + ["claude-haiku-4-5"] * 3 + ["claude-opus-4-7"]
    statuses = ["ok"] * 95 + ["rate_limited"] * 3 + ["error"] * 2

    rows = []
    seen_ids: list[str] = []
    for i in range(n_rows):
        # Spread across the full span for a multi-day Gold table.
        ts = start + timedelta(seconds=int(i * span_seconds / n_rows))
        m = random.choice(models)
        pt = random.randint(50, 4000)
        ct = random.randint(20, 2000)
        latency = _sample_latency(m, ct)

        # Inject ~DUP_RATE retries — same request_id, slightly later ts.
        if seen_ids and random.random() < DUP_RATE:
            rid = random.choice(seen_ids[-1024:])  # recent retries are realistic
        else:
            rid = str(uuid.uuid4())
            seen_ids.append(rid)

        rows.append({
            "request_id": rid,
            "ts": ts,
            "raw_json": json.dumps({
                "model": m,
                "user_id": f"u_{random.randint(1, 5000)}",
                "usage": {"input": pt, "output": ct},
                "latency_ms": latency,
                "status": random.choice(statuses),
            }),
        })

    df = pl.DataFrame(rows)
    out = path("bronze", "llm_calls_raw")
    reset(out)
    write_deltalake(out, df.to_arrow(), mode="overwrite")
    n_unique = df.select(pl.col("request_id").n_unique()).item()
    print(
        f"Wrote {n_rows:,} rows -> {out}\n"
        f"  unique request_ids: {n_unique:,}  ({n_rows - n_unique:,} duplicates seeded for dedup demo)\n"
        f"  date span: {DAYS_SPAN} UTC days from {start.date()}"
    )


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 200_000
    main(n)
