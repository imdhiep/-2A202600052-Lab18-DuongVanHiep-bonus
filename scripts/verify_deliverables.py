"""Quick deliverable check — run after all 4 notebooks to confirm rubric criteria."""
import sys, os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from deltalake import DeltaTable
from lakehouse import path
import polars as pl

PASS = "PASS"
FAIL = "FAIL"

def check(label, ok):
    print(f"  [{PASS if ok else FAIL}] {label}")
    return ok

print("=== NB1 — Delta Basics ===")
p1 = path("scratch", "users_delta")
log_files = [f for f in os.listdir(p1 + "/_delta_log") if f.endswith(".json")]
cols = DeltaTable(p1).schema().to_pyarrow().names
check(f"_delta_log has {len(log_files)} JSON files", len(log_files) >= 1)
check(f"Schema has 'tier' column: {cols}", "tier" in cols)

print("\n=== NB2 — Optimize + Z-order ===")
p2 = path("scratch", "events_smallfiles")
dt2 = DeltaTable(p2)
files_after = len(dt2.files())
check(f"Files after optimize: {files_after} (< 200 original)", files_after < 200)

print("\n=== NB3 — Time Travel ===")
p3 = path("scratch", "customers_tt")
history = DeltaTable(p3).history()
ops = [f"v{h['version']}:{h['operation']}" for h in history]
check(f"History >= 5 versions: {ops}", len(history) >= 5)
restore_in = any("RESTORE" in h["operation"].upper() for h in history)
check(f"RESTORE appears in history: {restore_in}", restore_in)

print("\n=== NB4 — Medallion Pipeline ===")
bronze_n = DeltaTable(path("bronze", "llm_calls_raw")).to_pyarrow_table().num_rows
silver_n = DeltaTable(path("silver", "llm_calls")).to_pyarrow_table().num_rows
gold_df = pl.from_arrow(DeltaTable(path("gold", "llm_daily_metrics")).to_pyarrow_table())
n_dates = gold_df["date"].n_unique()
n_models = gold_df["model"].n_unique()
check(f"Bronze exists: {bronze_n:,} rows", bronze_n > 0)
check(f"Silver < Bronze: {silver_n:,} < {bronze_n:,}", silver_n < bronze_n)
check(f"Gold >= 7 dates: {n_dates}", n_dates >= 7)
check(f"Gold >= 3 models: {n_models}", n_models >= 3)
check("cost_usd non-zero", gold_df["cost_usd"].sum() > 0)
check("p50/p95 populated", gold_df["p50_latency_ms"].sum() > 0)

print("\n=== Gold preview ===")
print(gold_df.sort("date", "model"))
