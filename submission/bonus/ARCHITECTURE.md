# Architecture Brief — LLM Observability Lakehouse at 1B Requests/Day

**Dương Văn Hiệp · 2A202600052 · VinUniversity AICB · Day 18 Track 2**

---

## 1. Problem Statement

A foundation-model API team (Claude-style, multi-tenant SaaS) needs to log
every request and response for billing, debugging, and compliance.

**Scale:** 1 B requests/day · ~5 KB raw/request → **5 TB/day raw ingestion**
(~11,574 req/sec peak, 3× spikier during US business hours → 35 K req/sec burst).

**Requirements:**
- Per-tenant cost & latency dashboards, refresh ≤ 5 min lag
- Full prompt/response retained **7 days** (incident review); aggregates only for **1 year**
- PII (user e-mail, IP, content) **redacted before any analyst can read**
- All tenants share one lakehouse but are **logically isolated** (no cross-read)
- Hard budget cap: **$5 K/month total storage + compute**

**Why hard:** streaming ingest at 35 K req/sec + sub-5-min freshness + $5 K cap
is contradictory at face value. The architecture must trade raw retention depth
for cost rather than latency.

---

## 2. Architecture Diagram

```
                          ┌─────────────────────────────────────────────────────┐
  API Gateway             │  BRONZE ZONE  (raw, PII-tokenized, 7-day TTL)       │
  (35K req/sec) ──Kafka──►│  s3://lh/bronze/llm_calls_raw/                      │
                          │  partition: date=YYYY-MM-DD/tenant_id=XXX            │
                          │  format: Delta Lake  (Append-only, no updates)       │
                          │  VACUUM after 7 days  →  files deleted by S3 LC rule │
                          └────────────────┬────────────────────────────────────┘
                                           │  5-min micro-batch
                                           ▼
                          ┌─────────────────────────────────────────────────────┐
                          │  SILVER ZONE  (deduped, typed, no raw PII, 1-year)  │
                          │  s3://lh/silver/llm_events/                          │
                          │  partition: date / tenant_id                         │
                          │  Z-ORDER BY: tenant_id, model                        │
                          │  OPTIMIZE cadence: every 30 min                      │
                          └────────────────┬────────────────────────────────────┘
                                           │  hourly aggregation
                                           ▼
                          ┌─────────────────────────────────────────────────────┐
                          │  GOLD ZONE  (tenant metrics, forever)               │
                          │  s3://lh/gold/tenant_hourly_metrics/                 │
                          │  schema: (date, hour, tenant_id, model,             │
                          │           p50_ms, p95_ms, cost_usd, error_rate,     │
                          │           req_count)                                 │
                          │  Dashboard reads Gold only — never Bronze/Silver     │
                          └─────────────────────────────────────────────────────┘

Catalog:  AWS Glue Data Catalog (REST-compatible)
Compute:  Flink (Bronze ingest) + DuckDB on Lambda (Silver→Gold agg)
Query:    Athena (ad-hoc) · Redshift Spectrum (dashboards)
```

---

## 3. Key Decisions with Rejected Alternatives

### D1 — Table Format: **Delta Lake** (rejected: Iceberg, raw Parquet)

| Choice | Reason rejected |
|---|---|
| **Apache Iceberg** | Rejected. Iceberg's manifest-list design gives better concurrent write scalability beyond 10 writers, but our Bronze write path is a single Flink job per partition. Delta's transaction log is simpler to operate and our team has existing delta-rs tooling from the lab. Iceberg's hidden-partition evolution is compelling but not worth the migration cost for this team's skill set. |
| **Raw Parquet + Hive metastore** | Rejected. No ACID → duplicate rows on Flink checkpoint replay; no time travel → can't RESTORE after a bad PII-redaction job; no schema evolution → adding new model fields requires rewriting all partitions. Eliminated on first principles. |
| **Delta Lake** ✓ | `write_deltalake` supports streaming appends with exactly-once semantics via txn IDs. VACUUM enforces 7-day Bronze TTL. Time travel lets us `RESTORE` Silver to pre-bad-job state within seconds (demonstrated: NB3 RESTORE in 0.01s). |

### D2 — Partitioning Strategy: **date-only partition + Z-order by (tenant_id, model)** (rejected: date+tenant partition, tenant-only partition)

| Choice | Reason rejected |
|---|---|
| **Partition by (date, tenant_id)** | Rejected. Delta-rs (and Spark delta) prohibit Z-ordering on partition columns — `DeltaError: Z-order columns cannot be partition columns`. So if `tenant_id` is a partition, we lose Z-order's file-skipping on tenant queries (the primary hot path). Also: 10 K tenants × 365 days = 3.65 M partition directories; S3 LIST at query-plan time becomes the bottleneck. |
| **Partition by tenant_id only** | Rejected. OPTIMIZE and VACUUM run per partition. 10 K tenant partitions × daily OPTIMIZE = 10 K jobs/day. Operational burden unacceptable. |
| **Partition by date + Z-order by (tenant_id, model)** ✓ | 365 daily partitions — manageable. Within each date partition, Z-order clusters rows by `tenant_id` so a `WHERE tenant_id = 'acme'` query skips files whose `tenant_id` min/max range excludes `'acme'`. PoC measured **5.3× file-pruning with 20 tenants** (7 of 37 files scanned). At 10 K tenants in production the same mechanism yields ~142× pruning, keeping Athena scan cost bounded. Dashboard queries always include `date =` (partition prune) + `tenant_id =` (Z-order skip) — two-level pushdown. |

### D3 — PII Handling: **SHA-256 tokenization at Bronze write time** (rejected: post-hoc redaction, field-level encryption)

| Choice | Reason rejected |
|---|---|
| **Post-hoc redaction (Silver job)** | Rejected. There is a window — between Bronze write and Silver redaction — when unredacted PII sits on S3 readable by anyone with bucket access. Decree 13/GDPR do not allow this window. Compliance risk outweighs the implementation simplicity. |
| **Field-level encryption (AES-256 per row)** | Rejected. Key rotation requires rewriting every encrypted row — at 5 TB/day × 7 days = 35 TB, rotation cost is ~$35 in S3 PUT fees plus compute. SHA-256 one-way token is sufficient for billing (tenant correlation) and cheaper; full prompt is retained encrypted at rest by S3 SSE, accessible only via audit-gated IAM role. |
| **SHA-256 tokenization at ingest** ✓ | Flink's Bronze writer applies `SHA256(user_id + daily_salt)` before the record touches S3. Salt rotates daily — old tokens are irrecoverable after 7 days, matching Bronze TTL. Silver and Gold contain only tokens, never raw PII. Audit table records every tokenization key issuance. |

### D4 — Freshness vs Cost: **5-min micro-batch Silver aggregation** (rejected: real-time streaming, hourly batch)

| Choice | Reason rejected |
|---|---|
| **Real-time streaming (Flink → Silver in < 30 s)** | Rejected. Flink stateful aggregation (deduplicate by request_id + compute p95) at 35 K req/sec requires ~32 GB of managed state. At $0.000064/GB-hour on Kinesis Data Analytics, that's $1,474/month for compute alone — 30% of our $5 K cap just for freshness we don't need. Dashboard SLA is 5 min, not 30 s. |
| **Hourly batch** | Rejected. Violates the 5-min dashboard SLA. A P1 incident (mass error spike) detected 60 min late is a support-ticket avalanche. |
| **5-min micro-batch** ✓ | DuckDB on Lambda reads the last 6-min Bronze window, runs the dedup + aggregation SQL, and `MERGE`s into Silver. Lambda cost: $0.20 per 1M invocations × 288 invocations/day × 30 days = $1.73/month. Silver freshness: p50 lag = 2.5 min (half the batch interval). |

### D5 — Catalog: **AWS Glue Data Catalog** (rejected: Unity Catalog, Hive Metastore, Polaris)

| Choice | Reason rejected |
|---|---|
| **Databricks Unity Catalog** | Rejected. Ties compute to Databricks' proprietary runtime. We use DuckDB + Athena for cost reasons ($0.005/query-TB vs Databricks DBU pricing). Unity Catalog's row-level security is excellent but reproducible with Iceberg/Delta row filters at lower cost. |
| **Apache Polaris (Iceberg REST)** | Rejected. Polaris is production-ready as of 2025 but assumes Iceberg format. Switching table format and catalog simultaneously is two blast radii. Re-evaluate at Q2 migration window. |
| **Hive Metastore on EC2** | Rejected. Operational burden: we'd own HA, backups, and schema-migration DDLs. Single point of failure with no managed SLA. |
| **AWS Glue Data Catalog** ✓ | Serverless, pays per API call (~$1/month at our query volume), natively integrated with Athena and S3. Supports Delta Lake tables via delta-rs Glue connector. No servers to operate. |

### D6 — Retention Enforcement: **VACUUM + S3 Lifecycle Rule** (rejected: TTL column + DELETE, separate archive bucket)

| Choice | Reason rejected |
|---|---|
| **TTL column + scheduled DELETE** | Rejected. `DELETE FROM bronze WHERE date < NOW()-7` on a Delta table writes deletion vectors and generates tombstone files — still costs $0.005/1K DELETE API calls. VACUUM is cheaper and cleans both data files and orphaned log files atomically. |
| **Separate archive bucket with Glacier** | Rejected. Compliance requirement is *deletion* at 7 days, not archival. Glacier copies would still contain PII and would need key destruction — complex. S3 Lifecycle `Expiration` rule deletes objects at day-8 with zero API cost. Combined with `VACUUM(retention=168h)`, no Bronze file survives day 7. |

---

## 4. Failure Modes

### FM1 — Duplicate records flood Silver (3 AM scenario)

**Trigger:** Flink checkpoint restores from offset T-5 min after node failure. Records between T-5 and T are re-delivered to Bronze with the same `request_id`.

**Detection:** Silver row-count alert: `silver_count / bronze_count > 1.005` within a 5-min window triggers PagerDuty.

**Rollback (Day 18 — Time Travel):**
```python
# Silver is partitioned by date. Rollback today's partition only.
dt = DeltaTable("s3://lh/silver/llm_events")
dt.restore(version=last_clean_version)   # identified from history()
# Re-run 5-min micro-batch from last clean Bronze checkpoint.
```
RESTORE on Silver takes < 5 s (NB3 demonstrated 0.01 s on 150 K rows). Blast radius: at most 5 minutes of stale Gold metrics until next aggregation cycle.

### FM2 — PII tokenization salt rotation bug exposes raw user_id

**Trigger:** Flink deployment rolls out new salt but old consumers still write with yesterday's salt → two tokens map to same user across dates → analyst can cross-correlate days and reverse-engineer identity.

**Detection:** `audit_token_issuance` table: `COUNT(DISTINCT salt) WHERE date = TODAY > 1` triggers immediate alert.

**Rollback:**
1. Halt Bronze writers immediately (circuit breaker in API Gateway).
2. `VACUUM Bronze RETAIN 0 HOURS` on today's partition — deletes the bad files.
3. Reissue correct salt, replay today's Kafka topic from offset 0.
4. Incident report to DPO within 72 hours (Decree 13 breach notification window).

### FM3 — Small-file explosion in Silver after OPTIMIZE job failure

**Trigger:** The 30-min OPTIMIZE Lambda times out (> 15 min) during a traffic spike. Next 10 batches each add 5-min micro-batch files → 50+ small files accumulate in hot partition.

**Detection:** `len(dt.files())` monitored in CloudWatch. Alert when `files_in_last_partition > 100`.

**Rollback (Day 18 — OPTIMIZE + Z-order):**
```python
# Force-compact the affected partition only:
dt.optimize.compact(partition_filters=[("date", "=", today_str)])
dt.optimize.z_order(["tenant_id", "model"],
                    partition_filters=[("date", "=", today_str)])
```
This reproduces exactly what NB2 demonstrated: 200 files → 55 files, query speedup 8.2×. Dashboard SLA is restored within 2 min of compact completion.

---

## 5. Cost Back-of-Envelope

| Component | Math | $/month |
|---|---|---:|
| **Bronze S3 storage** (7-day rolling) | 5 TB/day × 7 days = 35 TB × $0.023/GB = | $805 |
| **Silver S3 storage** (1-year, compressed) | 5 TB/day × 0.05 compress ratio × 365 days = 91 TB × $0.023/GB = | $2,093 |
| **Gold S3 storage** (forever, tiny) | 365 days × 10 K tenants × 8 models × 24 hours × ~200 B/row ≈ 50 GB × $0.023 = | $1 |
| **S3 PUT (Bronze ingest)** | 1B req/day × 30 days = 30B × $0.000005 = | $150 |
| **Lambda compute (Silver micro-batch)** | 288 runs/day × 30 days × $0.20/1M invocations + 512 MB × 60 s × $0.0000166667/GB-s = | $15 |
| **Athena ad-hoc queries** | 200 queries/day × 30 days × avg 50 GB scanned × $0.005/GB = | $1,500 |
| **Kafka (MSK)** | 35 K msg/sec → 2-broker m5.xlarge = | $350 |
| **CloudWatch + alerting** | — | $50 |
| **Total** | | **$4,964** |

**$36 headroom** below the $5 K cap. The largest lever is Athena scan cost — enforcing Silver partition pruning (`WHERE date = ? AND tenant_id = ?`) in all dashboard queries keeps this bounded.

---

## 6. MVP Slice — One Week

**Goal:** Prove that 5-min micro-batch Bronze→Silver→Gold works end-to-end
with PII tokenization and a live dashboard — without Kafka or 1B-scale load.

**Week 1 scope (single engineer, 40 hours):**

1. **Day 1–2:** Bronze writer — Python generator simulates 100 K req/hr into
   `s3://lh-dev/bronze/` via `write_deltalake`, with SHA-256 token applied to
   `user_id`. Partition by `date/tenant_id`.

2. **Day 3:** Silver micro-batch job — DuckDB SQL dedup + MERGE into Silver,
   cron every 5 min. Verify `silver_n < bronze_n` (same assertion as NB4).

3. **Day 4:** Gold aggregation — `QUANTILE_CONT` p50/p95 per `(hour, tenant,
   model)`. Matches NB4 Gold schema extended with `tenant_id` and `hour`.

4. **Day 5:** Cost meter — write a notebook that reads Gold and outputs
   `cost_usd` per tenant per day using the cost table from NB4. This is the
   billing artifact — if it's right, the architecture works.

**Definition of done:** A Grafana dashboard reading Gold shows p95 latency
and cost_usd per tenant, refreshed within 6 min of simulated ingest, running
entirely on a $0-cost dev environment (local DuckDB + Delta).

**Not in MVP:** Kafka, PagerDuty alerts, VACUUM automation, multi-region,
real PII data. Each is a week-2+ item with a clear hand-off point.

---

*Architecture brief for VinUniversity AICB Day 18 Track 2 Bonus Challenge.*
*Topic A: LLM Observability at 1B Requests/Day.*
