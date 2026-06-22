# LLM Observability Lakehouse at 1B Requests/Day — Architecture Brief

## Problem Statement

A foundation-model API team logs every request/response across multiple tenant deployments. At **1 billion requests per day** (~5 KB per request in JSON format), this generates **5 TB of raw data daily**. The team needs:

1. **Per-tenant cost & latency dashboards** that refresh **every 5 minutes** (BI/ops dashboards must not go stale).
2. **Full prompt/response retention** for 7 days only (incident investigation, security audits, SLA disputes), with aggregate metrics kept for 1 year (annual reports, trend analysis).
3. **PII redaction before any human-readable access** — phone numbers, email addresses, user IDs must never appear in their raw form in any layer that an analyst or engineer can query. The redaction must happen **at Bronze ingestion time**, not later.
4. **Storage cost capped at $5,000/month** — this is a hard constraint set by finance, and includes all tiers (hot, warm, cold).

**Why this is hard:** These four constraints are in tension. Cheap cloud storage favors bulk archival (cold tiers, delete aggregates after 1 year). Fast dashboards favor keeping all recent data in fast-access tiers (S3 Standard). 5-minute refresh and 7-day retention with PII redaction means the tokenization step is on the hot path and cannot be deferred.

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           INGEST PATH (API requests)                        │
│                                                                              │
│  Production API  ──→  Kafka/Kinesis  ──→  Micro-batch Consumer (~1 min)   │
│  (1B req/day)        (buffering)          (tokenize PII in memory)         │
│                                            ↓                                │
│                                    Bronze Table                             │
│                                (Parquet/Delta format)                       │
│              Partitioned: date, tenant_id | Z-ordered: tenant_id           │
│                   ↓                                                          │
│          [7-day window in S3 Standard]                                      │
│          [Auto-drop partitions > 7 days]                                    │
│          [~12 TB compressed, ~$280/mo]                                      │
└─────────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│                         SILVER LAYER (validated)                            │
│                                                                              │
│  DuckDB SQL: parse JSON, deduplicate, verify no raw PII remains            │
│  ROW_NUMBER() OVER (PARTITION BY request_id ORDER BY ts) dedup             │
│                                                                              │
│              Silver Table (same partitioning as Bronze)                     │
│                   ↓                                                          │
│          [Attached to ~2 TB cost for dedup-filtered rows]                   │
└─────────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│                    GOLD LAYER (aggregated metrics)                          │
│                                                                              │
│  5-min incremental aggregation job:                                         │
│  - QUANTILE_CONT(latency_ms, 0.50) / 0.95 for p50/p95 latency              │
│  - SUM(prompt_tokens), SUM(completion_tokens) for usage                     │
│  - AVG(status != 'ok') for error_rate                                       │
│  - JOIN with cost table: (prompt_tokens × in_rate) + (completion × out)    │
│  Grain: date × tenant_id × model                                           │
│  Z-ordered by tenant_id for dashboard filter speed                         │
│                                                                              │
│              Gold Table (date-partitioned, ~1–2 GB/year)                    │
│                   ↓                                                          │
│       [S3 Standard, 1-year retention, ~$0.50/year]                          │
│       [Queries p95 < 1 sec; dashboard-facing only]                          │
│                                                                              │
│  ┌─────────────────────────────────────┐                                    │
│  │    Analyst Dashboard (BI tool)       │                                    │
│  │  - Cost by tenant (p95 latency UX) │  ← Only reads Gold                  │
│  │  - Refresh every 5 min              │                                    │
│  │  - Never exposes raw PII            │                                    │
│  └─────────────────────────────────────┘                                    │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                    TOKEN VAULT (separate table, short TTL)                  │
│                                                                              │
│  user_id_token (SHA-256 HMAC) ← → original user_id (plaintext)             │
│  [ Valid only for 7 days — entries expire auto, become irreversible ]      │
│                                                                              │
│  Use case: 3-AM incident review                                            │
│  - Given token from Bronze, look up original user_id in vault              │
│  - After 7 days, tokens are "permanent pseudonyms" — no re-identification   │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Main Architectural Decisions

### 1. **Table Format: Delta Lake (not Iceberg, not Lance)**

**Chosen:** Delta Lake (via `deltalake` library, same format Spark/Databricks write).

**Rejected Iceberg** — While Iceberg's hidden partitioning would simplify schema evolution and partition-column flexibility, it adds:
- Extra cataloging layer (Nessie/REST catalog for multi-writer coordination) — complexity not justified at this stage.
- Engine ecosystem still consolidating; delta-rs + DuckDB is proven production-grade. Iceberg + delta-rs interop (per Apache XTable) is not yet mainstream.
- No clear benefit for this use case (partitioning is stable: `date`, `tenant_id`).
- **Tradeoff:** Iceberg wins on schema flexibility; Delta wins on operational simplicity here.

**Rejected Lance** — Lance is built for embedding/vector workloads with vector indexes (HNSW, IVF). LLM observability is structured event logs (timestamps, request_ids, latency numbers, cost floats). Lance adds overhead (vector index ingestion, vector-specific query planning) with zero benefit.
- **Tradeoff:** Lance optimizes the wrong workload.

**Cost impact:** Delta → Parquet on-disk compression ~3×, matching Iceberg/Lance; no cost advantage or disadvantage.

---

### 2. **PII Handling: Tokenize at Bronze Ingestion, Not Later**

**Chosen:** Deterministic HMAC-based tokenization of PII fields (user_id, email, phone) happens **in the Kafka consumer process**, before any write to Bronze. Tokens are reversible for 7 days via a separate Token Vault table, then vault entries expire and tokens become permanent pseudonyms.

**Rejected "Encrypt-at-rest only"** — Data at rest in S3 is encrypted (AWS KMS). But this only protects against bucket compromise; any engineer or analyst with legitimate read access to Bronze (for debugging, data pulls, etc.) can still see raw user_ids. Compliance requirement: "redact before anyone reads the data." Encryption does not meet this.
- **Tradeoff:** Encryption is cheaper (no tokenization CPU cost); tokenization is compliant.

**Rejected "Redact at Silver"** — Could push tokenization to the DuckDB SQL step at Silver ingestion. But this leaves raw PII sitting in Bronze for the duration of the dedup window (~few seconds to minutes) and requires Bronze table to have ACLs restricting read access. At high scale (11.5K req/sec), micro-batching introduces ordering variability — some raw requests may sit in Bronze longer than expected during backpressure. Also, audit logging (detecting who accessed what) is easier if PII never exists in a queryable form.
- **Tradeoff:** Silver-stage redaction is simpler in orchestration; Bronze-stage redaction is more defensible operationally.

**Cost impact:** ~50–100 CPU-cycles per request for HMAC + token-vault lookup in the consumer. At 11.5K req/sec, this is 575K–1.15M CPU-cycles/sec ≈ 1–2 cores sustained. Negligible vs. compute budget for 1B req/day.

---

### 3. **Partitioning & Clustering: `date` partitions, Z-order by `tenant_id`**

**Chosen:** Bronze partitioned by `date` (YYYY-MM-DD). Within each date partition, Z-order by `tenant_id` so that tenant-specific dashboards prune files aggressively.

**Rejected partition-by-tenant** — The system has an unbounded tenant cardinality (could be 1K, 10K, or 100K tenants). If each tenant is its own partition, a peak-time batch write could create 10K new partitions in seconds, fragmenting the Bronze table. This is the small-file problem (NB2 of the lab) at extreme scale.
- **Tradeoff:** Tenant partitioning is operationally cleaner ("each tenant's data is in one place"); date partitioning + Z-order distributes tenants within each date, requiring dynamic stat pruning at query time but avoids partition explosion.

**Rejected partition-by-hour** — Microservice-friendly (can drop old data by hour, not waiting until midnight UTC). At 1B req/day ÷ 24 = ~41.67M req/hour, if batching in 1-hour buckets, the micro-batch consumer would accumulate ~41M rows before flushing. At ~5 KB/row in-memory, that's ~200 GB buffering — infeasible. Even with 1-minute micro-batches (~694K rows, ~3.5 GB), hourly partitions create 24× more partitions than daily, worsening the small-file problem when OPTIMIZE runs.
- **Tradeoff:** Hour-level partitioning is operationally clean; day-level + Z-order is scalable.

**Cost impact:** Date partitioning → ~10–20 active partitions in memory at any time (recent ~10 days), manageable. Tenant Z-order → file pruning at query time (no extra storage cost, but query planning is slightly more expensive). Wash.

---

### 4. **Ingestion Cadence: Micro-batch (~1 min trigger), not per-request or hourly**

**Chosen:** Kafka consumer buffers requests in memory, flushes to Delta/Parquet every ~60 seconds (or whenever buffer reaches a threshold like 100M rows / 500 GB, whichever is first).

**Rejected per-request commits** — At 1B req/day ≈ 11.5K req/sec, if each request triggers a Delta commit, that's 11.5K new files created per second. The small-file problem (NB2 of the lab) shows that 100 small files already degrades query performance; at 11.5K files/sec, you'd accumulate 1 billion files in ~24 hours. OPTIMIZE would need to run continuously and still fall behind, costing ~$500K+/month in pure compaction compute (re-writing 5 TB daily).
- **Tradeoff:** Per-request latency is lowest (sub-second write durability); micro-batch latency is ~60 sec worst-case (one request arrives, waits 60s to be flushed).

**Rejected hourly batch** — Fails the 5-minute dashboard freshness SLA. If data lands every hour, dashboard aggregates refresh hourly, which is too stale for on-call response to spikes.
- **Tradeoff:** Hourly batch is orchestrationally simplest; 1-minute micro-batch meets the SLA.

**Cost impact:** 1-minute flushes → ~1,440 files/day per date partition ≈ 20K files total at steady state (14 days × 1,440). After OPTIMIZE Z-order runs nightly, files consolidate to ~50–100 files/partition (NB2 achieved 3× speedup with Z-order). Compute cost for OPTIMIZE: ~$50–100/month (a few hours of CPU per day). Per-request commits would cost 50× more; hourly batch is slightly cheaper but violates SLA.

---

### 5. **Retention & Lifecycle: Partition-drop at 7 days, not row-level DELETE**

**Chosen:** Cron job runs daily at 00:00 UTC. For each date partition older than 7 days, drop the entire partition directory (Hadoop `rm` + remove _delta_log entries). Cost: ~0 compute (just filesystem delete). Token Vault entries also auto-expire at 7 days (TTL column + cleanup job).

**Rejected `DELETE WHERE ts < now() - 7 days`** — Row-level deletes in Delta use deletion vectors (NB3 of the lab shows `restore()` concept; deletion vectors are the internal mechanism). Each DELETE rewrites all files in affected partitions, because Delta needs to update the transaction log. At 5 TB/day, deleting a day's worth of data forces a rewrite of that day's partition ≈ ~1.7 TB compressed. At $0.023/GB-mo for S3 Standard, that's ~$40 of read + write I/O per day per delete. Over 7 days (7 deletes = 7 days of retention lifecycle), that's $280/month just in delete I/O, plus compute. Against the $5K/mo storage cap, $280/mo is material.
- **Tradeoff:** Row-level DELETEs offer audit trails ("deleted at T, reason R"); partition-drop is silent. But partition-drop is much cheaper and acceptable for operational logs (not a system-of-record).

**Rejected "keep everything in S3 Standard"** — S3 Standard is $0.023/GB-mo. If you keep 5 TB/day × 365 days = 1.825 PB uncompressed = ~608 TB compressed in Standard, that's ~$14K/mo storage alone. Violates the $5K/mo cap. You *must* tier: hot (7-day Standard at ~$280/mo) + warm (30-day Intelligent-Tiering at ~$80/mo) + cold (365-day Glacier at ~$5/mo, nearly free). Lifecycle rules auto-transition objects by age.
- **Tradeoff:** Single-tier simplicity vs. multi-tier compliance with budget.

**Cost impact:** Tiered lifecycle is foundational to meeting the $5K/mo cap. Partition-drop is efficient because it doesn't rewrite data; it just removes old partitions.

---

### 6. **Governance & Access Control: Unity Catalog or Glue with column-level tags**

**Chosen:** Use a governed data catalog (Databricks Unity Catalog, AWS Glue + Lake Formation, or open-source Polaris) with column-level sensitivity tags. Each user's role maps to a tag permission set. At query time, the catalog/engine enforces column masking: queries to Bronze return `user_id` → `REDACTED`, but Gold queries return metrics unmasked.

**Rejected bare filesystem ACLs** — Bronze is on S3, Silver/Gold on S3. S3 bucket-level ACLs or IAM policies can restrict "who can read s3://bronze/*". But this is coarse: either you can read the entire Bronze table or you can't. No column-level masking. If an analyst needs to debug Silver dedup logic (legitimate use case), they'd have to read Bronze, exposing all raw PII. Also, audit logging (CloudTrail + S3 access logs) is reactive — you see *who* accessed *what* after the fact, but there's no preventative mechanism. Compliance auditors often want "the system prevents access, not just logs it."
- **Tradeoff:** Bare filesystem ACLs are cheap (free); catalogs add operational overhead (catalog server, metadata sync, query-engine integration).

**Rejected per-tenant separate buckets** — Each tenant gets their own S3 bucket. Isolates data (if tenant A's bucket is compromised, tenant B is unaffected). But at 1K–10K tenants, you're managing 1K–10K separate S3 buckets (AWS account limits, IAM role sprawl). The shared Gold dashboard ("compare cost across tenants") requires cross-bucket JOINs, which are complex. Tier retention schedules separately per bucket: administrative nightmare.
- **Tradeoff:** Bucket-per-tenant is cleanest isolation; single-bucket + catalog + tags is operationally scalable.

**Cost impact:** Unity Catalog: included in Databricks compute cost (no separate fee). Glue + Lake Formation: ~$1–2/mo metadata overhead for a 1K-table catalog. Polaris (open-source REST catalog, self-hosted): negligible if deployed on existing infra. Worth the cost.

---

## Failure Modes & Recovery

### Failure Mode 1: Tokenization job crashes mid-batch

**Scenario:** Consumer process crashes after writing 50% of a 1-minute batch to Bronze. Some rows have PII tokenized (safe), some still have raw user_ids (non-compliant). Next restart, the job replays from Kafka offset and writes again, now creating duplicate rows in Bronze.

**Detection:** 
- Schema/column-level write-time check: as each row arrives in the consumer, regex-scan the raw JSON for PII patterns (SSN, phone, email formats). If any pattern is detected, reject the entire batch (fail-safe: stop and alert ops).
- Bronze table read-time check: nightly SQL query scanning Bronze for suspicious patterns, alarming if any found.

**Rollback:**
- **Delta `restore(version=V)` to last good version** (Day 18 time-travel concept) — O(1) metadata-only operation, no rewrite. Restores Bronze to state before the partial write.
- Restart consumer from the Kafka offset that triggered the crash. Replay will de-duplicate because of ROW_NUMBER() OVER (PARTITION BY request_id) logic at Silver stage (inherited from NB4).
- RTO: < 1 minute (restore metadata op + replay 1-min batch).

---

### Failure Mode 2: Bad aggregation logic deployed (wrong cost rates)

**Scenario:** Cost table was updated at 02:30 UTC with wrong rates (Claude Haiku marked as $0.01/1M input by mistake, should be $0.80/1M). Gold aggregation job runs at 03:00 UTC, writing metrics with wrong cost_usd values. Dashboards start showing wildly wrong numbers. Ops team doesn't notice until 08:00 UTC (5 hours later).

**Detection:**
- Anomaly detection: compare cost_per_request (cost_usd / token_count) vs. rolling 30-day percentile. If today's metric is outside p1–p99 of the rolling window, alert.
- Audit log: track which cost table version was used for each Gold aggregation run. Alert on unexpected table version change.

**Rollback:**
- Query Gold with `versionAsOf(version=V)` (Day 18 time-travel) to read metrics from the last good version (02:00 UTC run).
- Serve dashboards from the good version while the job is being re-run with corrected cost table.
- Re-run Gold aggregation from 03:00 UTC onward.
- RTO: < 10 minutes (query plan changes to older version + cost-table fix + recompute).

---

### Failure Mode 3: Lifecycle job fails to drop old Bronze partitions

**Scenario:** Scheduled partition-drop job runs daily, deletes partitions > 7 days old. But one day it crashes (e.g., insufficient S3 permissions, network timeout to the Lakehouse). Operators miss the alert (PagerDuty flakiness or on-call was on vacation). 8 days of data is not dropped, 9 days, 10 days. After 30 days, 23 extra days worth of data (5 TB × 23 = 115 TB) is sitting in S3 Standard. Storage bill jumps from $280/mo to ~$2,800/mo. Violates the $5K cap significantly (is now $3K for storage alone, leaving only $2K for compute + licensing).

**Detection:**
- Daily alert: query Glue Catalog or S3 inventory to count partitions. Alert if partition count exceeds expected maximum (e.g., if keeping 14 days + 1 day buffer, expect ≤ 16 partitions; if > 20, something is wrong).
- Monthly budget tracker: if storage cost projected > $1K, alert ops *before* month-end surprise.

**Rollback:**
- Manual partition-prune: run `s3 rm s3://bucket/bronze/date=2026-05-xx --recursive` for each old partition.
- Fix lifecycle job: idempotent retry (mirrors `reset()` pattern in `scripts/lakehouse.py`). If job previously failed at partition P, retry will delete it on next run even if metadata log wasn't updated.
- Trigger `OPTIMIZE` on remaining Bronze partitions (consolidate small files created during the 30-day backlog).
- RTO: ~1 hour (manual cleanup + OPTIMIZE consolidation).

---

## Cost Estimate (Back-of-Envelope)

### Storage

| Component | Volume | Rate | Cost/mo |
|-----------|--------|------|---------|
| Bronze (7-day hot, S3 Standard) | 5 TB/day × 7 days = 35 TB raw → 11.7 TB compressed (3× Parquet) | $0.023/GB | $270 |
| Bronze (8–30 day warm, S3 Intelligent-Tiering) | 5 TB/day × 23 days = 115 TB raw → 38.3 TB compressed | $0.0125/GB (average IA tier) | $480 |
| Bronze (30+ day cold, S3 Glacier Flexible) | 5 TB/day × 335 days = 1,675 TB raw → 558 TB compressed | $0.004/GB | $2,230 |
| Silver (7-day, S3 Standard, after dedup ~40% of Bronze) | ~4.7 TB compressed | $0.023/GB | $110 |
| Gold (1-year, S3 Standard, aggregated ~1 GB/month) | ~12 GB / year | $0.023/GB | <$1 |
| **Total Storage** | | | **~$3,090/month** |

### Compute

| Task | Frequency | Estimated cost |
|------|-----------|-----------------|
| Micro-batch consumer (tokenization + write) | Continuous, 1 min batches | ~$200/mo (1–2 small EC2 instances or Lambda bulk) |
| DuckDB → Silver aggregation (5-min incremental, dedup) | Every 5 min | ~$100/mo (lightweight query, 10–30 sec per run) |
| Gold aggregation (join + aggregation, 5-min incremental) | Every 5 min | ~$100/mo |
| OPTIMIZE + Z-order nightly (consolidate Bronze/Silver) | Daily, 30–60 min | ~$50/mo (off-peak, S3 is not metered per hour for compute) |
| Token Vault TTL cleanup (delete 7-day-old entries) | Daily, ~1 min | <$1/mo |
| **Total Compute** | | **~$450/mo** |

### Governance & Tooling

| Tool | Cost |
|------|------|
| Databricks workspace (if using Unity Catalog) | $1,500–3,000/mo (separate from compute above) |
| AWS Lake Formation / Glue (if not Databricks) | $50–100/mo metadata |
| Monitoring & Alerting (CloudWatch + custom) | $100–200/mo |
| **Total Tooling** | **~$1,000–3,000/mo** |

### **Total TCO**
- **Storage:** $3,090/mo
- **Compute:** $450/mo
- **Tooling:** $1,000–3,000/mo (depending on platform choice)
- **Total: $4,500–6,500/mo**

**Against the $5,000 storage cap:** Storage is $3,090 (✅ within budget). The cap as literally stated in the brief is for storage only. Compute + tooling ($1,500+) is the real total cost of ownership, but not charged against the storage cap. If the brief meant "total cloud bill ≤ $5K," then this design fits only by choosing a lightweight open-source catalog (Polaris) instead of Databricks, bringing tooling down to <$100/mo and total TCO to ~$3,600/mo.

**Cost optimization levers:**
- Pre-compress Bronze with Zstandard (tunable compression ratio) instead of Parquet defaults; could push 3× compression to 4×.
- Archive Gold after 1 year to Glacier Deep Archive ($0.00099/GB) if there's no compliance requirement for fast retrieval.
- Use S3 Batch Operations to auto-transition lifecycle rules more aggressively (move to Glacier after 14 days instead of 30).

---

## MVP Slice (Week 1)

**Goal:** Prove the hardest piece works — compliant PII handling at 1B req/day scale, with controlled 7-day retention lifecycle.

**Deliverable:** Bronze layer only, end-to-end.
- Kafka consumer running (or simulated from file batch).
- HMAC tokenization of user_id in-flight.
- Bronze table written to S3 with date + tenant_id partitioning.
- Partition-drop job runs, old partitions expire, storage cost stays flat.
- Token Vault lookup tested: within 7 days, can re-identify; after 7 days, tokens are irreversible.

**Out of scope for MVP:** Silver dedup job, Gold aggregation, dashboards, Catalog integration, OPTIMIZE compaction.

**Why this slice:** If the tokenization + lifecycle piece works, the rest (Silver/Gold aggregation) is straightforward data pipeline — reuse the NB4 `DuckDB.sql()` patterns. The compliance + retention piece is the novel, highest-risk component. Proving it week 1 gives time to troubleshoot at scale before building analytics layers.

**Runbook to ship week 1:**
1. Spin up Kafka cluster (or use MSK).
2. Deploy consumer (Python + deltalake + hvac for key management).
3. Gen synthetic 1B-req simulation (10-hour test = 1M req/sec batch, or run lower-traffic shadow for 24 hours).
4. Verify Bronze partition layout, dedup'd content (no raw user_ids), and lifecycle job correctness.
5. Run incident-simulation test: pick a request from 6 days ago (should re-identify), pick one from 8 days ago (should fail). Success criteria: both behaviors work.

---

## Day 18 Concepts Applied

| Concept | How Applied |
|---------|-------------|
| **Medallion Architecture** | Bronze → Silver → Gold (NB4 directly; extended for PII tokenization & lifecycle) |
| **ACID & Transaction Log** | Delta transaction log records each write atomically; `restore()` rolls back mid-pipeline failures |
| **Time Travel** | `versionAsOf()` for dashboards to read "good" Gold snapshots while failures are repaired; `restore()` for partition-level recovery |
| **Schema Evolution** | Token Vault evolves to add `expired_at` column when TTL cleanup is needed; `schema_mode="merge"` handles Silver schema projection |
| **Partitioning & Z-order** | Date partitions (lifecycle), Z-order by tenant_id (query performance) — directly mirroring NB2 optimization techniques at extreme scale |
| **Deletion Vectors** | Row-level DELETEs in Token Vault (entries older than 7 days) use deletion vectors; alternative (partition-drop in Bronze) avoids this cost |
| **Lineage & Governance** | Catalog tags track PII columns across layers; lineage shows which queries have touched PII, for compliance audits |
| **FinOps & Tiering** | Multi-tier S3 (Standard → IA → Glacier) with lifecycle rules to hit cost cap; compute cost tracking per job |

---

## Conclusion

This architecture balances the four constraints (1B req/day, 5-min dash refresh, 7-day raw retention, $5K/mo storage) by:
1. **Delta Lake** for simplicity & proven production track record.
2. **Tokenization at ingestion** for compliance (PII never visible in queryable layers).
3. **Micro-batch 1-min cadence** for freshness without the small-file explosion.
4. **Date partitioning + Z-order** for lifecycle efficiency and query speed.
5. **Multi-tier S3 lifecycle** to stay within budget.
6. **Catalog + column tags** for governed access & audit.

The MVP proves compliance + retention (the hard part) before investing in the analytics layer. Post-MVP, Silver/Gold follow standard medallion patterns already validated by NB4.

