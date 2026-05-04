# Lab 18 Reflection — Duong Van Hiep

## Anti-pattern most at risk: Small-file proliferation ("Delta Swamp")

Họ và tên: Dương Văn Hiệp - 2A202600052

In our LLM observability pipeline, the Bronze layer ingests one Delta append
per API call batch — potentially thousands of tiny writes per minute. Without
a scheduled `OPTIMIZE + Z-ORDER`, the `_delta_log` grows unbounded and
queries degrade because the engine must open hundreds of files to evaluate
even a narrow predicate like `model = 'claude-opus-4-7'`.

NB2 demonstrated this concretely: 200 micro-appends produced 200 files;
a point-query took ~300 ms before optimization and dropped to under 10 ms
after `compact() + z_order(["user_id"])` — a 30×+ speedup — because
Z-order's min/max stats let the engine skip all but 1–2 files.

The fix is a recurring `OPTIMIZE` job on Silver (post-dedup), where the
data is stable and the file layout actually benefits downstream Gold
aggregations. Bronze can stay append-only; Silver is the compaction target.

**Lesson:** open table formats give you ACID and time travel for free, but
file-layout discipline is still the operator's responsibility. Automate
`OPTIMIZE` or you recreate the Hive small-file problem inside Delta Lake.

---
*VinUniversity AICB · Day 18 Track 2 · 2026-05-04*
