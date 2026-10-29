# Metrics, scope, and artifact layout

The copied analysis code is self-contained under `analysis/`, including its vendor dependencies. It uses AIPerf's actual measured phase timestamps to exclude preparation and warmup. Startup canaries stay under a separate `prepare` phase. No historical results are bundled as if they came from a new run.

## Outputs

```text
guard-direct-runs/RUN_ID/
├── plan.json                         # exact argv, env whitelist, paths, cache settings
├── status.json                       # preparing / starting / replaying / completed / failed
├── supervisor.log                    # detached launch log
├── receipts/                         # source pins, manifests, wheels, overlays, canary proofs
├── frozen-config/                    # input configuration snapshot
├── assets/                           # HF/tokenizer/native AgentX mmap cache + AIPerf venv
├── runtime/                          # private packages, serving venv, image-native links
├── raw/
│   ├── manifest.json
│   ├── monitor-raw.jsonl             # GPU, aggregate/labelled worker/cache metrics, sysfs IB
│   ├── telemetry/rnic-node-exporter.jsonl
│   └── kvcr/phases/
│       ├── prepare/logs/             # correctness probes; excluded from measured population
│       └── measured/
│           ├── logs/                 # Guard, frontend, worker, replay, observers
│           ├── telemetry-health/
│           └── results/guard/
│               ├── option-a-rank-*.json
│               ├── layout-rank-*.json
│               ├── router-geometry-*.json
│               ├── nixl-transfers/   # compact journals, key sidecars, native/source operations
│               └── replay-measured/ # AIPerf profiles, request export, server metrics
├── artifacts.json                    # per-file hashes and sizes
├── artifacts.tar.gz                  # verified raw + receipts + plan/status
├── collection.json                   # archive hash, file count, verification result
├── analysis/{base,operations,direct,rnic}/
│   ├── report.md
│   ├── report.json
│   ├── *.csv
│   └── *.png
└── report.md                         # links to generated reports
```

Runtime packages/native links are intentionally not portable artifacts. Recreate them from the pinned image and source manifest. The raw archive includes hashed preparation receipts and the native dataset cache proof, not the potentially large reusable dataset/model files. Preserve `assets/native-cache-preparation.json` separately if investigating corpus reconstruction; it is also copied into receipts by preparation.

## Measurement perspectives

| Perspective | Evidence and interpretation |
|---|---|
| TTFT, TPOT, throughput | AIPerf measured request/profile exports and aggregate summary; successes/failures, time distributions, measured duration and token counts. Retain failed/timed-out requests rather than selecting only fast successes. |
| Tiered cache hit rates | vLLM request/cache metrics, Direct Guard lookup/request accounting and operation journals. Separate GPU prefix reuse, own Guard and remote Guard returns, unavailable/READY misses, and recomputed tokens where observable. Framework host payload is zero by design. |
| Worker load over time | Timestamped metrics and per-request/rank records, GPU utilization/memory samples. Use worker labels when present; aggregate-only metrics do not prove balanced per-worker service. |
| Cache utilization and hits over time | Guard residency/READY/eviction metrics and GPU cache metrics, with interval deltas for counters. State metric units/denominators; do not equate prompt-token share with cache-query hit rate. |
| Transfer latency/size/bandwidth | Compact/native/source records joined to key sidecars and rank geometry; successful positive-byte measured transfers, plus separate error/zero-byte accounting. CSVs and CDF plots cover bytes, block counts, duration, effective GB/s and size-conditioned summaries. |
| Control overhead | Source-operation stage timestamps, receive→claim→submit→observed DONE→cleanup, progress gaps and CPU timing where emitted. Inclusive stages overlap; do not sum them as disjoint latency. |
| RNIC usage | Five-second whole-node port counter deltas over the measured interval. Directional Tx/Rx rates are context, not exclusively KVCR traffic or application throughput. |

Native NIXL bandwidth uses positive transferred bytes divided by native transfer duration. Source-operation bandwidth uses that operation's payload and larger control-inclusive duration. An unweighted mean of individual GB/s samples is different from total bytes / total duration. Compare identical populations and byte scopes; throughput/TTFT also depend on hit yield, prompt recomputation, queueing, worker load and GPU scheduling.

The RNIC sampler reads visible `/sys/class/infiniband` counters by default. Linux `port_*_data` are four-byte words and are multiplied by four **once at capture**. It writes equivalent byte-counter metric names to the historical `rnic-node-exporter.jsonl` format so the frozen analyzer can consume them. Capture rows explicitly say `kind=sysfs_infiniband`; they are not evidence of a live node-exporter scrape. `monitor-raw.jsonl` separately preserves original word counters. Do not multiply the derived byte series by four again.

Cache evidence has different denominators: GPU prefix-hit tokens, Guard request keys, READY claims, and served remote bytes measure different things. Missing rank data, absent metrics, failed joins, or unclosed telemetry are unavailable evidence, not zero hits or valid fast transfers. Inspect validation/reconciliation fields before publishing a table. For comparisons to Mooncake, exclude its local portion from remote-byte bandwidth; this guide does not generate a new Mooncake comparison on its own.

`completed` means replay exited successfully and closed-capture validation passed. An analyzer's zero exit status means the report generator finished, not that every evidence field is complete or the benchmark passed every scenario-quality check. Read the scenario outcome, warmup count (historically 796), error rate, phase bounds, closure, rank coverage, and reconciliation status in reports/receipts. A failed or incomplete run remains documented as such.
