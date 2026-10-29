# Troubleshooting and safe recovery

## Pod stays Pending

Inspect `tsh kubectl describe pod` and node allocatable GPUs, memory, RDMA resource names, PVC binding, node selectors and existing workloads. The template requests eight GPUs and large DRAM headroom. It does not overwrite node labels. The runner never deletes or replaces a pending/retained pod automatically.

## SSH, Teleport, or network failures

`repo sync` needs GitHub SSH access. Kubernetes control commands require a valid `tsh` session. A detached benchmark inside an already running pod continues without a host terminal session; credential renewal is needed to inspect/control it again, not to keep its local replay alive. Wheel/dataset downloads need network during preparation. Do not persist tokens or private keys in receipts.

## Historical Dynamo wheels unavailable

Supply the two exact files listed in `config/pins.json` through `prepare/all --wheelhouse PATH` inside the pod. A filename match with a different checksum is rejected. The archived Dynamo source reference does not prove the wheel build commit; rebuilding it or using current main is not parity with the earlier tests.

## No GPU utilization / loading count appears to restart

Check `status.json`, `supervisor.log`, and measured `logs/worker.log`. Preparation is CPU-only until correctness probes. Full serving then loads model shards and captures graphs before replay. An apparent decrease in a shard counter can come from another rank's logs; check PID/rank and startup timestamps. The runner does not automatically restart the model. An exited worker stops the run; a new attempt needs a fresh run ID.

## Memory preflight or OOM

Confirm actual cgroup limit/current usage and `/proc/meminfo`, not only the Kubernetes request. The payload budget is 992 GiB plus eight 100 MiB journals; registration, model loading, allocator metadata and compiled kernels require extra headroom. The runner requires 768 GiB beyond pools/journals. Inspect old run tmpfs pools and live owners before reclaiming anything. The script does not shrink Guard pools, increase host cache, kill unrelated workloads, or delete previous files to pass preflight. Such changes would alter the experiment.

The optional `prepare --reclaim-clean-cache` helper acts on the current cgroup's clean page cache. It cannot make live Guard payload disappear. Retained pods/files may still consume memory after an interrupted attempt; preserve evidence and ask an operator to confirm ownership before manual cleanup.

## RDMA/SGL probe fails

Verify `mlx5_0:1` exists and is ACTIVE, `/dev/infiniband/uverbs*` is exposed, driver/UCX/NIXL versions match the image, locked-memory permissions are sufficient, and the allocated RDMA resource covers this HCA. Required native settings are `UCX_TLS=rc,cuda_copy`, `UCX_NET_DEVICES=mlx5_0:1`, `UCX_MAX_RMA_RAILS=1`, and `NIXL_UCX_SGL_ENABLE=1`. The spelling is `ENABLE`, not `ENABLED` or `NIX_UCX`.

Inspect preparation `rdma-verification.json`, canary JSONs and UCX logs. Environment strings alone do not prove RDMA; the probes require verified payloads and RC peer lanes. Serving checks both Python and libc environment attestations after NIXL initialization. Do not substitute TCP to obtain a successful run with the RDMA label.

## Preparation fails or a run ID is already used

Inspect `raw/kvcr/phases/prepare/logs/`. A partially created runtime is retained and not reused implicitly. Correct the environmental cause and choose a new run ID. A fully prepared environment may be reused only with the identical plan. Source pins/cleanliness and run-plan hashes are validated.

## Replay exits, but finalization or collection fails

Model exit and a printed AIPerf summary are not enough to certify closed transfer telemetry. The supervisor waits for graceful shutdown, validates eight-rank compact capture closure, then archives raw data. A finalization timeout marks the run failed and retains processes/data. It does not send SIGKILL to convert the run into a false success.

Use `status` and inspect the process receipts. `stop` verifies identities before signaling. If live services remain, collection refuses a final snapshot. Resolve the outstanding owners and then run `collect` and `analyze`. Recovery collection can still produce useful diagnostics, but must not relabel an interrupted capture as valid.

## Missing cache tier or bandwidth plot

Inspect analyzer exit codes in `analysis/outcomes.json`, report JSON validation fields, raw capture completeness, and Prometheus rank labels. Unsupported/missing metrics stay unavailable. Zero framework host payload is intentional for this direct design; it is not a failed 1 GiB cache allocation. Whole-node RNIC rates include other node traffic. A missing phase window or failed sidecar join cannot support a measured CDF.
