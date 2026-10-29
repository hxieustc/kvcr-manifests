# Allocate, prepare, replay, and collect

## 1. Check prerequisites

Start in a directory outside any other `repo` client: `repo init` searches parent directories and can otherwise reuse an enclosing workspace.

On the host, install Google `repo`, Git, Python 3.11 or newer, and Teleport `tsh`; log in to the appropriate cluster. Configure SSH access to the four source repositories and this manifest repository. Inside the serving image, use its Python 3.12. Do not copy credentials into configs, receipts, images, or result archives.

You need one node with eight free B200 GPUs, RDMA devices, sufficient CPU, and roughly 2 TiB usable container DRAM. The supplied pod requests 120 CPUs, 1800 GiB memory, and eight GPUs; its limits are 192 CPUs and 2000 GiB. `/dev/shm` has a 1750 GiB ceiling, not a reservation. Preparation/serving preflight requires actual headroom for 992 GiB Guard pools, eight 100 MiB journals, and another 768 GiB for loading/registration/runtime. Allow substantial workspace disk space (at least 100 GiB free is recommended); request exports, compact journals, archives, and derived CSVs can each occupy many GiB. The populated `shared-model-cache` PVC is read-only; `kv-p2p-workspace` holds sources and results.

Inspect the cluster rather than changing node labels:

```bash
tsh kubectl config current-context
tsh kubectl -n kvcc-mbench get pvc shared-model-cache kv-p2p-workspace
tsh kubectl get nodes -o wide
tsh kubectl describe node YOUR_B200_NODE
```

Use the namespace/PVC names valid for your cluster. The dedicated KVCR taint is not required. The template has only a GPU `NoSchedule` toleration, no expiry timer, and `restartPolicy: Never`. The reaper opt-out annotation is cluster-specific; operator eviction policies can still apply. The runner never deletes pods.

## 2. Allocate or adopt a pod

First sync the manifest on the host using the command in the README. Render a pod without contacting Kubernetes:

```bash
./manifests/guard-direct/run.sh pod --pod guard-direct-agentx \
  --namespace kvcc-mbench --node YOUR_B200_NODE \
  --rdma-resource YOUR_CLUSTER_RDMA_RESOURCE --render-only > guard-direct-pod.json
```

`YOUR_CLUSTER_RDMA_RESOURCE` must be the allocatable extended resource shown by the node/device plugin (for example, a cluster-specific RDMA resource name), not a guessed name. The template requests one unit and exposes `/dev/infiniband`; verify the selected resource actually makes `mlx5_0:1` available. Where the established cluster setup explicitly permits host-device exposure without an extended resource, replace `--rdma-resource ...` with `--hostpath-rdma`. The latter is an explicit configuration choice, not proof that RDMA works; native transfer gates remain mandatory.

Review the rendered JSON, then run the same command without `--render-only` to allocate. Save a receipt using `--receipt guard-direct-pod-receipt.json`. The command refuses to overwrite an existing pod. To record/reuse a retained one:

```bash
./manifests/guard-direct/run.sh pod --pod guard-direct-agentx \
  --namespace kvcc-mbench --adopt --receipt guard-direct-pod-receipt.json
tsh kubectl -n kvcc-mbench wait --for=condition=Ready pod/guard-direct-agentx --timeout=30m
tsh kubectl -n kvcc-mbench get pod guard-direct-agentx -o wide
```

Adoption does not certify an arbitrary pod's setup: compare its image, limits, mounts, privileges, and device exposure with the rendered template. Preparation checks hardware and package versions, and the transfer probes check the active transport. Keep the actual pod receipt alongside the run artifacts; copy it into the run's `receipts/` before final collection.

## 3. Bootstrap code inside the pod

The template starts an idle retained container. It does not automatically clone private repositories or inject credentials. Enter it:

```bash
tsh kubectl -n kvcc-mbench exec -it guard-direct-agentx -- bash
```

Check that `git`, `ssh`, `numactl`, `taskset`, `nvidia-smi`, `libatomic.so.1`, and `repo` are available. If the image lacks bootstrap tools, an operator can install them inside the retained container:

```bash
apt-get update
apt-get install -y git openssh-client curl numactl util-linux libatomic1 python3-venv
curl -fsSL https://storage.googleapis.com/git-repo-downloads/repo -o /usr/local/bin/repo
chmod 0755 /usr/local/bin/repo
```

Make GitHub SSH credentials available using your organization's normal credential mount/agent mechanism. Do not hard-code a key. Alternatively sync on the host directly onto a PVC directory that is visible at the intended path inside the pod. Absolute paths in a run plan must resolve inside the pod.

Then, inside the pod:

```bash
mkdir -p /workspace/guard-direct-code
cd /workspace/guard-direct-code
repo init -u git@github.com:hxieustc/kvcr-manifests.git \
  -b develop -m manifests/kvcr-guard-direct-vllm.xml
repo sync -j4 --no-clone-bundle --no-tags
repo manifest -r > resolved-manifest.xml
./manifests/guard-direct/run.sh plan --workspace "$PWD" --run-id direct-agentx-r1 > preview.json
./manifests/guard-direct/run.sh preflight --workspace "$PWD" --run-id direct-agentx-r1
```

Source trees must remain at their exact locked commits with no tracked modifications. Preflight checks that eight B200s are visible and idle, required ports are unused, `mlx5_0:1` is ACTIVE, character devices exist, the model is mounted, and actual container DRAM headroom is sufficient. It never kills another job to obtain resources. The container must expose its own finite cgroup v2 memory budget; the template attempts a read-only bind of that cgroup at startup.

## 4. Execute one entry point

From the host, after the pod has the code at the same path:

```bash
./manifests/guard-direct/run.sh launch \
  --workspace /workspace/guard-direct-code --run-id direct-agentx-r1 \
  --namespace kvcc-mbench --pod guard-direct-agentx
```

This detaches `all` inside the selected pod. Do not use the legacy root `scripts/bootstrap.sh` / `build-all.sh`; those target a different environment. Direct Guard reuses compiled components from the pinned image instead of rebuilding Torch/vLLM/CUDA.

For an attached run inside the pod:

```bash
./manifests/guard-direct/run.sh all --workspace "$PWD" --run-id direct-agentx-r1
```

The phases are:

1. Verify source/version pins; create private serving and AIPerf environments; download or verify the exact Dynamo wheels; save source/package receipts.
2. Prepare the complete pinned AgentX corpus/tokenizer through AIPerf's own loader, reconstruction, and native mmap cache; verify a subsequent cache lookup succeeds. This is CPU-only and allows network access.
3. Run noncontiguous NIXL RDMA, Guard DRAM→GPU, and installed vLLM adapter correctness probes. Failed correctness gates prevent model startup. Isolated probe pools are small; the actual serving pool sizes are checked later.
4. Start Guard services, Dynamo frontend, and the eight-worker vLLM deployment. Wait for all eight rank attestations, actual Python/libc transport flags, exact Guard memory, zero framework payload/pin callbacks, and a ready model endpoint. Loading/graph capture can take considerable time.
5. Start five-second GPU/cache/worker/RNIC observations; replay AgentX with its native warmup, then 3600-second measured phase and 300-second grace. Preserve the native session scheduling; this is C72 AgentX, not an open-loop JSONL trace.
6. Stop replay, quiesce serving, then stop Guard/observers. Allow 1800 seconds per finalization step; validate closed compact telemetry captures. Do not use SIGKILL to force an apparently finished run into validity.
7. Hash raw data and receipts, build and verify the complete gzip/tar archive, then run the frozen report generators and plots. Retain all originals and the pod.

To split phases, use `prepare` then `run` with the same workspace, run ID, and optional model path. `prepare --wheelhouse /workspace/wheels` accepts the exact wheel files listed in `pins.json`. `prepare --reclaim-clean-cache` optionally runs the archived, own-cgroup clean-page reclaim helper; it does not drop global host caches. This is off by default.

A verified preparation can be reused with the **same** plan. A started or partial run is never overwritten or automatically cleaned. Choose a new run ID when restarting an interrupted attempt. Run-local tmpfs pools and data remain for inspection; consult an operator before reclaiming failed-run allocations. Source/config changes require a fresh run ID and are a new experiment.

## 5. Inspect or stop

Inside the pod:

```bash
./manifests/guard-direct/run.sh status --workspace "$PWD" --run-id direct-agentx-r1
tail -n 30 guard-direct-runs/direct-agentx-r1/supervisor.log
```

From the host, use `tsh kubectl exec` with that exact entry point/path. Live logs are under `raw/kvcr/phases/measured/logs/`. Status output is intentionally limited to five-minute updates during loading/replay. GPU utilization can be low during checkpoint load, registration, or graph capture; inspect `worker.log` and progress before concluding replay is active.

`run.sh stop --workspace ... --run-id ...` sends SIGTERM only to recorded process groups after checking PID, process-group ID, and `/proc` start ticks. It retains files and the pod. If process identity cannot be verified, it refuses to signal it. A finalization timeout leaves the run invalid and resources retained; inspect before intervening.

## 6. Collect, analyze, and download

Automatic collection follows replay. Re-run these inside the pod after services have stopped if needed:

```bash
./manifests/guard-direct/run.sh collect --workspace "$PWD" --run-id direct-agentx-r1
./manifests/guard-direct/run.sh analyze --workspace "$PWD" --run-id direct-agentx-r1
```

Collection refuses live recorded services, hashes every included file, verifies tar member contents and the gzip CRC, and records the final archive SHA256 in `collection.json`. The archive includes raw data, receipts, plan, status, and `artifacts.json`. Generated `analysis/` and `report.md` remain alongside it; copy these too if you need the rendered reports, or regenerate from raw data.

The source workspace, prepared assets, and run artifacts live on the workspace PVC. To download from a host:

```bash
mkdir -p downloaded-direct-agentx-r1
tsh kubectl -n kvcc-mbench cp \
  guard-direct-agentx:/workspace/guard-direct-code/guard-direct-runs/direct-agentx-r1/artifacts.tar.gz \
  downloaded-direct-agentx-r1/artifacts.tar.gz
tsh kubectl -n kvcc-mbench cp \
  guard-direct-agentx:/workspace/guard-direct-code/guard-direct-runs/direct-agentx-r1/collection.json \
  downloaded-direct-agentx-r1/collection.json
```

Compare `sha256sum` of the downloaded archive with `collection.json` before extraction. Extract into a new directory and retain the manifest/script revision used to generate it. Analysis can run without GPUs using Python 3.11+ and `pip install -r manifests/guard-direct/analysis/requirements.txt`. Use `analyze --workspace /download-parent --run-id ID` with artifacts extracted at `/download-parent/guard-direct-runs/ID/`.
