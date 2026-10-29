# Reproduce the Direct Guard AgentX benchmark

This workflow reproduces the **single-RNIC direct Guard r18/r19 setup** using the published KVCR and vLLM implementations. It prepares dependencies and data, verifies real RDMA/GPU transfers, launches eight workers and the Dynamo router, runs AgentX, shuts down services gracefully, collects raw data, and generates reports and plots. Kubernetes pods are retained on success, failure, and interruption.

Start with [the complete reproduction guide](docs/reproducing.md). Consult [pins and provenance](docs/pins.md), [metrics and artifacts](docs/metrics.md), and [troubleshooting](docs/troubleshooting.md) as needed.

## Get the code

Install Google's `repo` tool and configure GitHub SSH access. From an empty directory:

```bash
repo init -u git@github.com:hxieustc/kvcr-manifests.git \
  -b develop -m manifests/kvcr-guard-direct-vllm.xml
repo sync -j4 --no-clone-bundle --no-tags
repo manifest -r > resolved-manifest.xml
```

The workspace contains `manifests/`, `vllm/`, `kvcr/`, `aiperf/`, and `dynamo/`. The four software repositories are locked to exact commits. The manifest repository tracks `develop`; save `resolved-manifest.xml` with each reproduction to freeze the guide and scripts too.

## Run in an allocated pod

The workspace must be accessible **inside** the pod at the path supplied to `--workspace`. The model snapshot must already exist in the read-only shared-model PVC. After following the allocation and bootstrap instructions in the full guide:

```bash
# On the host; detach the supervisor inside the retained pod.
./manifests/guard-direct/run.sh launch \
  --workspace /workspace/guard-direct-code \
  --run-id direct-agentx-r1 \
  --namespace kvcc-mbench --pod guard-direct-agentx
```

Or run the single entry point directly inside the pod:

```bash
/workspace/guard-direct-code/manifests/guard-direct/run.sh all \
  --workspace /workspace/guard-direct-code --run-id direct-agentx-r1
```

The latter stays attached. Use `launch` to survive a terminal disconnect. Preparation requires network access; measured replay uses the prepared dataset/tokenizer offline. Exact historical Dynamo wheels are SHA256-checked; if unavailable from PyPI, supply them with `all --wheelhouse /path/to/wheels` inside the pod.

Results appear at `/workspace/guard-direct-code/guard-direct-runs/direct-agentx-r1/`. Start with `report.md`, `status.json`, `analysis/reports.json`, and `collection.json`. Status lines are printed every five minutes while loading/replaying; GPU and metric sampling runs every five seconds.

## Setup at a glance

| Component | Configuration |
|---|---|
| Model | DeepSeek-V4-Pro-DSpark snapshot `7c09739fd136abfb70a49ec334157f65f45b52cd` |
| Serving | Eight B200 GPUs; DP8, TP1, expert parallel; FP8 KV; MXFP4 indexer; DSpark 6 |
| Cache | Per worker: **0 framework host payload + 124 GiB Guard**; 992 GiB total Guard payload, plus journals/metadata |
| Transfers | `mlx5_0:1`, `UCX_TLS=rc,cuda_copy`, `UCX_MAX_RMA_RAILS=1`, `NIXL_UCX_SGL_ENABLE=1` |
| Router | Dynamo `1.6.0.dev20261014` wheels; KV routing, soft session affinity 3600 s; decode weight 64 |
| Workload | AgentX `inferencex-agentx-mvp`; 393 pinned Weka corpus entries; C72; seed 42 |
| Timing | 10 warmup requests/lane; 3600 s profiling + 300 s drain; warmup grace 1800 s |

This is an AgentX reproduction, not the earlier Kimi/WCE 4x trace replay. Traffic is between workers on **one node using its HCA**; results do not establish cross-node RDMA performance. This folder runs direct Guard only. Original Guard and Mooncake need their own pinned runtimes; historical results are not silently substituted for newly measured data.

## CPU checks

```bash
python3 -m unittest discover -s manifests/guard-direct/tests -v
./manifests/guard-direct/run.sh plan \
  --workspace "$PWD" --run-id inspect-r1 > plan.json
```

These commands do not allocate pods or load a model. GPU correctness gates and the full replay require the documented cluster resources.
