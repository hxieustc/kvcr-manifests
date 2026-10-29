# Source, runtime, and data provenance

`../config/pins.json` is the machine-readable lock. `../config/experiment.json` contains workload parameters. The XML manifest and lock are checked together by the CPU tests.

| Repository | Branch/reference | Locked commit |
|---|---|---|
| `hxieustc/vllm` | `harryx/guard-direct` | `239b61aae8ab62114a8543891c769ca6f0d664e0` |
| `ai-dynamo/kvcr` | `harryx/guard-direct` | `0cc2cc94b987cadce6c65eb000b61f425ab1ee7a` |
| `ai-dynamo/aiperf` | Archived AgentX source | `abf55f902cde0a3e8389c452a83ecbf5ba15dea9` |
| `ai-dynamo/dynamo` | Archived quickstart reference | `ea23e5130c1bfbadec477b99449d0cee5598caa0` |

The KVCR branch is based on `34bb11986e096e249c76186cd60e28091f2fcb2e`; vLLM is based on `43b4aaea3e40e20ef53ce2bfecfa1b72060bf2b5`. These are the published implementations of the earlier benchmark changes, using the registered in-tree `KVCRDirectGuardConnector` and `KVCRDirectGuardSpec`. No external `kvcr_direct` module is required.

## Dynamo: source checkout versus measured runtime

The archived experiments recorded Dynamo's quickstart reference and the installed wheel version/checksums, **not a verified wheel build commit**. The manifest therefore checks out the recorded reference for inspection, but the runner installs these exact runtime wheels, not a build of that checkout:

| Wheel | SHA256 |
|---|---|
| `ai_dynamo-1.6.0.dev20261014-py3-none-any.whl` | `fd7f9de21d39ffde8517aa3ddd1b5dcd1cdb9e3b4d73b7da27189373bec46a04` |
| `ai_dynamo_runtime-1.6.0.dev20261014-cp310-abi3-manylinux_2_28_x86_64.whl` | `0036a51f15fa1b7223c61577db77a4b7c9506e816a01501a7d9516759f26efae` |

Preparation fetches only a filename and SHA256 match from PyPI or accepts matching files in `--wheelhouse`. Missing wheels stop preparation. Building a newer router or substituting another wheel creates a different experiment.

## Serving image and private overlays

The pod uses this immutable image:

```text
vllm/vllm-openai:nightly-43b4aaea3e40e20ef53ce2bfecfa1b72060bf2b5@sha256:f8696a5176c1fb61e025b0772505674a3e092c25597eb26117003945b5121e8d
```

The image supplies Python 3.12, vLLM `0.31.1rc1.dev23+g43b4aaea3`, NIXL `1.5.0`, CUDA and compiled extensions. Preparation verifies package versions, creates a run-local virtual environment, retains image native libraries, and overlays Python files from the pinned vLLM checkout into a private package. It installs a private KVCR copy. Synced repositories and image-global packages are not edited.

Two historical startup-only adjustments are applied to that private KVCR copy: Guard core startup timeout 900 seconds and initial claim socket timeout 1200 seconds. Transfer-operation timeouts are unchanged. The runner saves the original source archive, patch against the pinned base, recursive source hashes, and exact overlay hashes in `receipts/`. `config/source-contract.json` is archived reference evidence; the runtime contract is regenerated from the published checkout rather than asserting its archive has the historical tarball checksum.

`config/aiperf-lock.txt` locks the archived AIPerf environment. A private loader overlay fixes its existing HF revision hook without changing corpus reconstruction. The cache builder invokes the pinned AIPerf CLI/config resolver and native dataset manager, verifies scenario validity and cache adoption, and saves file hashes. Auxiliary analysis requirements have minimum versions; their actual resolved versions are recorded with `pip freeze`. This workflow does not claim a completely hermetic Python dependency graph.

## Model and dataset

- Model/tokenizer: `deepseek-ai/DeepSeek-V4-Pro-DSpark`, revision `7c09739fd136abfb70a49ec334157f65f45b52cd`.
- Default model mount: `/models-shared/hub/models--deepseek-ai--DeepSeek-V4-Pro-DSpark/snapshots/7c09739fd136abfb70a49ec334157f65f45b52cd`.
- Dataset: `semianalysisai/cc-traces-weka-062126`, revision `23f152f6f0f9399a85901b89a6458def0ef16729`, all 393 source entries.
- The model is not downloaded by this runner. Use the populated shared-model PVC. `--model-path` can change its mount location without changing the required snapshot contents; apply it consistently to prepare/run/all.

## Historical origin

Configs, AIPerf command construction, preparation hooks, GPU/RDMA canaries, and frozen analysis modules were extracted from `kvcr-directguard-agentx/attempts/runtime-r18`. Local absolute paths, pod IDs, fixed workspace names, and dependency on the original benchmark tree were removed. Canaries now import the published vLLM in-tree package. `config/frozen-analysis-hashes.json` preserves the copied analysis hashes. No historical raw request data or model weights are included in this repository.

Record `repo manifest -r` and the run's receipts when publishing results. The manifest self-project follows `develop`; for a future completely fixed source tree, use the resolved manifest captured with the original run.
