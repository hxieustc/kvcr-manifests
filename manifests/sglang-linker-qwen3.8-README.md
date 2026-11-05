# GB300 Qwen3.8 direct-linker manifests

These manifests restore the completed historical KVCR/Mooncake experiments.
All serving/client source and benchmark scripts/configs are pinned to full commit
SHAs. Required Qwen compatibility and instrumentation patches are committed; the
old clean upstream pins alone would not restore the tested implementation.
The manifest repository's convenience checkout follows `develop`; save
`repo manifest -r` with each run. No default manifest or existing experiment changes.

Read [KVCR reproduction](https://github.com/hxieustc/kvcr-benchmark/blob/0ed96a2dfc02b2829295e7ead834b72946a4699d/sglang-linker-qwen3.8/sglang-linker-qwen3.8-kvcr.md),
[Mooncake reproduction](https://github.com/hxieustc/kvcr-benchmark/blob/0ed96a2dfc02b2829295e7ead834b72946a4699d/sglang-linker-qwen3.8/sglang-linker-qwen3.8-mooncake.md), and
[manual component startup](https://github.com/hxieustc/kvcr-benchmark/blob/0ed96a2dfc02b2829295e7ead834b72946a4699d/sglang-linker-qwen3.8/manual-running.md).

```bash
repo init -u https://github.com/hxieustc/kvcr-manifests.git \
  -b develop -m manifests/sglang-linker-qwen3.8-kvcr.xml
repo sync -j4 --no-clone-bundle --no-tags
repo manifest -r > resolved-manifest.xml
```

| Experiment stage | Manifest |
|---|---|
| `kvcr-baseline` | [sglang-linker-qwen3.8-kvcr.xml](sglang-linker-qwen3.8-kvcr.xml) |
| `kvcr-pool-index-m2261` | [sglang-linker-qwen3.8-kvcr-pool-index-m2261.xml](sglang-linker-qwen3.8-kvcr-pool-index-m2261.xml) |
| `kvcr-m2176` | [sglang-linker-qwen3.8-kvcr-m2176.xml](sglang-linker-qwen3.8-kvcr-m2176.xml) |
| `kvcr-m1536` | [sglang-linker-qwen3.8-kvcr-m1536.xml](sglang-linker-qwen3.8-kvcr-m1536.xml) |
| `kvcr-first-page-negative` | [sglang-linker-qwen3.8-kvcr-first-page-negative.xml](sglang-linker-qwen3.8-kvcr-first-page-negative.xml) |
| `kvcr-incremental-confirmation` | [sglang-linker-qwen3.8-kvcr-incremental-confirmation.xml](sglang-linker-qwen3.8-kvcr-incremental-confirmation.xml) |
| `kvcr-gil-1ms` | [sglang-linker-qwen3.8-kvcr-gil-1ms.xml](sglang-linker-qwen3.8-kvcr-gil-1ms.xml) |
| `kvcr-native-poll-1ms` | [sglang-linker-qwen3.8-kvcr-native-poll-1ms.xml](sglang-linker-qwen3.8-kvcr-native-poll-1ms.xml) |
| `mooncake-baseline` | [sglang-linker-qwen3.8-mooncake.xml](sglang-linker-qwen3.8-mooncake.xml) |
| `mooncake-gil-1ms` | [sglang-linker-qwen3.8-mooncake-gil-1ms.xml](sglang-linker-qwen3.8-mooncake-gil-1ms.xml) |
| `mooncake-random` | [sglang-linker-qwen3.8-mooncake-random.xml](sglang-linker-qwen3.8-mooncake-random.xml) |
| `mooncake-local-first` | [sglang-linker-qwen3.8-mooncake-local-first.xml](sglang-linker-qwen3.8-mooncake-local-first.xml) |

Capacity-only KVCR M2261/M2176/M1536 and Mooncake random/local-first reuse
identical source commits and differ in checked-in per-stage settings.
The Mooncake 1 ms GIL control has a real SGLang delta; it therefore has a separate
shared integration branch. The Mooncake transfer engine itself is unmodified.
No vLLM is used. NIXL/Mooncake/native wheel, image, model and trace pins reside in
the SHA-pinned benchmark checkout; Repo does not download weights or start a test.
