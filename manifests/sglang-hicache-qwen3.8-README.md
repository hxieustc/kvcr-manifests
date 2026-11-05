# Qwen3.8 native HiCache manifests on GB300

Three completed r5 arms; no external linker. Full source SHAs include the shared SGLang/Dynamo compatibility patches. KVCR core is the unchanged tested commit. Moon engine is unmodified; random/local_first is a configuration difference. No unmeasured optimization manifests are invented.

[KVCR reproduction](https://github.com/hxieustc/kvcr-benchmark/blob/faca33f18452a2cb5a8b73f74f09b4b3977fd040/sglang-hicache-qwen3.8/sglang-hicache-qwen3.8-kvcr.md) · [Mooncake reproduction](https://github.com/hxieustc/kvcr-benchmark/blob/faca33f18452a2cb5a8b73f74f09b4b3977fd040/sglang-hicache-qwen3.8/sglang-hicache-qwen3.8-mooncake.md) · [manual component execution](https://github.com/hxieustc/kvcr-benchmark/blob/faca33f18452a2cb5a8b73f74f09b4b3977fd040/sglang-hicache-qwen3.8/manual-running.md).

| Manifest | Case |
|---|---|
| [sglang-hicache-qwen3.8-kvcr.xml](sglang-hicache-qwen3.8-kvcr.xml) | `kvcr` |
| [sglang-hicache-qwen3.8-mooncake.xml](sglang-hicache-qwen3.8-mooncake.xml) | `mooncake-random` |
| [sglang-hicache-qwen3.8-mooncake-random.xml](sglang-hicache-qwen3.8-mooncake-random.xml) | `mooncake-random` |
| [sglang-hicache-qwen3.8-mooncake-local-first.xml](sglang-hicache-qwen3.8-mooncake-local-first.xml) | `mooncake-local-first` |

The default Mooncake manifest is an explicit alias for random. Set the matching CASE in the guide; selecting an XML annotation does not start a backend or choose an allocation.

```bash
repo init -u https://github.com/hxieustc/kvcr-manifests.git \
  -b develop -m manifests/sglang-hicache-qwen3.8-kvcr.xml
repo sync -j4 --no-clone-bundle --no-tags
repo manifest -r > resolved-manifest.xml
```

Source and benchmark projects are immutable SHA pins. The convenience manifest checkout follows develop; retain resolved-manifest.xml for each reproduction. Native package/image/model/trace pins and real qualification fixtures are in the benchmark checkout. Existing manifests/default workflows are unchanged.
