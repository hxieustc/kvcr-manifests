"""Build the official AgentX native mmap cache before starting GPU services."""
import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import time


def file_hash(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


async def prepare(config, assets, proof_path):
    from aiperf.cli import app
    from aiperf.cli_runner import _make_benchmark_run
    from aiperf.config.flags.resolver import resolve_config
    from aiperf.config.loader import build_benchmark_plan
    from aiperf.config.resolution.resolvers import build_default_resolver_chain
    from aiperf.dataset import mmap_cache
    from aiperf.dataset.dataset_manager import DatasetManager
    from aiperf.dataset.loader.semianalysis_cc_traces_weka import SemiAnalysisCCTracesWekaLoader
    from .commands import replay_command

    assert SemiAnalysisCCTracesWekaLoader.hf_revision == config['dataset_revision']
    args = replay_command(config, assets, assets / 'cache-build/artifacts')
    _, bound, unknown = app.parse_args(args[1:], exit_on_error=False, print_error=False)
    assert not unknown
    plan = build_benchmark_plan(resolve_config(bound.arguments['cli_config'], None))
    assert len(plan.configs) == 1
    run = _make_benchmark_run(plan.configs[0], random_seed=config['random_seed'])
    build_default_resolver_chain().resolve_all(run)
    assert run.resolved.scenario_outcome.submission_valid
    key = mmap_cache.compute_cache_key_from_run(run)
    if key is None:
        raise RuntimeError('The official AgentX workload did not produce a native cache key')
    report = dict(state='building', pid=os.getpid(), started_utc=time.time(),
        cache_key=key, dataset_revision=config['dataset_revision'],
        tokenizer_repository=config['tokenizer_repository'], tokenizer_revision=config['tokenizer_revision'],
        scenario=run.resolved.scenario_outcome.model_dump(mode='json'),
        argv=args, model_server_started=False)
    proof_path.write_text(json.dumps(report, indent=2) + '\n')

    class CacheOnlyDatasetManager(DatasetManager):
        async def publish(self, message):
            print(f'Native notification: {type(message).__name__}', flush=True)

    manager = CacheOnlyDatasetManager(run=run, service_id='agentx_cache_preparation')
    await manager._do_profile_configure(None)
    hit = mmap_cache.lookup(key, compressed=False)
    if hit is None:
        raise RuntimeError('Native dataset preparation completed without a verifiable cache entry')
    if hit.manifest.num_conversations < config['dataset_entries']:
        raise RuntimeError('Reconstructed corpus contains fewer conversations than the source traces')
    warm = CacheOnlyDatasetManager(run=run, service_id='agentx_cache_lookup_verification')
    actual = warm._try_cache_lookup()
    if actual is None or actual.manifest.cache_key != key:
        raise RuntimeError('DatasetManager could not adopt the prepared native cache')
    files = [hit.data_path, hit.index_path, hit.entry_dir / 'manifest.json']
    report.update(state='complete', native_manager_lookup_hit=True, completed_utc=time.time(),
        manifest_version=hit.manifest.version, mmap_format=hit.manifest.mmap_format,
        num_conversations=hit.manifest.num_conversations, total_size_bytes=hit.manifest.total_size_bytes,
        files=[dict(path=str(p.relative_to(assets)), bytes=p.stat().st_size, sha256=file_hash(p)) for p in files])
    proof_path.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({k: report[k] for k in ('state', 'cache_key', 'num_conversations', 'total_size_bytes')}), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--assets', type=Path, required=True)
    args = parser.parse_args()
    assets = args.assets.resolve();assets.mkdir(parents=True, exist_ok=True)
    config = json.loads(args.config.read_text())
    # Set every cache location before importing AIPerf/HF environment settings.
    os.environ.update(HF_HOME=str(assets / 'hf'), HF_HUB_CACHE=str(assets / 'hf/hub'),
        HF_DATASETS_CACHE=str(assets / 'hf/datasets'), HF_MODULES_CACHE=str(assets / 'hf/modules'),
        HF_HUB_DISABLE_XET='1', AIPERF_DATASET_MMAP_CACHE_ENABLED='true',
        AIPERF_DATASET_MMAP_CACHE_DIR=str(assets / 'mmap-cache'),
        AIPERF_DATASET_MMAP_BASE_PATH=str(assets / 'cache-build-mmap'),
        AIPERF_DATASET_WEKA_PARALLEL_WORKERS='16', CUDA_VISIBLE_DEVICES='')
    (assets / 'cache-build-mmap').mkdir(exist_ok=True)
    # Network is permitted during CPU-only preparation; measured runs are offline.
    for name in ('HF_HUB_OFFLINE', 'HF_DATASETS_OFFLINE', 'TRANSFORMERS_OFFLINE'):
        os.environ.pop(name, None)
    asyncio.run(prepare(config, assets, assets / 'native-cache-preparation.json'))


if __name__ == '__main__':
    main()
