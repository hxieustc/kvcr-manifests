"""Prepare private runtime packages, verified wheels and AgentX data before GPUs load."""
from __future__ import annotations
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import urllib.request
from workflow import CONFIG, GUIDE, PINS, atomic, digest, environment, preflight, read, verify_sources


def run(command, log, env=None, timeout=None):
    log = Path(log); log.parent.mkdir(parents=True, exist_ok=True)
    print(json.dumps(dict(step=log.stem, state='running', command=command[:3])), flush=True)
    with log.open('ab') as stream:
        result = subprocess.run(command, env=env, stdin=subprocess.DEVNULL,
                                stdout=stream, stderr=subprocess.STDOUT, timeout=timeout, check=False)
    if result.returncode:
        raise RuntimeError(f'Preparation failed ({result.returncode}); inspect {log}')


def wheel_files(directory):
    directory = Path(directory); directory.mkdir(parents=True, exist_ok=True); files = []
    for name, expected in PINS['runtime']['dynamo_wheels'].items():
        target = directory / name
        if not target.exists():
            package = 'ai-dynamo-runtime' if name.startswith('ai_dynamo_runtime-') else 'ai-dynamo'
            url = f'https://pypi.org/pypi/{package}/{PINS["runtime"]["dynamo_version"]}/json'
            with urllib.request.urlopen(url, timeout=60) as response:
                metadata = json.load(response)
            candidates = [row for row in metadata['urls'] if row['filename'] == name and row['digests']['sha256'] == expected]
            if len(candidates) != 1:
                raise RuntimeError('Exact historical wheel is unavailable; provide the hash-matching file in --wheelhouse. No fallback build/latest version is allowed.')
            temp = target.with_suffix('.partial')
            with urllib.request.urlopen(candidates[0]['url'], timeout=180) as source, temp.open('wb') as output:
                shutil.copyfileobj(source, output)
            if digest(temp) != expected:
                raise ValueError('Downloaded wheel checksum mismatch')
            temp.replace(target)
        if digest(target) != expected:
            raise ValueError('Wheel checksum mismatch: ' + str(target))
        files.append(target)
    return files


def private_vllm(workspace, overlay):
    # Keep compiled files from the pinned image; replace only private Python copies.
    spec = importlib.util.find_spec('vllm')
    if spec is None or spec.origin is None:
        raise ImportError('Run preparation inside the pinned vLLM image')
    installed = Path(spec.origin).parent
    overlay = Path(overlay); overlay.mkdir(parents=True, exist_ok=False)
    for path in installed.rglob('*'):
        relative = path.relative_to(installed)
        if '__pycache__' in relative.parts:
            continue
        target = overlay / relative
        if path.is_dir():
            target.mkdir(exist_ok=True)
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.symlink_to(path.resolve())
    changed = []
    for source in (Path(workspace) / 'vllm/vllm').rglob('*.py'):
        if '__pycache__' in source.parts:
            continue
        relative = source.relative_to(Path(workspace) / 'vllm/vllm'); target = overlay / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.is_symlink():target.unlink()
        shutil.copy2(source, target); changed.append(str(relative))
    return dict(installed_package=str(installed), private_package=str(overlay), python_files=len(changed),
                native_extensions='symlinked from the SHA256-pinned serving image; no compiled stack rebuild')


def prepare(plan, wheelhouse=None, reclaim=False):
    root = Path(plan['root']); root.mkdir(parents=True, exist_ok=True)
    lock = (root / 'prepare.lock').open('a'); fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    signature = hashlib.sha256(json.dumps(plan, sort_keys=True).encode()).hexdigest()
    existing = read(root / 'receipts/prepared.json')
    if existing:
        if existing.get('verified') and existing.get('plan_sha256') == signature:
            print('Existing prepared environment matches this exact plan', flush=True); return
        raise ValueError('Prepared inputs differ; use a fresh run ID')
    if (root / 'benchmark-started.json').exists() or (root / 'runtime').exists():
        raise FileExistsError('Partial/previous run retained. Inspect it, then choose a fresh run ID; no automatic cleanup.')
    if sys.version_info[:2] != (3, 12):
        raise RuntimeError('Preparation requires Python 3.12 from the pinned image')
    sources = verify_sources(plan['workspace'])
    preflight(plan)
    from importlib.metadata import version
    for package, expected in [('vllm', PINS['runtime']['vllm_version']), ('nixl', PINS['runtime']['nixl_version'])]:
        if version(package) != expected:
            raise RuntimeError(f'{package} must be {expected}; use the pinned serving image')
    atomic(root / 'status.json', dict(state='preparing', run_id=plan['run_id'], pod_retained=True))
    atomic(root / 'plan.json', plan); atomic(root / 'receipts/source-commits.json', sources)
    receipts = root / 'receipts'; receipts.mkdir(exist_ok=True)
    if (Path(plan['workspace']) / '.repo').exists():
        manifest = subprocess.check_output(['repo', 'manifest', '-r'], cwd=plan['workspace'])
        (receipts / 'resolved-manifest.xml').write_bytes(manifest)
    runtime = root / 'runtime'; runtime.mkdir()
    logs = root / 'raw/kvcr/phases/prepare/logs'; logs.mkdir(parents=True)
    serving = runtime / 'serving-env'
    run([sys.executable, '-m', 'venv', '--system-site-packages', str(serving)], logs / 'serving-venv.log')
    python = serving / 'bin/python'
    wheels = wheel_files(wheelhouse or root / 'wheelhouse')
    run([str(python), '-m', 'pip', 'install', '--ignore-installed', '--no-deps', *map(str, wheels)], logs / 'dynamo-install.log')
    run([str(python), '-m', 'pip', 'install', '-r', str(CONFIG / 'runtime-extra.txt'), '-r', str(GUIDE / 'analysis/requirements.txt')], logs / 'runtime-extras.log')
    kvcr_source = Path(plan['workspace']) / 'kvcr'
    shutil.copytree(kvcr_source, runtime / 'kvcr', ignore=shutil.ignore_patterns('.git', '__pycache__', '*.pyc', '.pytest_cache'))
    run([str(python), '-m', 'pip', 'install', '--no-deps', '-e', str(runtime / 'kvcr')], logs / 'kvcr-install.log')
    overlay = private_vllm(plan['workspace'], runtime / 'python/vllm')
    atomic(receipts / 'vllm-overlay.json', overlay)
    archive = receipts / 'kvcr-source.tar.gz'
    subprocess.run(['git', '-C', str(kvcr_source), 'archive', '--format=tar.gz', 'HEAD', '--output', str(archive)], check=True)
    patch = subprocess.check_output(['git', '-C', str(kvcr_source), 'diff', '34bb11986e096e249c76186cd60e28091f2fcb2e', 'HEAD'])
    (receipts / 'kvcr.patch').write_bytes(patch)
    package = runtime / 'kvcr/src/kvcr'
    contract = dict(upstream_commit='34bb11986e096e249c76186cd60e28091f2fcb2e', source_archive_sha256=digest(archive),
                    custom_patch_sha256=hashlib.sha256(patch).hexdigest(),
                    package_sha256={str(p.relative_to(package)): digest(p) for p in package.rglob('*.py')})
    atomic(receipts / 'source-contract.json', contract)
    # The original startup-only overlay is applied to the private copy, never the synced source.
    bundle = root / 'runtime/bootstrap-bundle/config'; bundle.mkdir(parents=True)
    atomic(bundle / 'source-contract.json', contract)
    env = environment(plan); env['BUNDLE_DIR'] = str(bundle.parent)
    env.update(ROOT=str(logs.parent), RUN_DIR=str(logs.parent / 'results/guard'),
               UCX_LOG_FILE=str(logs / 'ucx-%h-%p.log'),
               KVCR_TELEMETRY_DIR=str(logs.parent / 'results/guard/nixl-transfers'),
               KVCR_TELEMETRY_HEALTH_DIR=str(logs.parent / 'telemetry-health'))
    run([str(python), str(GUIDE / 'scripts/configure-compact-telemetry.py'), str(receipts / 'runtime-overlay.json')], logs / 'startup-overlay.log', env=env)
    (receipts / 'runtime-versions.txt').write_text(subprocess.check_output([str(python), '-m', 'pip', 'freeze'], text=True))
    # Build the exact AgentX source privately so the HF revision hook cannot dirty repo sync.
    assets = Path(plan['assets']); assets.mkdir()
    aiperf = runtime / 'aiperf'
    shutil.copytree(Path(plan['workspace']) / 'aiperf', aiperf, ignore=shutil.ignore_patterns('.git', '__pycache__', '*.pyc', '.venv'))
    run([sys.executable, '-m', 'venv', str(assets / 'aiperf-env')], logs / 'aiperf-venv.log')
    ap = assets / 'aiperf-env/bin/python'
    run([str(ap), '-m', 'pip', 'install', '-r', str(CONFIG / 'aiperf-lock.txt'), '-e', str(aiperf)], logs / 'aiperf-install.log')
    original = Path(plan['workspace']) / 'aiperf/src/aiperf/dataset/loader/semianalysis_cc_traces_weka.py'
    run([str(ap), str(GUIDE / 'scripts/agentx_runtime/pin_aiperf_dataset.py'), '--config', str(CONFIG / 'experiment.json'),
         '--original', str(original), '--output', str(receipts / 'dataset-revision-overlay.json')], logs / 'dataset-pin.log')
    config = root / 'frozen-config'; shutil.copytree(CONFIG, config)
    atomic(config / 'experiment.json', plan['experiment'])
    cache_env = dict(os.environ, PYTHONPATH=str(GUIDE / 'scripts'))
    run([str(ap), '-m', 'agentx_runtime.prepare_cache', '--config', str(config / 'experiment.json'), '--assets', str(assets)], logs / 'agentx-cache.log', env=cache_env)
    shutil.copy2(assets / 'native-cache-preparation.json', receipts / 'native-cache-preparation.json')
    (receipts / 'aiperf-versions.txt').write_text(subprocess.check_output([str(ap), '-m', 'pip', 'freeze'], text=True))
    # Correctness gates precede model allocation. All canary data remains in prepare, not measured.
    run([str(python), str(GUIDE / 'scripts/rdma-preflight.py')], logs / 'rdma-preflight.log', env=env, timeout=300)
    run([str(python), str(GUIDE / 'verification/direct-gpu-rdma.py'), '--output', str(logs / 'direct-gpu-rdma.json'),
         '--source-contract', str(receipts / 'source-contract.json'), '--instrumentation-manifest', str(receipts / 'runtime-overlay.json')], logs / 'direct-gpu-rdma.log', env=env, timeout=1800)
    run([str(python), str(GUIDE / 'verification/direct-adapter-smoke.py'), '--output', str(logs / 'direct-adapter-smoke.json')], logs / 'direct-adapter-smoke.log', env=env, timeout=1800)
    if reclaim:
        env.update(B200_RECLAIM_CLEAN_CACHE='1')
        run([str(python), str(GUIDE / 'scripts/reclaim-clean-cache.py')], logs / 'clean-cache-reclaim.log', env=env)
    atomic(receipts / 'prepared.json', dict(verified=True, plan_sha256=signature, wall_ns=time.time_ns(), sources=sources,
                                           vllm_overlay=overlay, wheel_hashes={p.name: digest(p) for p in wheels}))
    atomic(root / 'status.json', dict(state='prepared', run_id=plan['run_id'], pod_retained=True))
    print('Preparation and native canaries complete; model has not been loaded', flush=True)
