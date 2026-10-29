#!/usr/bin/env python3
"""Single-entry Direct Guard AgentX lifecycle. No pod deletion or global cleanup."""
from __future__ import annotations
import argparse
import fcntl
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import subprocess
import sys
import tarfile
import time
import urllib.request

GUIDE = Path(__file__).resolve().parents[1]
CONFIG = GUIDE / 'config'
PINS = json.loads((CONFIG / 'pins.json').read_text())
PREFIX = 'vllm.distributed.kv_transfer.kv_connector.v1.kvcr.guard_direct'


def read(path, default=None):
    return json.loads(Path(path).read_text()) if Path(path).exists() else default


def atomic(path, value):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + f'.{os.getpid()}.partial')
    temp.write_text(json.dumps(value, indent=2) + '\n'); temp.replace(path)


def digest(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def validate_run_id(value):
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,79}', value or ''):
        raise ValueError('Run ID must be 1-80 safe characters, starting with a letter or digit')
    return value


def process_identity(pid):
    text = (Path('/proc') / str(pid) / 'stat').read_text()
    fields = text[text.rfind(')') + 2:].split()
    return dict(pid=pid, pgid=int(fields[2]), start_ticks=int(fields[19]))


def same_process(record):
    try:
        return process_identity(record['pid']) == {k: record[k] for k in ('pid', 'pgid', 'start_ticks')}
    except (FileNotFoundError, ProcessLookupError):
        return False


def stop_owned(record, timeout=1800):
    if record['pid'] <= 1 or record['pid'] == os.getpid() or record['pgid'] != record['pid']:
        raise RuntimeError('Refusing a non-owned process-group identity')
    if not same_process(record):
        raise RuntimeError('Process identity changed; refusing to signal it')
    os.killpg(record['pgid'], signal.SIGTERM)
    deadline = time.monotonic() + timeout
    while same_process(record):
        # Reap only our own child when possible; zombies must not consume the budget.
        try:
            if os.waitpid(record['pid'], os.WNOHANG)[0]:
                return
        except ChildProcessError:
            state = (Path('/proc') / str(record['pid']) / 'stat').read_text().split(') ', 1)[1][0]
            if state == 'Z':
                return
        if time.monotonic() >= deadline:
            raise TimeoutError('Owned process remains live; retained without SIGKILL for native safety')
        time.sleep(.25)


def build_plan(workspace, run_id, model_path=None):
    validate_run_id(run_id); workspace = Path(workspace).resolve()
    config = read(CONFIG / 'experiment.json')
    if model_path:
        config['model_path'] = str(Path(model_path))
    root = workspace / 'guard-direct-runs' / run_id
    result = root / 'raw/kvcr/phases/measured/results/guard'
    token = hashlib.sha256(str(root).encode()).hexdigest()[:12]
    ipc = Path('/dev/shm') / ('gd-' + token)
    serving = root / 'runtime/serving-env/bin/python'
    assets = root / 'assets'
    kv = read(CONFIG / 'kv-transfer.json')
    kv['kv_connector_extra_config']['guard_socket_path'] = str(ipc / 'memory.sock')
    env = {
        'ROOT': str(root / 'raw/kvcr/phases/measured'), 'RUN_DIR': str(result),
        'PYTHONPATH': os.pathsep.join([str(root / 'runtime/python'), str(root / 'runtime/kvcr/src'), str(GUIDE / 'scripts')]),
        'PYTHONHASHSEED': '42', 'PYTHONNOUSERSITE': '1', 'PYTHONDONTWRITEBYTECODE': '1',
        'HF_HOME': str(assets / 'hf'), 'HF_HUB_CACHE': str(assets / 'hf/hub'),
        'HF_DATASETS_CACHE': str(assets / 'hf/datasets'), 'HF_HUB_OFFLINE': '1',
        'HF_DATASETS_OFFLINE': '1', 'TRANSFORMERS_OFFLINE': '1',
        'DYN_REQUEST_PLANE': 'tcp', 'DYN_EVENT_PLANE': 'zmq', 'DYN_DISCOVERY_BACKEND': 'file',
        'DYN_FILE_KV': str(root / 'discovery'), 'DYN_SYSTEM_PORT': '9090',
        'DYN_RUNTIME_NUM_WORKER_THREADS': '8', 'DYN_RUNTIME_MAX_BLOCKING_THREADS': '8',
        'VLLM_USE_V2_MODEL_RUNNER': '1', 'VLLM_USE_RUST_FRONTEND': '1',
        'VLLM_ENGINE_READY_TIMEOUT_S': '7200', 'VLLM_FLOAT32_MATMUL_PRECISION': 'high',
        'TORCH_CUDA_ARCH_LIST': '10.0', 'NCCL_NVLS_ENABLE': '1',
        'VLLM_KV_EVENTS_USE_INT_BLOCK_HASHES': '1', 'VLLM_WORKER_SHUTDOWN_TIMEOUT_SECONDS': '60',
        'VLLM_RPC_BASE_PATH': str(ipc / 'rpc'), 'KVCR_DIRECT_META_DIR': str(ipc / 'rpc'),
        'KVCR_TELEMETRY_DIR': str(result / 'nixl-transfers'),
        'KVCR_TELEMETRY_HEALTH_DIR': str(root / 'raw/kvcr/phases/measured/telemetry-health'),
        'KVCR_SOURCE_MANIFEST': str(root / 'receipts/source-contract.json'),
        'NIXL_UCX_SGL_ENABLE': '1', 'KVCR_NVTX_LEVEL': 'off',
        'UCX_TLS': 'rc,cuda_copy', 'UCX_NET_DEVICES': 'mlx5_0:1', 'UCX_MAX_RMA_RAILS': '1',
        'UCX_PROTO_ENABLE': 'y', 'UCX_PROTO_INFO': 'y', 'UCX_LOG_LEVEL': 'info',
        'UCX_LOG_FILE': str(result / 'ucx-%h-%p.log'), 'UCX_MEMTYPE_CACHE': 'n',
        'UCX_RNDV_THRESH': '4096', 'UCX_ZCOPY_THRESH': '4096', 'UCX_RNDV_SCHEME': 'get_zcopy',
        'UCX_REG_NONBLOCK_MEM_TYPES': '', 'KVCR_UCX_HOST_REGISTRATION': '', 'UCX_REG_NONBLOCK_FALLBACK': 'n',
        'AIPERF_DATASET_MMAP_CACHE_ENABLED': 'true', 'AIPERF_DATASET_MMAP_CACHE_DIR': str(assets / 'mmap-cache'),
        'AIPERF_DATASET_MMAP_BASE_PATH': str(root / 'aiperf-mmap'),
    }
    from agentx_runtime.commands import replay_command, common_environment
    env.update(common_environment(config))
    frontend = [str(serving), '-m', 'dynamo.frontend', '--discovery-backend', 'file',
                '--http-host', '0.0.0.0', '--http-port', '8888', '--model-name', config['model'],
                '--model-path', config['model_path'], '--router-mode', 'kv',
                '--router-session-affinity-ttl-secs', '3600', '--router-session-affinity-mode', 'soft',
                '--router-decode-active-request-weight', '64']
    worker = [str(serving), '-m', 'dynamo.vllm', '--discovery-backend', 'file', '--model', config['model_path']]
    worker += read(CONFIG / 'worker-args.json') + ['--kv-transfer-config', json.dumps(kv)]
    commands = {
        'guard': ['numactl', '--interleave=all', str(serving), '-m', 'kvcr.kvcr_service',
                  '--socket-path', str(ipc / 'memory.sock'), '--pool-dir', str(ipc / 'pools'),
                  '--guard-count', '8', '--pool-sizes-gb', '124',
                  '--compatibility-digest', kv['kv_connector_extra_config']['compatibility_digest']],
        'frontend': ['taskset', '-c', '0-7'] + frontend,
        'worker': worker, 'replay': replay_command(config, assets, result / 'replay-measured'),
        'rnic': [str(serving), str(GUIDE / 'scripts/observe-rnic.py'), '--run', str(root / 'raw'), '--interval', '5'],
        'monitor': [str(serving), str(GUIDE / 'analysis/monitor.py'), '--output', str(root / 'raw/monitor-raw.jsonl'), '--interval', '5'],
    }
    return dict(schema_version=1, run_id=run_id, workspace=str(workspace), root=str(root), result=str(result),
                ipc=str(ipc), assets=str(assets), experiment=config, kv_transfer=kv, environment=env,
                commands=commands, topology='eight workers on one node; cross-worker HCA loopback, not off-node RDMA')


def verify_sources(workspace):
    rows = {}
    for name, expected in PINS['sources'].items():
        path = Path(workspace) / name
        actual = subprocess.check_output(['git', '-C', str(path), 'rev-parse', 'HEAD'], text=True).strip()
        dirty = subprocess.check_output(['git', '-C', str(path), 'status', '--porcelain', '--untracked-files=no'], text=True)
        if actual != expected['revision'] or dirty:
            raise RuntimeError(f'{name}: source pin/cleanliness mismatch: {actual}; expected {expected["revision"]}')
        rows[name] = dict(commit=actual, source_role=expected.get('role', 'runtime_source'))
    return rows


def render_pod(name, namespace, rdma_resource, node, workspace_pvc='kv-p2p-workspace', model_pvc='shared-model-cache'):
    pod = read(CONFIG / 'pod-template.json')
    pod['metadata'].update(name=name, namespace=namespace)
    pod['spec']['volumes'][1]['persistentVolumeClaim']['claimName'] = workspace_pvc
    pod['spec']['volumes'][2]['persistentVolumeClaim']['claimName'] = model_pvc
    if node:
        pod['spec']['nodeSelector']['kubernetes.io/hostname'] = node
    if rdma_resource:
        for field in ('requests', 'limits'):
            pod['spec']['containers'][0]['resources'][field][rdma_resource] = '1'
    return pod


def kubectl(args, **kwargs):
    return subprocess.run(['tsh', 'kubectl', *args], check=True, **kwargs)


def pod_action(args):
    if not args.adopt and not args.rdma_resource and not args.hostpath_rdma:
        raise ValueError('Choose --rdma-resource CLUSTER_RESOURCE or explicitly --hostpath-rdma')
    if args.render_only:
        if args.adopt:
            raise ValueError('--render-only and --adopt are mutually exclusive')
        print(json.dumps(render_pod(args.pod, args.namespace, args.rdma_resource,
                                   args.node, args.workspace_pvc, args.model_pvc), indent=2))
        return
    prefix = (['--context', args.context] if args.context else []) + ['-n', args.namespace]
    query = subprocess.run(['tsh', 'kubectl', *prefix, 'get', 'pod', args.pod, '-o', 'json', '--ignore-not-found'], capture_output=True, text=True, check=True)
    existing = json.loads(query.stdout) if query.stdout.strip() else None
    if args.adopt:
        if existing is None:
            raise ValueError('Requested retained pod does not exist')
        pod = existing
    else:
        if existing is not None:
            raise FileExistsError('Pod exists; use --adopt to reuse it. Existing pod was not modified.')
        pod = render_pod(args.pod, args.namespace, args.rdma_resource, args.node, args.workspace_pvc, args.model_pvc)
        kubectl([*prefix, 'apply', '-f', '-'], input=json.dumps(pod), text=True)
    atomic(args.receipt, dict(pod=pod, adopted=args.adopt, retained=True))
    print(f'Pod retained. Inspect with: tsh kubectl -n {args.namespace} get pod {args.pod} -o wide')


def environment(plan):
    env = dict(os.environ); env.update(plan['environment'])
    for key in ('NATS_SERVER', 'ETCD_ENDPOINTS'):
        env.pop(key, None)
    contract = read(Path(plan['root']) / 'receipts/source-contract.json')
    if contract:
        env['KVCR_SOURCE_ARCHIVE_SHA256'] = contract['source_archive_sha256']
    return env


def preflight(plan, hardware=True):
    root = Path(plan['root']); receipts = root / 'receipts'; receipts.mkdir(parents=True, exist_ok=True)
    sources = verify_sources(plan['workspace'])
    model = Path(plan['experiment']['model_path'])
    if not model.is_dir() or not (model / 'config.json').is_file():
        raise FileNotFoundError(f'Model snapshot missing: {model}')
    if not hardware:
        atomic(receipts / 'source-preflight.json', sources); return
    for port in [8888, 9090, *range(17771, 17779)]:
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', port))
    inventory = subprocess.check_output(['nvidia-smi', '--query-gpu=index,name', '--format=csv,noheader'], text=True, timeout=30)
    gpus = [line for line in inventory.splitlines() if line.strip()]
    if len(gpus) != 8 or any('B200' not in row for row in gpus):
        raise RuntimeError('Exactly eight visible B200 GPUs are required')
    processes = subprocess.check_output(['nvidia-smi', '--query-compute-apps=pid', '--format=csv,noheader,nounits'], text=True, timeout=30)
    if any(line.strip().isdigit() for line in processes.splitlines()):
        raise RuntimeError('GPU compute processes already exist; refuse to clean another workload')
    state = Path('/sys/class/infiniband/mlx5_0/ports/1/state')
    if not state.exists() or 'ACTIVE' not in state.read_text().upper() or not list(Path('/dev/infiniband').glob('uverbs*')):
        raise RuntimeError('mlx5_0:1 is not ACTIVE or RDMA character devices are missing')
    mem = {line.split(':')[0]: int(line.split()[1]) * 1024 for line in Path('/proc/meminfo').read_text().splitlines() if line.startswith('MemAvailable:')}
    cgroup = Path('/sys/fs/cgroup'); limit = (cgroup / 'memory.max').read_text().strip()
    if limit == 'max':
        raise RuntimeError('A finite container memory budget is required; expose the current cgroup as in the pod template')
    current = int((cgroup / 'memory.current').read_text())
    budget = 992 * 1024**3 + 8 * 100 * 1024**2
    if min(mem['MemAvailable'], int(limit) - current) - budget < 768 * 1024**3:
        raise RuntimeError('Insufficient DRAM: Guard pools/journals require an additional 768 GiB startup headroom')
    atomic(receipts / 'preflight.json', dict(sources=sources, gpu_inventory=gpus, rdma_state=state.read_text(),
                                          container_limit_bytes=int(limit), container_current_bytes=current, pool_and_journal_bytes=budget))


def start_process(plan, role, command=None, env=None):
    root = Path(plan['root']); log = root / 'raw/kvcr/phases/measured/logs' / (role + '.log')
    log.parent.mkdir(parents=True, exist_ok=True)
    with log.open('ab') as stream:
        child = subprocess.Popen(command or plan['commands'][role], env=env or environment(plan),
                                 stdout=stream, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, start_new_session=True)
    record = dict(process_identity(child.pid), role=role, log=str(log))
    atomic(root / 'processes' / (role + '.json'), record)
    return child, record


def wait_ready(plan, children, timeout=7200):
    deadline = time.monotonic() + timeout; result = Path(plan['result']); next_status = 0
    while time.monotonic() < deadline:
        if any(child.poll() is not None for child, _ in children):
            raise RuntimeError('A serving process exited during startup; inspect per-service logs')
        ranks = list(result.glob('option-a-rank-*.json'))
        if len(ranks) == 8:
            proofs = [read(p) for p in ranks]
            if {p['rank'] for p in proofs} != set(range(8)):
                raise RuntimeError('Rank identity mismatch')
            for proof in proofs:
                if (proof.get('primary_host_data_bytes') != 0 or not proof.get('guard_only_sources')
                        or proof.get('opportunistic_query') is not False or proof.get('source_pin_callback_calls') != 0
                        or proof.get('framework_region_mem_types') != ['VRAM']):
                    raise RuntimeError('Direct Guard rank contract failed')
                actual = proof.get('transport', {}).get('after_create', {})
                for view in ('python', 'libc'):
                    for key in ('UCX_TLS', 'UCX_NET_DEVICES', 'UCX_MAX_RMA_RAILS', 'NIXL_UCX_SGL_ENABLE', 'KVCR_NVTX_LEVEL'):
                        if actual.get(view, {}).get(key) != plan['environment'][key]:
                            raise RuntimeError('Actual rank native transport differs from the single-RNIC contract')
                if abs(proof.get('guard_pool_bytes', 0) - 124 * 1024**3) > PINS['runtime']['host_page_bytes']:
                    raise RuntimeError('Actual Guard pool size differs from 124 GiB')
            try:
                with urllib.request.urlopen('http://127.0.0.1:8888/v1/models', timeout=5) as response:
                    if json.load(response).get('data'):
                        atomic(Path(plan['root']) / 'receipts/serving-ready.json', dict(ranks=proofs, verified=True)); return
            except (OSError, ValueError):
                pass
        if time.monotonic() >= next_status:
            print(json.dumps(dict(state='starting_model', rank_proofs=len(ranks), run_id=plan['run_id'])), flush=True)
            next_status = time.monotonic() + 300
        time.sleep(5)
    raise TimeoutError('Model/8-rank readiness timed out; pod and files retained')


def validate_closed(plan):
    root = Path(plan['root']); python = root / 'runtime/serving-env/bin/python'
    subprocess.run([str(python), str(GUIDE / 'scripts/validate-closed.py'),
                    '--root', str(root), '--result', plan['result']], env=environment(plan), check=True)


def benchmark(plan):
    root = Path(plan['root']); root.mkdir(parents=True, exist_ok=True)
    lock = (root / 'supervisor.lock').open('a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    if (root / 'benchmark-started.json').exists():
        raise FileExistsError('This run ID already started; inspect it or use a fresh ID')
    prepared = read(root / 'receipts/prepared.json', {})
    signature = hashlib.sha256(json.dumps(plan, sort_keys=True).encode()).hexdigest()
    if not prepared.get('verified') or prepared.get('plan_sha256') != signature:
        raise RuntimeError('Run prepare first; no model processes were started')
    preflight(plan)
    Path(plan['result']).mkdir(parents=True, exist_ok=False)
    ipc = Path(plan['ipc']); (ipc / 'rpc').mkdir(parents=True, exist_ok=False); (ipc / 'pools').mkdir()
    (root / 'discovery').mkdir(); (root / 'aiperf-mmap').mkdir(exist_ok=True)
    (root / 'raw').mkdir(exist_ok=True)
    atomic(root / 'plan.json', plan); atomic(root / 'raw/manifest.json', dict(experiment=plan['experiment']))
    atomic(root / 'benchmark-started.json', dict(wall_ns=time.time_ns(), supervisor=process_identity(os.getpid())))
    atomic(root / 'status.json', dict(state='starting', run_id=plan['run_id'], pod_retained=True))
    children = []; error = None; exit_code = None
    def interrupted(signum, frame):
        raise InterruptedError(f'Supervisor interrupted by signal {signum}; retaining pod')
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, interrupted)
    try:
        children.append(start_process(plan, 'guard'))
        deadline = time.monotonic() + 1800
        while not (ipc / 'memory.sock').exists():
            if children[0][0].poll() is not None or time.monotonic() > deadline:
                raise RuntimeError('Guard service startup failed')
            time.sleep(1)
        children.append(start_process(plan, 'frontend'))
        children.append(start_process(plan, 'worker'))
        wait_ready(plan, children)
        children.append(start_process(plan, 'monitor'))
        children.append(start_process(plan, 'rnic'))
        replay, replay_id = start_process(plan, 'replay')
        children.append((replay, replay_id)); next_status = 0
        atomic(root / 'status.json', dict(state='replaying', run_id=plan['run_id'], pod_retained=True))
        while replay.poll() is None:
            if any(child.poll() is not None for child, record in children if record['role'] in ('guard', 'worker', 'frontend')):
                raise RuntimeError('Serving process exited during replay')
            if time.monotonic() >= next_status:
                print(json.dumps(dict(state='replaying', run_id=plan['run_id'], artifact_directory=str(Path(plan['result']) / 'replay-measured'))), flush=True)
                next_status = time.monotonic() + 300
            time.sleep(5)
        exit_code = replay.returncode
        if exit_code:
            raise RuntimeError(f'AIPerf exited {exit_code}; raw artifacts retained')
    except BaseException as failure:
        error = f'{type(failure).__name__}: {failure}'
    finally:
        failures = []
        # Stop replay first, then worker (quiesce DMA), frontend, Guard, monitor.
        for role in ('replay', 'worker', 'frontend', 'guard', 'monitor', 'rnic'):
            if role == 'guard' and any(item.startswith('worker:') for item in failures):
                failures.append('guard: retained because worker DMA quiescence is unverified')
                continue
            for child, record in children:
                if record['role'] == role and child.poll() is None:
                    try:
                        stop_owned(record, timeout=plan['experiment']['finalize_timeout_seconds'])
                    except Exception as failure:
                        failures.append(f'{role}: {failure}')
        if failures:
            error = (error or '') + '; shutdown incomplete: ' + '; '.join(failures)
        if error is None:
            try:
                validate_closed(plan)
            except Exception as failure:
                error = 'Capture finalization: ' + str(failure)
        atomic(root / 'status.json', dict(state='failed' if error else 'completed', run_id=plan['run_id'],
                                         error=error, aiperf_exit_code=exit_code, pod_retained=True, wall_ns=time.time_ns()))
    collect_run(root)
    analyze(root)
    if error:
        raise RuntimeError(error)


def collect_run(root):
    root = Path(root).resolve()
    for record in root.glob('processes/*.json'):
        if same_process(read(record)):
            raise RuntimeError('Refusing final collection while owned services remain live')
    if not (root / 'raw').is_dir() or not (root / 'status.json').is_file():
        raise FileNotFoundError('No recorded run/raw data to collect')
    paths = [p for name in ('raw', 'receipts') for p in (root / name).rglob('*') if p.is_file()]
    paths += [p for p in (root / 'plan.json', root / 'status.json') if p.is_file()]
    rows = []
    for path in paths:
        if path.is_symlink() or root not in path.resolve().parents:
            raise ValueError('Artifact symlink/outside-root path refused')
        rows.append(dict(path=str(path.relative_to(root)), bytes=path.stat().st_size, sha256=digest(path)))
    atomic(root / 'artifacts.json', rows)
    target = root / 'artifacts.tar.gz'; temp = root / 'artifacts.tar.gz.partial'
    with tarfile.open(temp, 'w:gz') as archive:
        for row in rows:
            archive.add(root / row['path'], arcname=row['path'], recursive=False)
        archive.add(root / 'artifacts.json', arcname='artifacts.json')
    # Drain the gzip/tar stream and verify every member body, not just headers.
    expected = {row['path']: row['sha256'] for row in rows}
    with tarfile.open(temp, 'r:gz') as archive:
        observed = {}
        for member in archive:
            stream = archive.extractfile(member)
            if stream is None:
                raise ValueError('Unexpected non-file archive member')
            actual = hashlib.file_digest(stream, 'sha256').hexdigest()
            if member.name == 'artifacts.json':
                if actual != digest(root / 'artifacts.json'):
                    raise ValueError('Archive manifest integrity mismatch')
            else:
                observed[member.name] = actual
        if observed != expected:
            raise ValueError('Archive member integrity mismatch')
    with gzip.open(temp, 'rb') as stream:
        while stream.read(8 * 1024**2):
            pass  # Validate the gzip footer/CRC as well as all tar member bodies.
    temp.replace(target)
    atomic(root / 'collection.json', dict(verified=True, file_count=len(rows), archive=str(target), sha256=digest(target)))
    print(json.dumps(read(root / 'collection.json')), flush=True)


def analyze(root):
    root = Path(root).resolve(); python = root / 'runtime/serving-env/bin/python'
    if not python.exists():
        python = Path(sys.executable)
    jobs = [('analyze.py', ['--root', str(root / 'raw'), '--output', str(root / 'analysis/base')])]
    for filename, name in [('analyze-opt.py', 'operations'), ('analyze-direct.py', 'direct'), ('analyze-rnic.py', 'rnic')]:
        jobs.append((filename, ['--root', str(root / 'raw'), '--base', str(root / 'analysis/base'), '--output', str(root / 'analysis' / name)]))
    outcomes = []
    for filename, args in jobs:
        result = subprocess.run([str(python), str(GUIDE / 'analysis' / filename), *args], check=False)
        outcomes.append(dict(analyzer=filename, exit_code=result.returncode))
    atomic(root / 'analysis/outcomes.json', outcomes)
    reports = [dict(name=name, report=read(root / 'analysis' / name / 'report.json')) for name in ('base', 'operations', 'direct', 'rnic')]
    atomic(root / 'analysis/reports.json', reports)
    text = '# Direct Guard AgentX reproduction\n\n'
    text += 'Lifecycle: `' + str(read(root / 'status.json')) + '`\n\n'
    text += '\n'.join(f'- [{name} analysis](analysis/{name}/report.md)' for name in ('base', 'operations', 'direct', 'rnic'))
    text += '\n\nKeep cache-query denominators separate from prompt-token shares. Missing/reconciliation-failed evidence remains unavailable. Same-node HCA transfers are not inter-node bandwidth.\n'
    (root / 'report.md').write_text(text)
    if any(row['exit_code'] for row in outcomes):
        raise RuntimeError('One or more analyzers failed; partial outputs retained')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='action', required=True)
    for name in ('plan', 'preflight', 'prepare', 'run', 'all', 'status', 'stop', 'collect', 'analyze', 'launch'):
        p = sub.add_parser(name)
        p.add_argument('--workspace', type=Path, required=True); p.add_argument('--run-id', required=True)
        p.add_argument('--model-path', type=Path)
        if name in ('prepare', 'all'):
            p.add_argument('--wheelhouse', type=Path); p.add_argument('--reclaim-clean-cache', action='store_true')
        if name == 'launch':
            p.add_argument('--pod', required=True); p.add_argument('--namespace', default='kvcc-mbench'); p.add_argument('--context')
    p = sub.add_parser('pod')
    p.add_argument('--pod', required=True); p.add_argument('--namespace', default='kvcc-mbench'); p.add_argument('--context')
    p.add_argument('--node'); p.add_argument('--rdma-resource'); p.add_argument('--hostpath-rdma', action='store_true')
    p.add_argument('--workspace-pvc', default='kv-p2p-workspace'); p.add_argument('--model-pvc', default='shared-model-cache')
    p.add_argument('--adopt', action='store_true'); p.add_argument('--render-only', action='store_true')
    p.add_argument('--receipt', type=Path, default=Path('guard-direct-pod.json'))
    args = parser.parse_args()
    if args.action == 'pod':
        pod_action(args); return
    plan = build_plan(args.workspace, args.run_id, args.model_path); root = Path(plan['root'])
    if args.action == 'plan':
        print(json.dumps(plan, indent=2)); return
    if args.action == 'launch':
        # Execute only on the selected pod. JSON transports arguments, never shell interpolation.
        command = ['python3', str(args.workspace / 'manifests/guard-direct/scripts/workflow.py'), 'all', '--workspace', str(args.workspace), '--run-id', args.run_id]
        if args.model_path:
            command += ['--model-path', str(args.model_path)]
        payload = dict(command=command, root=str(root))
        code = "import json,subprocess,sys;from pathlib import Path;p=json.loads(sys.argv[1]);sys.path.insert(0,str(Path(p['command'][1]).parent));from workflow import atomic,process_identity,same_process,read;r=Path(p['root']);r.mkdir(parents=True,exist_ok=True);old=read(r/'launcher.json');assert not old or not same_process(old),'Supervisor already running';f=(r/'supervisor.log').open('ab');c=subprocess.Popen(p['command'],stdin=subprocess.DEVNULL,stdout=f,stderr=subprocess.STDOUT,start_new_session=True);atomic(r/'launcher.json',process_identity(c.pid));print(json.dumps({'supervisor_pid':c.pid,'root':str(r)}))"
        prefix = (['--context', args.context] if args.context else []) + ['-n', args.namespace]
        kubectl([*prefix, 'exec', args.pod, '--', 'python3', '-c', code, json.dumps(payload)])
        return
    if args.action == 'preflight':
        preflight(plan); return
    if args.action in ('prepare', 'all'):
        from prepare import prepare
        prepare(plan, wheelhouse=args.wheelhouse, reclaim=args.reclaim_clean_cache)
        if args.action == 'prepare': return
    if args.action in ('run', 'all'):
        benchmark(plan)
    elif args.action == 'status':
        print(json.dumps(dict(status=read(root / 'status.json', {'state': 'not_started'}), processes=[dict(read(p), alive=same_process(read(p))) for p in root.glob('processes/*.json')]), indent=2))
    elif args.action == 'stop':
        state = read(root / 'benchmark-started.json')
        if not state:
            launcher = read(root / 'launcher.json')
            if launcher and same_process(launcher):
                stop_owned(launcher, timeout=plan['experiment']['finalize_timeout_seconds'])
                atomic(root / 'status.json', dict(state='preparation_stopped', pod_retained=True))
                return
        if state and same_process(state['supervisor']):
            # An attached supervisor may share the caller's process group.
            # Signal its verified PID only; its handler drains owned child groups.
            os.kill(state['supervisor']['pid'], signal.SIGTERM)
            print('Graceful shutdown requested; inspect status before collecting. Pod retained.')
        else:
            for role in ('replay', 'worker', 'frontend', 'guard', 'monitor', 'rnic'):
                record = read(root / 'processes' / (role + '.json'))
                if record and same_process(record):
                    stop_owned(record, timeout=plan['experiment']['finalize_timeout_seconds'])
    elif args.action == 'collect':
        collect_run(root)
    elif args.action == 'analyze':
        analyze(root)


if __name__ == '__main__':
    try:
        main()
    except Exception as failure:
        # Preserve an explicit failure status for preparation as well as serving.
        if len(sys.argv)>1 and sys.argv[1] in ('prepare', 'all'):
            try:
                index=sys.argv.index('--workspace'); workspace=Path(sys.argv[index+1])
                index=sys.argv.index('--run-id'); run_id=validate_run_id(sys.argv[index+1])
                target=workspace/'guard-direct-runs'/run_id/'status.json'
                old=read(target,{})
                if old.get('state') not in ('completed','failed'):
                    atomic(target,dict(state='failed',error=str(failure),pod_retained=True))
            except Exception:
                pass
        raise
