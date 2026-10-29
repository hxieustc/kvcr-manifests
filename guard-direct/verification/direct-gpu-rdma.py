#!/usr/bin/env python3
"""Real GPU0 PUT/local GET and paused-caller Guard READY-subset GET to GPU1.

Run only after installing this experiment's KVCR and passing rdma-preflight.py.
No vLLM model or fake NIXL implementation is used by this correctness canary.
"""
import argparse
import hashlib
import importlib.util
import json
import logging
import multiprocessing as mp
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import time
import traceback

BLOCK = 1_228_800
PHYSICAL_BLOCKS = 9
SOURCE_IDS = (1, 6)
LOCAL_GET_IDS = (3, 8)
REMOTE_GET_IDS = (7, 2, 5)
DIGEST = hashlib.sha256(b'direct-gpu-canary:1228800:packed:uint8:v1').hexdigest()


class CanaryStats:
    """Recording sink required by KVCR to enable its real monotonic timers."""
    def __init__(self):
        self.values = {}

    def increase_counter(self, name, value, labelvalues=()):
        key = (name, labelvalues)
        self.values[key] = self.values.get(key, 0) + value

    def set_gauge(self, name, value, labelvalues=()):
        self.values[(name, labelvalues)] = value

    def observe_histogram(self, name, value, labelvalues=()):
        self.increase_counter(name+'_count', 1, labelvalues)
        self.increase_counter(name+'_sum', value, labelvalues)

    def reduce(self):
        return {repr(key): value for key, value in self.values.items()}

    def is_empty(self):
        return not self.values


def validate_observed_samples(rows, source_pid, target_pid):
    sources = [r for r in rows if r.get('event') == 'source_nixl_terminal'
               and r.get('pid') == source_pid and r.get('result') == 'success' and (r.get('bytes') or 0) > 0]
    targets = [r for r in rows if r.get('event') == 'target_terminal'
               and r.get('pid') == target_pid and r.get('status') in ('success', 'partial') and (r.get('bytes') or 0) > 0]
    if len(sources) != 1 or len(targets) != 1:
        raise ValueError('Expected exactly one positive source and target terminal for the byte-verified subset')
    source, target = sources[0], targets[0]
    if any(r.get('bytes') != 2*BLOCK or r.get('blocks') != 2 for r in (source, target)):
        raise ValueError('Observer counted requested payload instead of the actual two-block subset')
    if any(not isinstance(source.get(k), (int, float)) or source[k] <= 0
           for k in ('native_seconds', 'source_operation_seconds')):
        raise ValueError('Missing positive native/source-operation transfer clocks')
    if source.get('source_agent') != 'newguard-canary-source' or source.get('target_agent') != 'newguard-canary-target':
        raise ValueError('Native source/target attribution does not match the canary')
    if source.get('guard_only_sources') is not True or source.get('initial_caller_handoff') != 'absent_by_guard_only_progress_path':
        raise ValueError('Observed transfer did not prove the Guard-only source path')
    if source.get('has_framework_pins') is not False:
        raise ValueError('Framework source pins were observed')
    if any(r.get('completed_indices') != [0, 2] for r in (source, target)):
        raise ValueError('Completion indices differ from the byte-verified READY subset')
    return dict(verified=True, actual_bytes=source['bytes'], actual_blocks=source['blocks'],
                native_seconds=source['native_seconds'], source_operation_seconds=source['source_operation_seconds'],
                source_pid=source_pid, target_pid=target_pid)


class Keys:
    def encode(self, value):
        from kvcr.types import BlockKey
        return BlockKey(int(value).to_bytes(8, 'big'))

    def decode(self, key):
        return int.from_bytes(key, 'big')


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def wait_op(client, operation, seconds=90):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        for handle, entries in client.poll_completed():
            if handle == operation:
                return entries
        time.sleep(.001)
    raise TimeoutError(f'KVCR operation {operation} did not complete')


def validate_local_samples(rows, source_pid):
    local = [r for r in rows if r.get('event') == 'native_transfer_terminal'
             and r.get('pid') == source_pid
             and r.get('source_agent') == 'newguard-canary-source'
             and r.get('target_agent') == 'newguard-canary-source']
    if len(local) != 2:
        raise ValueError('Expected exactly two released local native handles: PUT and GET')
    directions = set()
    for row in local:
        directions.add((row.get('source_mem_type'), row.get('destination_mem_type')))
        if (row.get('native_handle_released') is not True or row.get('status') != 'success'
                or row.get('bytes') != 2*BLOCK or row.get('descriptor_bytes') != 2*BLOCK
                or any(not isinstance(row.get(k), (int, float)) or row[k] <= 0
                       for k in ('native_seconds', 'host_submit_to_release_seconds'))):
            raise ValueError('Local native evidence lacks actual payload, release, or positive clocks')
    if directions != {('VRAM', 'DRAM'), ('DRAM', 'VRAM')}:
        raise ValueError('Expected typed VRAM->DRAM PUT and DRAM->VRAM GET')
    return dict(verified=True, transfers=local, bytes_each=2*BLOCK)


def make_client(name, port, memory, pin_calls, service=None):
    from kvcr import KVCR, KVCRBindings
    from kvcr.config import KVCRBackendConfigs, KVCRConfig, KVCRGuardConfig, RemoteFWDramOptions
    from kvcr.control_channels import ZmqPeerControlChannel
    from kvcr.types import RegionDescriptor

    def forbidden_pin(keys):
        pin_calls.append(('request', [bytes(k).hex() for k in keys]))
        raise AssertionError('Guard-only GET invoked framework source pin')

    def forbidden_release(handle):
        pin_calls.append(('release', str(handle)))
        raise AssertionError('Unexpected framework source-pin release')

    def forbidden_cancel(handle):
        pin_calls.append(('cancel', str(handle)))
        raise AssertionError('Unexpected framework source-pin cancel')

    regions = [RegionDescriptor(addr=memory.data_ptr(), size=BLOCK, count=PHYSICAL_BLOCKS,
                                mem_type='VRAM', device_Id=memory.device.index)]
    if service is None:
        regions.append(RegionDescriptor(addr=memory.data_ptr(), size=BLOCK,
                                       count=PHYSICAL_BLOCKS, label=':piece',
                                       mem_type='VRAM', device_Id=memory.device.index))
    def resilience(error):
        # Any uncertain physical hazard fails this canary. Tensor references
        # remain alive until close/quiescence or child process exit.
        raise RuntimeError(f'Canary native lifecycle failure: {error}')

    return KVCR(
        KVCRConfig(nixl_agent_name=name, pool_layouts=[('', BLOCK)], enable_telemetry=True,
                   operation_timeout_ms=5000, abandon_timeout_ms=15000,
                   nixl_listen_port=0, startup_timeout_seconds=120),
        KVCRBindings(request_pin=forbidden_pin, poll_pin_results=lambda: (),
                     release_pin=forbidden_release, cancel_pin_request=forbidden_cancel,
                     framework_control=ZmqPeerControlChannel('127.0.0.1', port, '127.0.0.1'),
                     key_adapter=Keys(), stats_factory=CanaryStats, on_resilience_event=resilience),
        KVCRBackendConfigs(framework_regions=regions,
                           remote_fw_dram=RemoteFWDramOptions(
                               opportunistic_query=False,
                               guard_only_sources=True, guard_only_source_proof=True)),
        KVCRGuardConfig(kvcr_service_socket_path=service, guard_index=0,
                        compatibility_digest=DIGEST) if service is not None else None,
    )


def region_proof(client, device):
    regions = client._core._memory_regions[0]
    assert regions and all(r.mem_type == 'VRAM' and r.device_Id == device for r in regions.values())
    options = client._core._remote_fw_dram._options
    assert options.guard_only_sources and options.guard_only_source_proof
    assert options.opportunistic_query is False
    return [dict(label=r.label, addr=r.addr, size=r.size, count=r.count,
                 stride=r.stride, mem_type=r.mem_type, device_Id=r.device_Id)
            for r in regions.values()]


def descriptors(client, mapping):
    from kvcr.types import MemoryRef
    return {Keys().encode(key): [MemoryRef(end_point_name=client.config.nixl_agent_name,
                                          element_index=index, label=label)]
            for key, index, label in mapping}


def assert_entries(entries, requested, expected):
    assert set(entries) == {Keys().encode(k) for k in requested}, entries
    observed = [entries[Keys().encode(k)].success for k in requested]
    assert observed == expected, (requested, observed, expected)
    return observed


def compare_gpu(torch, memory, expected, policy):
    # Policy acts only after NIXL terminal completion, before GPU readers.
    policy.complete_reads()
    torch.cuda.synchronize(memory.device)
    assert torch.equal(memory, expected), 'GPU payload or untouched sentinel corrupted'


def source_process(pipe, service, port):
    logging.basicConfig(level=logging.DEBUG)
    client = None
    pin_calls = []
    try:
        import torch
        from vllm.distributed.kv_transfer.kv_connector.v1.kvcr.guard_direct.visibility import GPUVisibility
        torch.cuda.set_device(0)
        policy = GPUVisibility.for_device(0)
        memory = torch.full((PHYSICAL_BLOCKS, BLOCK), 0x5a, dtype=torch.uint8, device='cuda:0')
        torch.cuda.synchronize(0)
        client = make_client('newguard-canary-source', port, memory, pin_calls, service)
        registrations = region_proof(client, 0)
        producer = torch.cuda.Stream(device=0)
        ready_event = torch.cuda.Event()
        with torch.cuda.stream(producer):
            torch.cuda._sleep(10_000_000)
            memory[SOURCE_IDS[0]].fill_(ord('A'))
            memory[SOURCE_IDS[1]].fill_(ord('B'))
            ready_event.record(producer)
        event_pending_observed = not ready_event.query()
        event_wait_started = time.monotonic()
        ready_event.synchronize()  # Producer completion is mandatory before PUT.
        assert ready_event.query()
        event_wait_seconds = time.monotonic()-event_wait_started
        started = time.monotonic()
        entries = wait_op(client, client.deposit(descriptors(client, [
            (1, SOURCE_IDS[0], ''), (2, SOURCE_IDS[1], '')])))
        assert_entries(entries, [1, 2], [True, True])
        put_seconds = time.monotonic()-started
        expected = torch.full_like(memory, 0x5a)
        expected[SOURCE_IDS[0]].fill_(ord('A'))
        expected[SOURCE_IDS[1]].fill_(ord('B'))
        started = time.monotonic()
        entries = wait_op(client, client.deliver(descriptors(client, [
            (1, LOCAL_GET_IDS[0], ''), (2, LOCAL_GET_IDS[1], '')])))
        assert_entries(entries, [1, 2], [True, True])
        expected[LOCAL_GET_IDS[0]].fill_(ord('A'))
        expected[LOCAL_GET_IDS[1]].fill_(ord('B'))
        compare_gpu(torch, memory, expected, policy)
        local_get_seconds = time.monotonic()-started
        assert client._core._local_dram is not None
        with client._core._state_lock:
            pipe.send({'ready': True, 'endpoint': f'tcp://127.0.0.1:{port}',
                       'source_pid': os.getpid(), 'caller_state_lock_held': True})
            if not pipe.poll(150):
                raise TimeoutError('Target did not finish while source caller was paused')
            assert pipe.recv() == 'finish'
            deadline = time.monotonic()+30
            while client._core._local_dram._export_readers():
                if time.monotonic() >= deadline:
                    raise TimeoutError('Native reader cleanup depended on source caller polling')
                time.sleep(.001)
        compare_gpu(torch, memory, expected, policy)
        proof = dict(source_pin_request_calls=len(pin_calls), source_pin_requests=pin_calls,
                     source_pin_callback_calls=len(pin_calls),
                     legacy_source_pin_ops=len(client._core._remote_fw_dram._source_pin_ops),
                     legacy_pending_pin_ops=len(client._core._remote_fw_dram._pending_pin_ops),
                     guard_only_sources=True, opportunistic_query=False, source_pid=os.getpid(),
                     framework_regions=registrations, host_framework_regions=0, device=0,
                     cuda_visibility=policy.proof(),
                     native_readers_released_without_caller_poll=True,
                     source_caller_paused_and_state_lock_held_during_get=True,
                     local_put=dict(bytes=2*BLOCK, physical_ids=SOURCE_IDS, api_seconds=put_seconds),
                     local_get=dict(bytes=2*BLOCK, physical_ids=LOCAL_GET_IDS,
                                    api_seconds=local_get_seconds, gpu_bytes_verified=True,
                                    untouched_sentinels_verified=True),
                     producer_event_completed_before_deposit=True,
                     delayed_kernel_cycles=10_000_000, producer_event_wait_seconds=event_wait_seconds,
                     producer_event_pending_observed=event_pending_observed)
        assert not pin_calls and not proof['legacy_source_pin_ops'] and not proof['legacy_pending_pin_ops']
        client.close()
        assert client._core.is_quiescent()
        client = None
        pipe.send(proof)
    except BaseException:
        pipe.send({'error': traceback.format_exc(), 'pin_requests': pin_calls})
        raise
    finally:
        if client is not None:
            client.close()  # Do not free tensors before native teardown.


def target_process(pipe, source_endpoint, port):
    logging.basicConfig(level=logging.DEBUG)
    client = None
    pin_calls = []
    try:
        import torch
        from vllm.distributed.kv_transfer.kv_connector.v1.kvcr.guard_direct.visibility import GPUVisibility
        torch.cuda.set_device(1)
        policy = GPUVisibility.for_device(1)
        memory = torch.full((PHYSICAL_BLOCKS, BLOCK), 0x5a, dtype=torch.uint8, device='cuda:1')
        client = make_client('newguard-canary-target', port, memory, pin_calls)
        registrations = region_proof(client, 1)
        assert client._core._local_dram is None, 'Target Guard relay is forbidden'
        cases = [('mixed', [1, 3, 2], ['', '', ''], list(REMOTE_GET_IDS), [True, False, True]),
                 ('all_missing', [3], [''], [2], [False]),
                 ('layout_mismatch', [1], [':piece'], [7], [False])]
        proofs = []
        for name, requested, labels, indices, expected_success in cases:
            memory.fill_(0x5a)
            expected = torch.full_like(memory, 0x5a)
            torch.cuda.synchronize(1)  # Finish sentinel writes before native DMA.
            request = 'guard-canary-' + name
            client.submit_hint({'protocol_version': '0.1', 'message_id': request,
                'actions': [{'action_type': 'kv.fetch', 'action_version': '1.0',
                    'payload': {'source_control_endpoint': source_endpoint,
                                'block_hashes': requested}}]}, request)
            blocks = descriptors(client, [(key, index, label) for key, index, label
                                          in zip(requested, indices, labels)])
            started = time.monotonic()
            entries = wait_op(client, client.deliver(blocks, request_id=request))
            observed = assert_entries(entries, requested, expected_success)
            if name == 'mixed':
                expected[REMOTE_GET_IDS[0]].fill_(ord('A'))
                expected[REMOTE_GET_IDS[2]].fill_(ord('B'))
            compare_gpu(torch, memory, expected, policy)
            proofs.append(dict(case=name, requested_keys=requested, physical_ids=indices,
                               expected_success=expected_success, observed_success=observed,
                               bytes_verified=True, untouched_sentinels_verified=True,
                               remote_payload_bytes=sum(expected_success)*BLOCK,
                               api_seconds=time.monotonic()-started))
            client.discard_hint(request)
        assert not pin_calls
        client.close()
        assert client._core.is_quiescent()
        client = None
        pipe.send(dict(target_pid=os.getpid(), cases=proofs, target_pin_request_calls=0,
                       target_pin_callback_calls=0, device=1, framework_regions=registrations,
                       host_framework_regions=0, target_guard_pool=False,
                       opportunistic_query=False, cuda_visibility=policy.proof()))
    except BaseException:
        pipe.send({'error': traceback.format_exc(), 'pin_requests': pin_calls})
        raise
    finally:
        if client is not None:
            client.close()


def receive(pipe, timeout):
    if not pipe.poll(timeout):
        raise TimeoutError('Canary child did not respond')
    result = pipe.recv()
    if result.get('error'):
        raise RuntimeError(result['error'])
    return result


def verify_runtime(package, contract, instrument):
    assert instrument['source_archive_sha256'] == contract['source_archive_sha256']
    assert instrument['source_sha256'] == contract['package_sha256']
    if instrument.get('startup_overlays') != dict(guard_core_startup_timeout_seconds=900,
            claim_socket_timeout_seconds=1200, operation_timeouts_changed=False):
        raise ValueError('Unreviewed startup-only overlay contract')
    assert instrument['custom_patch_sha256'] == contract['custom_patch_sha256']
    assert instrument.get('full_ast_observer_enabled') is False
    assert instrument.get('guard_only_sources_required') is True
    assert set(instrument['patched_sha256']) == {'guard.py', 'guard_protocol.py'}
    expected = dict(contract['package_sha256'], **instrument['patched_sha256'])
    assert instrument['helper_file'] == 'compact_telemetry.py'
    expected[instrument['helper_file']] = instrument['helper_sha256']
    actual = {name: hashlib.sha256((package/name).read_bytes()).hexdigest() for name in expected}
    assert actual == expected, 'Installed runtime source differs from sealed/instrumented source'
    recursive = {str(f.relative_to(package)): hashlib.sha256(f.read_bytes()).hexdigest()
                 for f in sorted(package.rglob('*.py'))}
    assert recursive == expected, 'Unexpected Python source in installed KVCR package'
    return actual


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--source-contract', type=Path, required=True)
    parser.add_argument('--instrumentation-manifest', type=Path, required=True)
    args = parser.parse_args()
    if not __debug__:
        raise RuntimeError('Canary assertions require Python without -O')
    assert os.environ.get('UCX_TLS') == 'rc,cuda_copy'
    assert os.environ.get('UCX_NET_DEVICES') == 'mlx5_0:1'
    assert os.environ.get('UCX_MAX_RMA_RAILS') == '1'
    assert os.environ.get('NIXL_UCX_SGL_ENABLE') == '1'
    contract = json.loads(args.source_contract.read_text())
    instrument = json.loads(args.instrumentation_manifest.read_text())
    package = Path(importlib.util.find_spec('kvcr').origin).parent
    actual = verify_runtime(package, contract, instrument)
    if args.output.exists():
        raise FileExistsError(args.output)
    os.environ['KVCR_SOURCE_ARCHIVE_SHA256'] = contract['source_archive_sha256']
    os.environ['KVCR_SOURCE_MANIFEST'] = str(args.source_contract.resolve())
    os.environ['KVCR_NVTX_LEVEL'] = 'off'
    root = args.output.parent / 'direct-gpu-canary'
    root.mkdir(parents=True, exist_ok=False)
    observer_dir = root/'observer-events'
    observer_dir.mkdir()
    os.environ['KVCR_TELEMETRY_DIR'] = str(observer_dir)
    # Guard control sockets stay on tmpfs even though raw proofs live on PVC.
    socket_root = Path('/dev/shm') / f'kvcr-direct-gpu-canary-{os.getpid()}'
    socket_root.mkdir()
    pool_dir = socket_root / 'pools'
    pool_dir.mkdir()
    service_socket = str(socket_root/'service.sock')
    context = mp.get_context('spawn')
    source = target = service = None
    with (root/'service.log').open('wb') as service_log:
        try:
            service = subprocess.Popen([sys.executable, '-m', 'kvcr.kvcr_service',
                '--socket-path', service_socket, '--pool-dir', str(pool_dir),
                '--guard-count', '1', '--pool-sizes-gb', '0.015625',
                '--compatibility-digest', DIGEST], stdout=service_log,
                stderr=subprocess.STDOUT, start_new_session=True)
            deadline = time.monotonic()+90
            while not Path(service_socket).exists():
                if service.poll() is not None or time.monotonic() >= deadline:
                    raise RuntimeError('Canary Guard service failed to start; inspect service.log')
                time.sleep(.05)
            source_pipe, source_child = context.Pipe()
            source = context.Process(target=source_process, args=(source_child, service_socket, free_port()))
            source.start()
            ready = receive(source_pipe, 120)
            target_pipe, target_child = context.Pipe()
            target = context.Process(target=target_process, args=(target_child, ready['endpoint'], free_port()))
            target.start()
            target_proof = receive(target_pipe, 120)
            target.join(30)
            assert target.exitcode == 0, target.exitcode
            source_pipe.send('finish')
            source_proof = receive(source_pipe, 60)
            source.join(30)
            assert source.exitcode == 0, source.exitcode
            # Joined workers have closed their captures. The service is still alive;
            # copy only the source/target files so an open standby file cannot pass.
            joined = root/'observer-joined'
            joined.mkdir()
            for pid in (source.pid, target.pid):
                files = list(observer_dir.glob(f'source-{pid}-*.jsonl'))
                assert len(files) == 1, files
                shutil.copy2(files[0], joined/files[0].name)
                with files[0].open() as stream:
                    header = json.loads(stream.readline())
                key_file = observer_dir/header['key_journal']
                shutil.copy2(key_file, joined/key_file.name)
            from kvcr.compact_telemetry import validate_capture
            validation = validate_capture(joined, ['newguard-canary-source', 'newguard-canary-target'])
            assert validation['valid'] is True, validation
            rows = []
            for file in joined.glob('source-*.jsonl'):
                with file.open() as stream:
                    rows.extend(json.loads(line) for line in stream)
            observer_proof = validate_observed_samples(rows, source.pid, target.pid)
            local_observer_proof = validate_local_samples(rows, source.pid)
            proof = dict(verified=True, topology='GPU0-Guard-local-and-Guard-to-GPU1-two-process-NIXL', block_bytes=BLOCK,
                         payload_backend='UCX', ucx_tls=os.environ['UCX_TLS'],
                         ucx_net_devices=os.environ['UCX_NET_DEVICES'], nixl_ucx_sgl_enable='1',
                         source=source_proof, target=target_proof, wall_ns=time.time_ns(),
                         source_archive_sha256=contract['source_archive_sha256'],
                         installed_package_verified=True, runtime_package_sha256=actual,
                         observer_capture=observer_proof, local_native_capture=local_observer_proof,
                         capture_validation=validation,
                         visibility_helper_sha256=hashlib.sha256(Path(
                             importlib.util.find_spec('vllm.distributed.kv_transfer.kv_connector.v1.kvcr.guard_direct.visibility').origin).read_bytes()).hexdigest(),
                         scope='Real CUDA semantic canary; native SDK durations are not wire timing or benchmark throughput')
            args.output.write_text(json.dumps(proof, indent=2)+'\n')
            print(json.dumps(proof), flush=True)
        finally:
            for process in [target, source]:
                if process is not None and process.is_alive():
                    process.terminate()
                    process.join(15)
                    if process.is_alive():
                        process.kill()
                        process.join(15)
            if service is not None:
                service.terminate()
                try:
                    service.wait(timeout=60)
                except subprocess.TimeoutExpired:
                    service.kill()
                    service.wait(timeout=15)


if __name__ == '__main__':
    main()
