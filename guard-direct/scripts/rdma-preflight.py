"""Verify a real NIXL WRITE between processes using an RC-only data path."""
import ctypes
import json
import mmap
import multiprocessing as mp
import os
from pathlib import Path
import re
import time

SIZE = 4 * 1024**2
CHUNK = SIZE // 4
ALLOCATION = SIZE * 2


def segments(base):
    return [(base+2*i*CHUNK,CHUNK,0) for i in range(4)]


def verified_payload(memory):
    return all(memory[2*i*CHUNK:(2*i+1)*CHUNK]==b'\xa5'*CHUNK
               and memory[(2*i+1)*CHUNK:(2*i+2)*CHUNK]==b'\x00'*CHUNK
               for i in range(4))


def target(connection):
    from nixl import nixl_agent, nixl_agent_config
    memory = mmap.mmap(-1, ALLOCATION)
    address = ctypes.addressof(ctypes.c_char.from_buffer(memory))
    agent = nixl_agent('b200-rdma-preflight-target', nixl_agent_config(
        enable_listen_thread=True, listen_port=0, num_threads=4, capture_telemetry=True))
    registration = agent.register_memory([(address, ALLOCATION, 0, '')], 'DRAM')
    connection.send((agent.get_agent_metadata(), address))
    if not connection.poll(90):
        raise TimeoutError('Source did not finish preflight')
    connection.recv()
    connection.send(verified_payload(memory))
    agent.deregister_memory(registration)
    del agent
    memory.close()


def main():
    assert os.environ.get('NIXL_UCX_SGL_ENABLE') == '1', 'SGL optimization is mandatory'
    assert os.environ['UCX_TLS'] == 'rc,cuda_copy', 'RDMA-only payload transports required'
    assert os.environ.get('UCX_NET_DEVICES') == 'mlx5_0:1'
    assert os.environ.get('UCX_MAX_RMA_RAILS') == '1'
    assert list(Path('/dev/infiniband').glob('uverbs*')), 'RDMA character devices missing'
    from nixl import nixl_agent, nixl_agent_config
    context = mp.get_context('spawn')
    parent, child = context.Pipe()
    process = context.Process(target=target, args=(child,))
    process.start()
    try:
        if not parent.poll(90):
            raise TimeoutError('NIXL receiver initialization failed')
        metadata, destination = parent.recv()
        memory = mmap.mmap(-1, ALLOCATION)
        address = ctypes.addressof(ctypes.c_char.from_buffer(memory))
        ctypes.memset(address, 0xa5, ALLOCATION)
        agent = nixl_agent('b200-rdma-preflight-source', nixl_agent_config(
            enable_listen_thread=True, listen_port=0, num_threads=4, capture_telemetry=True))
        registration = agent.register_memory([(address, ALLOCATION, 0, '')], 'DRAM')
        remote = agent.add_remote_agent(metadata)
        handle = agent.initialize_xfer('WRITE',
            agent.get_xfer_descs(segments(address), 'DRAM'),
            agent.get_xfer_descs(segments(destination), 'DRAM'), remote, backends=['UCX'])
        agent.transfer(handle)
        deadline = time.monotonic() + 60
        while (state := agent.check_xfer_state(handle)) != 'DONE':
            if state not in ('PROC', 'PEND') or time.monotonic() >= deadline:
                raise RuntimeError(f'NIXL transfer failed: {state}')
            time.sleep(.001)
        telemetry = agent.get_xfer_telemetry(handle)
        parent.send('verify')
        assert parent.poll(30) and parent.recv(), 'Destination byte verification failed'
        process.join(30)
        assert process.exitcode == 0, process.exitcode
        # Publish buffered C UCX protocol records before the live log check.
        if ctypes.CDLL(None).fflush(None) != 0:
            raise RuntimeError("Could not flush UCX protocol log")
        lanes_by_pid = {}
        for pid in [os.getpid(), process.pid]:
            logs = (Path(os.environ['ROOT']) / 'logs').glob(f'ucx-*-{pid}.log')
            lanes_by_pid[str(pid)] = [line for path in logs
                for line in path.read_text(errors='replace').splitlines()
                if re.search(r'(?:ep_cfg\[|(?:intra-node|inter-node) cfg#).*\brma\(rc_(mlx5|verbs)/', line)]
        assert all(lanes_by_pid.values()), 'No active RC peer lane for one of the preflight processes'
        lanes = [line for values in lanes_by_pid.values() for line in values]
        proof = {'verified': True, 'ucx_tls': os.environ['UCX_TLS'],
                 'ucx_net_devices': os.environ.get('UCX_NET_DEVICES', 'all'),
                 'topology': 'same-node-two-process-hca-loopback',
                 'payload_bytes': SIZE, 'nixl_total_bytes': int(telemetry.totalBytes),
                 'native_seconds': telemetry.xferDuration / 1e6,
                 'nixl_ucx_sgl_enable': '1', 'sgl_segments': 4, 'noncontiguous': True, 'gap_bytes_verified': SIZE,
                 'destination_verified': True, 'rc_endpoint_lanes': lanes,
                 'source_pid': os.getpid(), 'target_pid': process.pid,
                 'verification_wall_ns': time.time_ns()}
        assert proof['nixl_total_bytes'] == SIZE
        (Path(os.environ['ROOT']) / 'logs/rdma-verification.json').write_text(json.dumps(proof, indent=2) + '\n')
        agent.release_xfer_handle(handle)
        agent.deregister_memory(registration)
        del handle, agent
        memory.close()
        print(json.dumps(proof))
    finally:
        if process.is_alive():
            process.terminate()
            process.join(10)


if __name__ == '__main__':
    main()
