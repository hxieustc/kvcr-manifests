#!/usr/bin/env python3
"""Observation-only pod sampler; stdlib only. See README.md for raw schema."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import math
from pathlib import Path
import signal
import subprocess
import threading
import time
from urllib.request import urlopen


def finite_number(value):
    try:
        value = float(value)
        return value if math.isfinite(value) else None
    except (ValueError, TypeError):
        return None


def scrape(endpoint, timeout):
    start = time.time_ns()
    row = dict(kind='prometheus', timestamp_ns=start, endpoint=endpoint)
    try:
        with urlopen(endpoint, timeout=timeout) as response:
            raw = response.read()
            row.update(http_status=response.status, text=raw.decode('utf-8', errors='replace'),
                       sha256=hashlib.sha256(raw).hexdigest())
    except Exception as error:
        row['error'] = str(error)
    row['end_timestamp_ns'] = time.time_ns()
    return row


def ib_sample(root):
    rows = []
    for port in sorted(root.glob('*/ports/*')):
        row = dict(kind='ib', timestamp_ns=time.time_ns(), device=port.parent.parent.name,
                   port=port.name, counters={}, counter_units='port_*_data are 4-byte words')
        for name in ('port_xmit_data', 'port_rcv_data', 'port_xmit_packets', 'port_rcv_packets',
                     'port_xmit_wait', 'port_rcv_errors', 'port_xmit_discards'):
            try:
                row['counters'][name] = int((port/'counters'/name).read_text().strip())
            except (OSError, ValueError) as error:
                row.setdefault('errors', {})[name] = str(error)
        for name in ('rate', 'state', 'phys_state'):
            try:
                row[name] = (port/name).read_text().strip()
            except OSError as error:
                row.setdefault('errors', {})[name] = str(error)
        rows.append(row)
    return rows or [dict(kind='ib', timestamp_ns=time.time_ns(), error='No visible IB ports')]


def gpu_sample(timeout):
    fields = ('index', 'uuid', 'utilization.gpu', 'utilization.memory', 'memory.used',
              'memory.total', 'power.draw', 'temperature.gpu')
    row = dict(kind='gpu', timestamp_ns=time.time_ns(), fields=list(fields),
               units=['index', 'uuid', 'percent', 'percent', 'MiB', 'MiB', 'W', 'C'])
    try:
        result = subprocess.run(['nvidia-smi', '--query-gpu='+','.join(fields),
                                 '--format=csv,noheader,nounits'], capture_output=True,
                                text=True, timeout=timeout, check=False)
        row.update(returncode=result.returncode, raw_stdout=result.stdout, raw_stderr=result.stderr)
        row['gpus'] = [dict(zip(fields, [parts[0], parts[1]] +
                               [finite_number(v) for v in parts[2:]]))
                       for line in result.stdout.splitlines()
                       if len(parts := [v.strip() for v in line.split(',')]) == len(fields)]
    except (OSError, subprocess.TimeoutExpired) as error:
        row['error'] = str(error)
    row['end_timestamp_ns'] = time.time_ns()
    return row


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output', type=Path, required=True, help='append-only raw JSONL path')
    p.add_argument('--endpoint', action='append', help='repeat per worker; default http://127.0.0.1:9090/metrics')
    p.add_argument('--interval', type=float, default=5)
    p.add_argument('--timeout', type=float, default=3)
    p.add_argument('--driver-pid', type=int, help='take final sample when this PID disappears')
    p.add_argument('--ib-root', type=Path, default=Path('/sys/class/infiniband'))
    p.add_argument('--no-gpu', action='store_true')
    p.add_argument('--once', action='store_true')
    args = p.parse_args()
    if args.interval <= 0 or args.timeout <= 0:
        p.error('interval and timeout must be positive')
    endpoints = args.endpoint or ['http://127.0.0.1:9090/metrics']
    # Endpoint configuration is local and credential-free; never persist credentials.
    from urllib.parse import urlsplit
    for endpoint in endpoints:
        url = urlsplit(endpoint)
        if url.scheme not in ('http','https') or url.username or url.password or url.query or url.fragment:
            p.error('endpoints must be HTTP(S) URLs without credentials, queries or fragments')
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('a', buffering=1) as stream, ThreadPoolExecutor(max_workers=len(endpoints)+1) as pool:
        def write(row):
            stream.write(json.dumps(dict(schema_version=1, **row), allow_nan=False)+'\n')
        def sample(final=False):
            futures = [pool.submit(scrape, url, args.timeout) for url in endpoints]
            if not args.no_gpu:
                futures.append(pool.submit(gpu_sample, args.timeout))
            for row in ib_sample(args.ib_root):
                write(dict(row, final=final))
            for future in futures:
                write(dict(future.result(), final=final))
        write(dict(kind='monitor_start', timestamp_ns=time.time_ns(), endpoints=endpoints,
                   interval_seconds=args.interval, driver_pid=args.driver_pid))
        while not stop.is_set():
            started = time.monotonic()
            sample()
            if args.once:
                break
            if args.driver_pid and not Path('/proc', str(args.driver_pid)).exists():
                break
            stop.wait(max(0, args.interval-(time.monotonic()-started)))
        sample(final=True)
        write(dict(kind='monitor_end', timestamp_ns=time.time_ns()))

if __name__ == '__main__':
    main()
