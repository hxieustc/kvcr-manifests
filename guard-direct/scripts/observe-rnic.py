#!/usr/bin/env python3
"""Independent, read-only whole-node RNIC counter capture; values are bytes."""
import argparse
import hashlib
import json
from pathlib import Path
import signal
import threading
import time
from urllib.request import urlopen


def main():
    p=argparse.ArgumentParser();p.add_argument('--run',type=Path,required=True);p.add_argument('--endpoint', help='Optional node-exporter URL; default reads visible sysfs counters');p.add_argument('--interval',type=float,default=10);args=p.parse_args()
    stop=threading.Event()
    for sig in (signal.SIGTERM,signal.SIGINT):signal.signal(sig,lambda *_:stop.set())
    path=args.run/'telemetry/rnic-node-exporter.jsonl';path.parent.mkdir(exist_ok=True)
    with path.open('x',buffering=1) as out:
        while not stop.is_set():
            start=time.time_ns();row=dict(kind='node_exporter_infiniband',timestamp_ns=start,endpoint=args.endpoint,scope='Whole-node visible HCA counters including all workloads; not process-attributed',counter_units='node_infiniband_port_data_*_bytes_total are bytes, no word multiplier')
            try:
                if args.endpoint:
                    with urlopen(args.endpoint,timeout=5) as response:
                        text=response.read().decode();row['http_status']=response.status
                else:
                    lines=[]
                    for port in sorted(Path('/sys/class/infiniband').glob('*/ports/*')):
                        labels=f'device="{port.parent.parent.name}",port="{port.name}"'
                        for counter,metric in [('port_xmit_data','transmitted'),('port_rcv_data','received')]:
                            value=int((port/'counters'/counter).read_text())*4
                            lines.append(f'node_infiniband_port_data_{metric}_bytes_total{{{labels}}} {value}')
                    text='\n'.join(lines)
                    row.update(kind='sysfs_infiniband',endpoint=None,
                               counter_units='sysfs data words converted to bytes once during capture')
                text='\n'.join(line for line in text.splitlines() if line.startswith('node_infiniband_'))+'\n'
                if not text.strip():raise ValueError('No node_infiniband metrics returned')
                row.update(text=text,sha256=hashlib.sha256(text.encode()).hexdigest())
            except Exception as error:row['error']=str(error)
            row['end_timestamp_ns']=time.time_ns();out.write(json.dumps(row)+'\n')
            if (args.run/'driver.exit').exists():break
            stop.wait(max(0,args.interval-(time.time_ns()-start)/1e9))

if __name__=='__main__':main()
