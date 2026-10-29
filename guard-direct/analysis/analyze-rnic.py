#!/usr/bin/env python3
"""Whole-node byte-counter context; never attributed exclusively to KVCR."""
import argparse
from collections import defaultdict
import json,os
from pathlib import Path
import observations as obs
legacy=obs.legacy

def intervals(records,bounds):
    samples=defaultdict(dict)
    for row in records:
        if row.get('error'):continue
        for sample in obs.parse_prometheus(row.get('text','')):
            if sample['metric'] not in ('node_infiniband_port_data_received_bytes_total','node_infiniband_port_data_transmitted_bytes_total'):continue
            labels=sample['labels'];samples[(labels.get('device'),labels.get('port'),sample['metric'])][row['timestamp_ns']]=sample['value']
    result=[]
    for (device,port,metric),values in sorted(samples.items()):
        rows=sorted(values.items())
        for (t0,a),(t1,b) in zip(rows,rows[1:]):
            if bounds is None or not bounds[0]<=t0<t1<=bounds[1]:continue
            valid=legacy.finite(a) and legacy.finite(b);reset=valid and b<a
            size=b-a if valid and not reset else None;seconds=(t1-t0)/1e9
            result.append(dict(device=device,port=port,direction='rx' if 'received' in metric else 'tx',start_ns=t0,end_ns=t1,seconds=seconds,bytes=size,GBps=size/seconds/1e9 if size is not None else None,counter_reset=reset,scope='Whole-node port; background included; NOT process-attributed'))
    return result

def analyze(root,base,output):
    root,base,output=map(lambda p:Path(p).resolve(),(root,base,output))
    if any(output==p or p in output.parents or output in p.parents for p in (root,base)):raise ValueError('Output must be outside inputs')
    bounds=json.loads((base/'report.json').read_text()).get('phase_bounds_ns');records=[];used=[]
    for path in sorted(root.glob('**/telemetry/rnic-node-exporter.jsonl')):
        used.append(path)
        with path.open() as stream:records.extend(json.loads(line) for line in stream)
    rows=intervals(records,bounds);group=defaultdict(list)
    for row in rows:group[(row['device'],row['port'],row['direction'])].append(row)
    summary=[dict(device=k[0],port=k[1],direction=k[2],**legacy.stats(r['GBps'] for r in v),total_bytes=sum(r['bytes'] or 0 for r in v),resets=sum(r['counter_reset'] for r in v)) for k,v in group.items()]
    output.mkdir(parents=True,exist_ok=True);legacy.write_csv(output/'rnic-intervals.csv',rows);legacy.write_csv(output/'rnic-summary.csv',summary)
    legacy.write_json(output/'report.json',dict(scope='Whole-node port context, not exclusive KVCR traffic; byte counters need NO four-byte multiplier',phase_bounds_ns=bounds,summary=summary,errors=[r for r in records if r.get('error')],inputs=[dict(path=str(p),sha256=legacy.digest(p)) for p in used]))
    text='# RNIC usage over time\n\nWhole-node port counters include background traffic. The experiment is constrained to mlx5_0:1; other ports are retained as context. Tx and Rx must not be added and called application throughput.\n\n'+'\n'.join(legacy.table(summary,['device','port','direction','count','mean','p50','p90','p99','total_bytes','resets']))+'\n\n![Whole-node RNIC usage](rnic-usage.png)\n'
    (output/'report.md').write_text(text)
    os.environ.setdefault('MPLCONFIGDIR',str(output/'.mpl-cache'));import matplotlib;matplotlib.use('Agg');import matplotlib.pyplot as plt
    fig,ax=plt.subplots(figsize=(11,4))
    for key,series in group.items():ax.plot([(r['end_ns']-bounds[0])/1e9 for r in series],[r['GBps'] for r in series],label=':'.join(key))
    ax.set(xlabel='Profile seconds',ylabel='Port GB/s',title='Whole-node RNIC traffic (includes background)')
    if group:ax.legend(fontsize=6,ncol=4)
    fig.tight_layout();fig.savefig(output/'rnic-usage.png',dpi=140);plt.close(fig)
if __name__=='__main__':
    p=argparse.ArgumentParser()
    for name in ('root','base','output'):p.add_argument('--'+name,required=True,type=Path)
    a=p.parse_args();analyze(a.root,a.base,a.output)
