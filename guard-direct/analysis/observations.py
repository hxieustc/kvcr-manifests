#!/usr/bin/env python3
"""Offline Mooncake-only archive analysis; never contacts a pod or edits inputs."""
import argparse
from collections import Counter, defaultdict
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import reference_agentx as ref
from server_metrics_phase import reconcile
legacy = ref.legacy()


def tolerant_reader(function, default):
    """Only parse/read failures become gaps; programming errors still surface."""
    def read(*args):
        import ijson
        try:
            return function(*args)
        except (ValueError, OSError, ijson.JSONError, OverflowError) as error:
            args[-1].append(f'Cannot parse/read {function.__name__}: {error}')
            return default()
    return read


legacy.load_exports=tolerant_reader(legacy.load_exports,lambda:(None,None))
legacy.server_metrics=tolerant_reader(legacy.server_metrics,
    lambda:dict(series=[],cache_rates=[],request_histograms=[],queue_gauges=[]))
legacy.hca_metrics=tolerant_reader(legacy.hca_metrics,
    lambda:dict(available=False,intervals=[],summary=[]))


def discover(root):
    """Accept run, case/pair, backend, measured phase, or results directory."""
    candidates = []
    if (root/'replay-measured').is_dir() or (root/'store-events').is_dir():
        candidates.append(root)
    for mode in ('mooncake','guard'):
        candidates.extend(root.glob('**/phases/measured/results/'+mode))
        for suffix in ('results/'+mode, 'measured/results/'+mode):
            if (root/suffix).is_dir(): candidates.append(root/suffix)
    # Path.resolve collapses retained compatibility symlinks.
    candidates = sorted({p.resolve() for p in candidates if p.is_dir()})
    if len(candidates) > 1:
        raise ValueError('Ambiguous measured roots; pass a specific case: '+', '.join(map(str,candidates)))
    return candidates[0] if candidates else None


def parse_prometheus(text):
    rows = []
    for line in text.splitlines():
        if not line.strip() or line.lstrip().startswith('#'):
            continue
        match = re.match(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})?\s+(\S+)(?:\s+\S+)?(?:\s+#.*)?$', line.strip())
        if not match:
            continue
        name, labels_text, raw = match.groups()
        labels = {}
        for label in re.finditer(r'(\w+)="((?:\\.|[^"\\])*)"', labels_text or ''):
            labels[label[1]] = re.sub(r'\\(\\|"|n)', lambda m: '\n' if m[1]=='n' else m[1], label[2])
        try:
            value = float(raw)
        except ValueError:
            value = None
        rows.append(dict(metric=name, labels=labels, raw_value=raw,
                         value=value if legacy.finite(value) else None))
    return rows


def worker_intervals(points, bounds):
    grouped = defaultdict(dict)
    for row in points:
        grouped[(row['endpoint'], row['metric'], json.dumps(row['labels'],sort_keys=True))][row['timestamp_ns']] = row
    intervals = []
    for (endpoint, metric, labels), by_time in sorted(grouped.items()):
        observations = [by_time[t] for t in sorted(by_time)]
        for left, right in zip(observations, observations[1:]):
            start,end = left['timestamp_ns'],right['timestamp_ns']
            if end<=start or (bounds and not bounds[0]<=start<end<=bounds[1]):
                continue
            a,b = left.get('value'),right.get('value')
            valid = legacy.finite(a) and legacy.finite(b)
            reset = bool(valid and b<a)
            counter = bool(re.search(r'(hits|queries|_total|_count|_sum)$',metric))
            delta = b-a if valid and not reset else None
            intervals.append(dict(endpoint=endpoint, metric=metric, labels=json.loads(labels),
                start_ns=start,end_ns=end,seconds=(end-start)/1e9,value=b,
                delta=delta if counter else None,per_second=delta/((end-start)/1e9) if counter and delta is not None else None,
                counter_reset=reset if counter else False,nonfinite=not valid))
    return intervals, cache_interval_rates(intervals)


def cache_interval_rates(intervals):
    paired = defaultdict(dict)
    for row in intervals:
        for prefix,tier in [('vllm:prefix_cache','GPU prefix'),('vllm:external_prefix_cache','External prefix'),
                            ('vllm:kv_offload_tiering_chunk','Framework tier chunks')]:
            for suffix in ('_hits','_queries'):
                if row['metric']==prefix+suffix:
                    key=(row['endpoint'],json.dumps(row['labels'],sort_keys=True),row['start_ns'],row['end_ns'],tier)
                    paired[key][suffix[1:]]=row['delta']
    rates=[]
    for (endpoint,labels,start,end,tier),counts in sorted(paired.items()):
        hits,queries=counts.get('hits'),counts.get('queries')
        rates.append(dict(endpoint=endpoint,labels=json.loads(labels),start_ns=start,end_ns=end,cache=tier,
            hits=hits,queries=queries,hit_rate=hits/queries if legacy.finite(hits) and legacy.finite(queries) and 0<=hits<=queries and queries>0 else None,
            denominator_unit='chunks' if tier=='Framework tier chunks' else 'tokens'))
    return rates


def sdk_intervals(slices, reconciliation):
    evidence={(r['endpoint'],r['metric'],json.dumps(r['labels'],sort_keys=True)):r
              for r in (reconciliation or {}).get('series',[])}
    grouped=defaultdict(list)
    for row in slices:
        grouped[(row['endpoint'],row['metric'],json.dumps(row['labels'],sort_keys=True))].append(row)
    intervals=[]
    for key,rows in grouped.items():
        proof=evidence.get(key,{})
        offset=(proof.get('warmup_offset') or {}).get('total',0)
        needs_offset=proof.get('method')=='warmup_offset_subtracted'
        for row in sorted(rows,key=lambda r:r['start_ns']):
            seconds=(row['end_ns']-row['start_ns'])/1e9
            item=row['exported_timeslice']
            delta=item.get('total') if row['type']=='counter' and proof.get('verified') else None
            if needs_offset and legacy.finite(delta) and delta!=0:
                delta=delta-offset if legacy.finite(offset) and delta>=offset else None
                needs_offset=False
            intervals.append(dict(endpoint=row['endpoint'],metric=row['metric'],labels=row['labels'],
                start_ns=row['start_ns'],end_ns=row['end_ns'],seconds=seconds,value=item.get('avg'),
                delta=delta,per_second=delta/seconds if legacy.finite(delta) else None,
                counter_reset=None,source='SDK timeslice; phase-reconciled counters, raw gauge averages'))
    return intervals,cache_interval_rates(intervals)


def read_monitor(root, paths, bounds, used, issues):
    points=[];ib=defaultdict(dict);gpus=[];errors=[]
    for path in paths:
        if not path.is_file():
            issues.append(f'Missing monitor capture: {path}')
            continue
        used.add(path)
        for row in legacy.read_jsonl(path,issues):
            stamp=row.get('timestamp_ns')
            if not legacy.finite(stamp):
                issues.append(f'Monitor row without timestamp: {path}:{row["source_line"]}')
                continue
            if row.get('error'):
                errors.append(row)
            if row.get('kind')=='prometheus' and 'text' in row:
                # Keep all timestamps before interval selection so boundary/reset decisions are explicit.
                for sample in parse_prometheus(row['text']):
                    points.append(dict(sample,timestamp_ns=stamp,endpoint=row['endpoint'],source_file=str(path),source_line=row['source_line']))
            elif row.get('kind')=='ib' and row.get('device'):
                ib[(row['device'],row['port'])][stamp]=row
            elif row.get('kind')=='gpu' and (bounds is None or bounds[0]<=stamp<=bounds[1]):
                for gpu in row.get('gpus',[]):
                    gpus.append(dict(timestamp_ns=stamp,**gpu))
    intervals,rates=worker_intervals(points,bounds)
    nic=[]
    for (device,port),by_time in sorted(ib.items()):
        rows=[by_time[t] for t in sorted(by_time)]
        for a,b in zip(rows,rows[1:]):
            t0,t1=a['timestamp_ns'],b['timestamp_ns']
            if t1<=t0 or (bounds and not bounds[0]<=t0<t1<=bounds[1]):
                continue
            match=re.match(r'([\d.]+)\s+Gb/sec',b.get('rate',''))
            capacity=float(match[1])*1e9/8 if match else None
            for counter,direction in [('port_xmit_data','tx'),('port_rcv_data','rx')]:
                v0,v1=a.get('counters',{}).get(counter),b.get('counters',{}).get(counter)
                valid=all(legacy.finite(v) for v in (v0,v1))
                delta=(v1-v0)*4 if valid and v1>=v0 else None
                seconds=(t1-t0)/1e9
                speed=delta/seconds if delta is not None else None
                nic.append(dict(device=device,port=port,direction=direction,start_ns=t0,end_ns=t1,
                    bytes=delta,seconds=seconds,bytes_per_second=speed,counter_reset=bool(valid and v1<v0),
                    link_capacity_bytes_per_second=capacity,link_usage_percent=speed/capacity*100 if speed is not None and capacity else None,
                    scope='whole visible IB port; includes background traffic'))
    return points,intervals,rates,nic,gpus,errors


def export_worker_series(replay, bounds, used, issues):
    path=replay/'server_metrics_export.json'
    if not path.exists():
        return []
    import ijson
    used.add(path);out=[]
    with path.open('rb') as stream:
        for name,metric in ijson.kvitems(stream,'metrics',use_float=True):
            if not any(s in name for s in ('cache','requests_running','requests_waiting','gpu_cache','kv_cache')):
                continue
            for series in metric.get('series',[]):
                for item in series.get('timeslices',[]):
                    start,end=item.get('start_ns'),item.get('end_ns')
                    if start is None or end is None or bounds is None or not bounds[0]<=start<end<=bounds[1]:
                        continue
                    out.append(dict(endpoint=series.get('endpoint_url'),labels=series.get('labels',{}),metric=name,
                        type=metric.get('type'),start_ns=start,end_ns=end,exported_timeslice=item,
                        scope='raw SDK timeslice; counters require phase reconciliation; not independent raw scrape'))
    return out


def event_cache_series(run,bounds,used,issues,bin_seconds=10):
    """Key and token denominators stay separate in timestamp bins per emitting PID."""
    if not run or not bounds:return []
    bins=defaultdict(list);seen=set()
    for path in sorted((run/'store-events').glob('*.jsonl')):
        used.add(path)
        for row in legacy.read_jsonl(path,issues):
            stamp=row.get('wall_ns')
            if row.get('event') not in ('store_exists','external_prefix_accounting') or not legacy.finite(stamp) or not bounds[0]<=stamp<=bounds[1]:continue
            identity=tuple(row.get(k) for k in ('session','pid','sequence'))
            if all(v is not None for v in identity):
                if identity in seen:continue
                seen.add(identity)
            index=int((stamp-bounds[0])//(bin_seconds*1e9))
            bins[(str(row.get('pid')),str(row.get('rank')),index)].append(row)
    out=[]
    for (pid,rank,index),rows in sorted(bins.items()):
        counts=legacy.telemetry().summarize_cache(rows)
        start=int(bounds[0]+index*bin_seconds*1e9)
        for cache,n,d in [('Store keys','positive_keys','valid_checked_keys'),
                           ('External lookup tokens','external_prefix_token_matches','external_prefix_token_queries'),
                           ('GPU cached prompt tokens','gpu_cached_tokens','prompt_tokens'),
                           ('External cached prompt tokens','external_cached_tokens','prompt_tokens')]:
            numerator,denominator=counts[n],counts[d]
            out.append(dict(pid=pid,rank=rank,start_ns=start,end_ns=min(bounds[1],start+int(bin_seconds*1e9)),
                cache=cache,numerator=numerator,denominator=denominator,denominator_field=d,
                rate=numerator/denominator if denominator>0 and 0<=numerator<=denominator else None,
                scope='all captured in-phase events; non-preempted token lookups/admissions; not completed client cohort'))
    return out


def plots(output,requests,samples,intervals,rates,nic,gpus):
    os.environ.setdefault('MPLCONFIGDIR',str(output/'.mpl-cache'))
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    for filename,rows,metrics in [('latency-cdf',requests,('ttft_seconds','tpot_seconds')),
                                  ('transfer-cdf',samples,('bytes','blocks_or_objects','seconds','GBps'))]:
        fig,axes=plt.subplots(1,len(metrics),figsize=(5*len(metrics),4),squeeze=False)
        for ax,metric in zip(axes[0],metrics):
            groups=sorted({r.get('dataset','Completed profiling requests') for r in rows})
            for group in groups:
                values=[r.get(metric) for r in rows if r.get('dataset','Completed profiling requests')==group]
                values=list(legacy.ecdf(values))
                if values:
                    x,y=zip(*values);ax.step(x,y,where='post',label=f'{group} (n={sum(legacy.finite(v) for v in [r.get(metric) for r in rows if r.get("dataset","Completed profiling requests")==group])})')
            ax.set(xlabel=metric,ylabel='CDF',ylim=(0,1),title=metric)
            if ax.lines: ax.legend(fontsize=7)
            else: ax.text(.5,.5,'Unavailable',transform=ax.transAxes,ha='center')
        fig.tight_layout();fig.savefig(output/(filename+'.png'),dpi=140);plt.close(fig)
    series=defaultdict(list)
    identities=sorted({(str(r['endpoint']),json.dumps(r['labels'],sort_keys=True)) for r in intervals+rates})
    identity_names={key:f's{i+1} '+','.join(f'{k}={v}' for k,v in json.loads(key[1]).items()
                    if k in ('rank','dp_rank','engine','tier')) for i,key in enumerate(identities)}
    def identity(row):
        return identity_names[(str(row['endpoint']),json.dumps(row['labels'],sort_keys=True))]
    for row in intervals:
        metric=row['metric']
        if legacy.finite(row['value']):
            if metric in ('vllm:num_requests_running','vllm:num_requests_waiting'):
                series['load '+metric.rsplit(':',1)[-1].replace('num_requests_','')+' '+identity(row)].append((row['end_ns']/1e9,row['value']))
            elif any(k in metric for k in ('cache_usage','kv_cache_usage')):
                series['cache '+metric.rsplit(':',1)[-1]+' '+identity(row)].append((row['end_ns']/1e9,row['value']))
    rate_bins=defaultdict(lambda:[0,0])
    for row in rates:
        if legacy.finite(row['hit_rate']):
            key=('hit '+row['cache']+' '+identity(row),int(row['end_ns']//(30*1e9)))
            rate_bins[key][0]+=row['hits'];rate_bins[key][1]+=row['queries']
    for (name,index),(hits,queries) in rate_bins.items():
        series[name].append(((index+1)*30,hits/queries))
    for row in nic:
        if legacy.finite(row.get('bytes_per_second')):
            stamp=row.get('end_ns',row.get('end_unix',0)*1e9)/1e9
            series['RNIC '+str(row.get('device'))+' '+row['direction']].append((stamp,row['bytes_per_second']/1e9))
    for row in gpus:
        if legacy.finite(row.get('utilization.gpu')):
            series['GPU '+str(row['index'])].append((row['timestamp_ns']/1e9,row['utilization.gpu']))
    origin=min((t for values in series.values() for t,_ in values),default=0)
    fig,axes=plt.subplots(5,1,figsize=(14,19))
    for prefix,ax,label in zip(('load','cache','hit','RNIC','GPU'),axes,('Running/waiting requests','Cache usage (exporter units)','Cache hit fraction (token-weighted 30s bins)','RNIC GB/s (whole port)','GPU utilization %')):
        for name,values in series.items():
            if name.startswith(prefix):
                x,y=zip(*sorted(values));ax.plot([v-origin for v in x],y,label=name,linewidth=.7)
        ax.set(xlabel='Seconds since first plotted observation',ylabel=label)
        if ax.lines: ax.legend(fontsize=6,loc='upper center',bbox_to_anchor=(.5,-.18),ncol=4)
        else: ax.text(.5,.5,'Unavailable: see report',transform=ax.transAxes,ha='center')
    fig.tight_layout();fig.savefig(output/'worker-rnic-timeseries.png',dpi=140);plt.close(fig)


def clean(value):
    if isinstance(value,float) and not math.isfinite(value): return None
    if isinstance(value,dict): return {k:clean(v) for k,v in value.items()}
    if isinstance(value,(tuple,list)): return [clean(v) for v in value]
    return value


def store_memory_series(points, endpoint_pids):
    """Native allocator gauges; pair only identical endpoint/time/labels."""
    definitions={'master_allocated_bytes':('global','allocated_bytes'),
                 'master_total_capacity_bytes':('global','capacity_bytes'),
                 'segment_allocated_bytes':('segment','allocated_bytes'),
                 'segment_total_capacity_bytes':('segment','capacity_bytes')}
    grouped=defaultdict(dict);conflicts=set()
    for point in points:
        if point['metric'] not in definitions:continue
        scope,field=definitions[point['metric']]
        key=(point['endpoint'],point['timestamp_ns'],json.dumps(point['labels'],sort_keys=True),scope)
        if field in grouped[key] and grouped[key][field]!=point['value']:
            conflicts.add((key,field))
        grouped[key][field]=point['value']
    rows=[];missing=invalid=0
    for key,values in sorted(grouped.items()):
        endpoint,stamp,label_text,scope=key;labels=json.loads(label_text)
        used,capacity=values.get('allocated_bytes'),values.get('capacity_bytes')
        finite=legacy.finite(used) and legacy.finite(capacity)
        conflict=any((key,field) in conflicts for field in ('allocated_bytes','capacity_bytes'))
        valid=finite and 0<=used<=capacity and capacity>0 and not conflict
        if not finite:missing+=1
        elif not valid:invalid+=1
        segment=labels.get('segment') if scope=='segment' else None
        rows.append(dict(endpoint=endpoint,timestamp_ns=stamp,labels=labels,scope=scope,segment=segment,
            worker_pid=endpoint_pids.get(segment) if segment else None,
            allocated_bytes=used,capacity_bytes=capacity,
            utilization_fraction=used/capacity if valid else None,
            utilization_percent=100*used/capacity if valid else None,
            valid_utilization=valid,conflicting_samples=conflict,
            definition='native allocated memory bytes / mounted memory capacity; not hit rate or logical KV residency'))
    segments=sorted({r['segment'] for r in rows if r['segment']})
    gaps=[]
    if not rows:gaps.append('No native Store memory gauges captured; include master :9003/metrics endpoint')
    elif not any(r['scope']=='segment' for r in rows):gaps.append('Store occupancy is global only; no per-segment gauges captured')
    if any(r['scope']=='segment' and not r['segment'] for r in rows):gaps.append('Native segment gauge lacks segment label; worker ownership unavailable')
    if any(r['segment'] and r['worker_pid'] is None for r in rows):gaps.append('Some Store segments lack exact TE endpoint-to-PID evidence; per-worker attribution unavailable for those segments')
    if missing:gaps.append(f'{missing} Store snapshots missing finite allocated bytes or matching capacity; no carry-forward or configured denominator used')
    if invalid or conflicts:gaps.append(f'{invalid} invalid Store allocated/capacity pairs; {len(conflicts)} conflicting duplicate fields; utilization unavailable')
    return rows,dict(available=bool(rows),usable_utilization_samples=sum(r['valid_utilization'] for r in rows),
        segments=segments,attributed_worker_pids=sorted({r['worker_pid'] for r in rows if r['worker_pid'] is not None}),
        global_only=bool(rows) and not any(r['scope']=='segment' for r in rows),issues=gaps)


def store_memory_plot(output, rows):
    os.environ.setdefault('MPLCONFIGDIR',str(output/'.mpl-cache'))
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    groups=defaultdict(list)
    for row in rows:groups[(row['endpoint'],row['scope'],json.dumps(row['labels'],sort_keys=True))].append(row)
    fig,axes=plt.subplots(2,1,figsize=(13,9),layout='constrained')
    origin=min((r['timestamp_ns'] for r in rows),default=0)
    for i,((endpoint,scope,labels),samples) in enumerate(sorted(groups.items())):
        samples.sort(key=lambda r:r['timestamp_ns'])
        identity=samples[0]['segment'] or scope
        label=f's{i+1} {identity}'
        if samples[0]['worker_pid'] is not None:label+=f' pid={samples[0]["worker_pid"]}'
        x=[(r['timestamp_ns']-origin)/1e9 for r in samples]
        for field,style in [('allocated_bytes','-'),('capacity_bytes','--')]:
            y=[r[field]/2**30 if legacy.finite(r[field]) else math.nan for r in samples]
            if any(math.isfinite(v) for v in y):axes[0].plot(x,y,style,label=label+' '+field,linewidth=.9)
        y=[r['utilization_percent'] if r['utilization_percent'] is not None else math.nan for r in samples]
        if any(math.isfinite(v) for v in y):axes[1].plot(x,y,label=label,linewidth=.9)
    for ax,ylabel in zip(axes,('Store allocator memory (GiB)','Allocated / mounted capacity (%)')):
        ax.set(xlabel='Seconds since first Store observation',ylabel=ylabel)
        if ax.lines:ax.legend(fontsize=6,ncol=3)
        else:ax.text(.5,.5,'Unavailable: no matched native Store gauges',transform=ax.transAxes,ha='center')
    fig.suptitle('Mooncake Store memory allocation; global and segment series kept separate')
    fig.savefig(output/'store-cache-utilization.png',dpi=140);plt.close(fig)


def analyze(root,output,monitor_paths=(),make_plots=True):
    root=root.resolve();output=output.resolve()
    run=discover(root)
    if output==root or output==run or (run and (run in output.parents or output in run.parents)):
        raise ValueError('Output must be separate from raw results; use a new analysis directory')
    output.mkdir(parents=True,exist_ok=True)
    used=set();issues=[]
    if run:
        # backend_data expects the retained pair topology. A tiny path adapter
        # maps only its exact measured results suffix; all reads remain raw.
        class PairRoot:
            def __truediv__(self,key):
                if key!='mooncake': raise ValueError(key)
                return run.parents[3]
        data=legacy.backend_data(PairRoot(),'mooncake',used,source_directory=run)
        replay=run/'replay-measured'
        issues.extend(data['issues'])
        requests,cohort=ref.profiling_requests(replay/'profile_export.jsonl','mooncake',issues)
        if (replay/'profile_export.jsonl').exists():used.add(replay/'profile_export.jsonl')
        data['requests']=requests
        bounds=data['phase_bounds_ns']
        data['server_metrics']=reconcile(replay/'server_metrics_export.json',data['server_metrics'],bounds,used,issues,
            independent_totals={metric:data['exists'][key] for metric,key in (
                ('vllm:prefix_cache_queries','prompt_tokens'),('vllm:prefix_cache_hits','gpu_cached_tokens'),
                ('vllm:external_prefix_cache_queries','external_prefix_token_queries'),
                ('vllm:external_prefix_cache_hits','external_prefix_token_matches'))
                if data['capture_valid'] is True and data['exists'].get('admitted_requests',0)>0})
        gets,get_issues=ref.mooncake_gets(data,used);issues.extend(get_issues)
        joined,join_coverage,join_issues=ref.request_cache_samples(data,'mooncake',used);issues.extend(join_issues)
        sdk=tolerant_reader(export_worker_series,list)(replay,bounds,used,issues)
    else:
        issues.append('No measured results found; absent phases are not reconstructed')
        data=dict(samples=[],failures=[],request_outcomes={},exists={},coverage=[],capture_valid=None,
                  server_metrics=dict(series=[],cache_rates=[]),hca=dict(intervals=[],summary=[]),endpoint_process_map={})
        requests=[];cohort={};bounds=None;gets=[];joined=[];join_coverage={};sdk=[]
    for request in requests:
        request['ttft_seconds']=ref.request_seconds(request,'time_to_first_token')
        request['tpot_seconds']=ref.request_seconds(request,'time_per_output_token')
    paths=set(Path(p).resolve() for p in monitor_paths)
    paths.update(p.resolve() for p in root.glob('**/monitor-raw*.jsonl') if output not in p.parents)
    points,intervals,rates,nic,gpus,errors=read_monitor(root,sorted(paths),bounds,used,issues)
    sdk_rows,sdk_rates=sdk_intervals(sdk,data['server_metrics'].get('phase_reconciliation'))
    event_series=event_cache_series(run,bounds,used,issues)
    if bounds is None:
        # Monitor data can describe observations, but never measured-phase totals.
        issues.append('No verified phase bounds: monitor series are unscoped observations')
    selected_points=[r for r in points if bounds is None or bounds[0]<=r['timestamp_ns']<=bounds[1]]
    store_memory,store_coverage=store_memory_series(selected_points,data['endpoint_process_map'])
    issues.extend(store_coverage['issues'])
    samples=data['samples']
    summary,cdfs,bins,bin_cdfs,exact=legacy.distributions(samples)
    byte_bins,byte_cdfs=legacy.byte_distributions(samples)
    # Strip the unused empty KVCR population inherited from frozen routines.
    summary,cdfs,bins,bin_cdfs,exact,byte_bins,byte_cdfs=[
        [r for r in rows if r.get('dataset')!='KVCR native']
        for rows in (summary,cdfs,bins,bin_cdfs,exact,byte_bins,byte_cdfs)]
    cache_rows=[]
    exists=data['exists']
    for label,numerator,denominator,unit in [('Store key hits','positive_keys','valid_checked_keys','keys'),
                ('External scheduler lookup','external_prefix_token_matches','external_prefix_token_queries','tokens'),
                ('GPU cached prompt share','gpu_cached_tokens','prompt_tokens','tokens'),
                ('External cached prompt share','external_cached_tokens','prompt_tokens','tokens')]:
        n,d=exists.get(numerator),exists.get(denominator)
        cache_rows.append(dict(cache=label,numerator=n,denominator=d,denominator_field=denominator,unit=unit,
            rate=n/d if legacy.finite(n) and legacy.finite(d) and 0<=n<=d and d>0 else None,
            scope='phase-scoped Store queries or non-preempted scheduler admissions, all outcomes'))
    request_cohorts=[]
    if run and (run/'replay-measured/profile_export.jsonl').exists():
        for row in legacy.read_jsonl(run/'replay-measured/profile_export.jsonl',issues):
            meta=row.get('metadata') or {};metrics=row.get('metrics') or {}
            status='error' if row.get('error') or row.get('error_details') else 'usable_ttft' if legacy.finite(metrics.get('time_to_first_token',{}).get('value')) else 'missing_ttft'
            request_cohorts.append(dict(legacy.reference(row),request_id=meta.get('x_request_id'),phase=meta.get('benchmark_phase'),status=status,error=row.get('error'),error_details=row.get('error_details')))
    tables={'requests':requests,'request-cohorts':request_cohorts,'request-cache-joined':joined,
        'cache-rates':cache_rows,'server-cache-rates':data['server_metrics']['cache_rates'],
        'server-selected-series':data['server_metrics']['series'],'server-sdk-timeslices':sdk,
        'store-get-accounting':gets,'transfer-samples':samples,'transfer-failures':data['failures'],
        'transfer-summary':summary,'transfer-cdf':cdfs,'transfer-object-bins':bins,
        'transfer-object-bin-cdf':bin_cdfs,'transfer-exact-bytes':exact,'transfer-byte-bins':byte_bins,
        'transfer-byte-bin-cdf':byte_cdfs,'worker-samples':selected_points,'worker-intervals':intervals,
        'worker-cache-rates':rates,'rnic-intervals':nic or data['hca']['intervals'],
        'worker-sdk-intervals':sdk_rows,'worker-sdk-cache-rates':sdk_rates,
        'worker-event-cache-rates':event_series,
        'store-memory-occupancy':store_memory,
        'rnic-archive-summary':data['hca']['summary'],'gpu-samples':gpus,'monitor-errors':errors}
    latency_cdf=[]
    for metric in ('ttft_seconds','tpot_seconds'):
        latency_cdf.extend(dict(metric=metric,value=v,cdf=p) for v,p in legacy.ecdf(r[metric] for r in requests))
    tables['latency-cdf']=latency_cdf
    for name,rows in tables.items():legacy.write_csv(output/(name+'.csv'),clean(rows))
    rank_keys={'rank','worker','worker_id','dp_rank','data_parallel_rank','engine','engine_id'}
    identities=sorted({str(r['endpoint'])+' '+json.dumps(r['labels'],sort_keys=True) for r in selected_points+sdk
                       if rank_keys.intersection(r['labels'])})
    if not identities:issues.append('No rank-labeled metrics: endpoint aggregates cannot establish eight-worker utilization')
    if not paths:issues.append('No independent monitor capture; archived SDK timeslices are exported separately')
    if not (nic or data['hca']['intervals']):issues.append('No measured RNIC bandwidth evidence')
    report=dict(scope='Offline Mooncake diagnostic analysis; not a reproduction completion audit',root=str(root),
        source_directory=str(run) if run else None,phase_bounds_ns=bounds,
        status='incomplete' if issues or data['capture_valid'] is not True else 'evidence_available',
        issues=list(dict.fromkeys(issues)),capture_valid=data['capture_valid'],capture_coverage=data['coverage'],
        request_cohort=cohort,request_outcomes=data['request_outcomes'],request_cache_join_coverage=join_coverage,
        latency_summary={m:legacy.stats(r[m] for r in requests) for m in ('ttft_seconds','tpot_seconds')},
        store_cache=exists,cache_rates=cache_rows,server_cache_rates=data['server_metrics']['cache_rates'],
        store_memory_coverage=store_coverage,
        phase_reconciliation=data['server_metrics'].get('phase_reconciliation'),
        endpoint_process_map=data['endpoint_process_map'],rank_labeled_metric_identities=identities,
        counts={name:len(rows) for name,rows in tables.items()},
        limitations=['local_buffer_size is Store staging/transfer memory, not framework primary host cache',
                    'External token rates, GPU token rates, Store key rates, and tier chunk rates have distinct denominators',
                    'Secondary Guard/KVCR hits can include remote hints and are never labeled local residency',
                    'Remote GET samples exclude self bytes; mixed GET duration includes the entire Store API and self work',
                    'RNIC rates include whole-port background traffic; no process attribution',
                    'Client latency cohort is completed profiling rows with finite TTFT; cancellations/errors remain separate',
                    'TPOT = (request latency - TTFT)/(output tokens - 1); not inter-token-latency samples',
                    'Raw monitor interval counter resets/nonfinite endpoints are excluded, never treated as zeros'])
    legacy.write_json(output/'report.json',clean(report))
    text=['# Offline Mooncake analysis','',f'Status: {report["status"]}', '',report['scope'],'',
          f'Measured results: {run}',f'Profiling window (ns): {bounds}',
          f'Completed profiling latency rows: {len(requests)}; remote GET samples: {len(samples)}','',
          '## Cohorts','',json.dumps(cohort,indent=2),json.dumps(data['request_outcomes'],indent=2),'',
          '## Cache accounting','', '| Cache | Numerator | Denominator | Unit | Rate |','|---|---:|---:|---|---:|']
    for r in cache_rows:text.append(f'| {r["cache"]} | {r["numerator"]} | {r["denominator"]} ({r["denominator_field"]}) | {r["unit"]} | {r["rate"]} |')
    text+=['','## Interpretation','']+['- '+s for s in report['limitations']]+['','## Evidence gaps','']+['- '+s for s in report['issues']]
    text+=['','## Store memory occupancy','',json.dumps(store_coverage,indent=2),
           'Native allocated bytes / mounted capacity, paired within each snapshot and exact label set. '
           'Global and segment series remain separate; segment-to-worker attribution requires exact endpoint/PID evidence. '
           'This is allocator occupancy, not a cache hit rate or proof of logical KV residency.']
    text+=['','## Latency distributions','',
           'Completed profiling requests only; TTFT and TPOT below are in seconds.',
           '','\n'.join(legacy.table([dict(metric=m,**stats) for m,stats in report['latency_summary'].items()],
                           ['metric','count','mean','p50','p90','p99'])),
           '', '[Latency CDF](latency-cdf.png) · [CDF data](latency-cdf.csv) · [Request cohorts](request-cohorts.csv)',
           '', '## Remote Store GET distributions','',
           'Each observation is one classified successful Store GET batch. Size is remote payload bytes '
           'and remote objects only. Duration is whole Store GET API seconds; GB/s is decimal remote '
           'bytes / whole API seconds / 1e9. Mixed batches include self work in duration. '
           'These are operation-weighted CDFs, not native wire measurements.',
           '','\n'.join(legacy.table(summary,['dataset','metric','count','mean','p50','p90','p99'])),
           '', '[Byte, object count, duration and effective bandwidth CDFs](transfer-cdf.png) · '
           '[CDF data](transfer-cdf.csv) · [Exact byte groups](transfer-exact-bytes.csv) · '
           '[Byte-conditioned bandwidth](transfer-byte-bins.csv)',
           '', '[Worker and RNIC time series](worker-rnic-timeseries.png) · '
           '[Store allocator occupancy](store-cache-utilization.png) · [Store occupancy data](store-memory-occupancy.csv)']
    (output/'report.md').write_text('\n'.join(text)+'\n')
    if make_plots:plots(output,requests,samples,intervals or sdk_rows,rates or sdk_rates,nic or data['hca']['intervals'],gpus)
    if make_plots:store_memory_plot(output,store_memory)
    # Includes all consumed raw files and this package, so frozen code is auditable.
    used.update(p for p in HERE.rglob('*') if p.is_file() and '__pycache__' not in p.parts and p.suffix in ('.py','.json','.txt','.md') and output not in p.parents)
    legacy.write_json(output/'input-hashes.json',[dict(path=str(p),bytes=p.stat().st_size,sha256=legacy.digest(p)) for p in sorted(used) if p.is_file()])
    return report


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--monitor',type=Path,action='append',default=[])
    p.add_argument('--no-plots',action='store_true')
    args=p.parse_args()
    if not args.root.is_dir():p.error('--root must be an existing directory')
    try:report=analyze(args.root,args.output,args.monitor,not args.no_plots)
    except ValueError as error:p.error(str(error))
    print(json.dumps(dict(status=report['status'],output=str(args.output),counts=report['counts']),indent=2))

if __name__=='__main__':main()
