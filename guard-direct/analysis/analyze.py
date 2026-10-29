#!/usr/bin/env python3
"""Offline KVCR Guard-only analysis; no raw input writes or cluster access."""
import argparse
from collections import defaultdict
import json
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parent))
import observations as obs
import reference_agentx as ref
from server_metrics_phase import reconcile
from capture_scope import capture_inventory, measured_runs, measured_rank_paths, separate_canary_evidence
legacy=ref.legacy()


def source_clock_samples(samples):
    rows=[]
    for row in samples:
        seconds=row.get('source_operation_seconds');size=row.get('bytes')
        if legacy.finite(size) and size>0 and legacy.finite(seconds) and seconds>0:
            rows.append(dict(row,dataset='KVCR source operation',seconds=seconds,GBps=size/seconds/1e9,
                timing_scope='Full source operation wall time; actual transferred subset bytes/blocks only'))
    return rows


def normalized_requests(rows):
    return [dict(row,ttft_seconds=ref.request_seconds(row,'time_to_first_token'),
                 tpot_seconds=ref.request_seconds(row,'time_per_output_token')) for row in rows]


def guard_slot_utilization(points):
    grouped=defaultdict(dict);conflicts=set()
    for row in points:
        resource=row.get('labels',{}).get('resource')
        if resource not in ('local_g2_total_slots','local_g2_free_slots','local_g2_allocated_slots','local_g2_evictable_slots'):continue
        labels={k:v for k,v in row['labels'].items() if k!='resource'}
        key=(row['endpoint'],row['timestamp_ns'],json.dumps(labels,sort_keys=True))
        if resource in grouped[key] and grouped[key][resource]!=row['value']:conflicts.add(key)
        grouped[key][resource]=row['value']
    result=[]
    for (endpoint,stamp,labels),counts in grouped.items():
        total=counts.get('local_g2_total_slots');allocated=counts.get('local_g2_allocated_slots')
        valid=(endpoint,stamp,labels) not in conflicts and legacy.finite(total) and legacy.finite(allocated) and 0<=allocated<=total and total>0
        result.append(dict(endpoint=endpoint,timestamp_ns=stamp,labels=json.loads(labels),**counts,
            allocated_fraction=allocated/total if valid else None,
            scope='Same endpoint/timestamp/pool labels; allocated slots include FILLING, not Guard READY export keys; no multi-pool byte weighting assumed'))
    return result


def guard_utilization_plot(output,rows,points):
    import os
    os.environ.setdefault('MPLCONFIGDIR',str(output/'.mpl-cache'))
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    fig,axes=plt.subplots(2,1,figsize=(13,9),layout='constrained');groups=defaultdict(list)
    origin=min((row['timestamp_ns'] for row in rows),default=min((row['timestamp_ns'] for row in points),default=0))
    for row in rows:
        if row['allocated_fraction'] is not None:groups[(row['endpoint'],json.dumps(row['labels'],sort_keys=True))].append(row)
    for (endpoint,labels),items in sorted(groups.items()):
        items.sort(key=lambda row:row['timestamp_ns'])
        axes[0].plot([(row['timestamp_ns']-origin)/1e9 for row in items],[row['allocated_fraction'] for row in items],label=endpoint+' '+labels)
    gauges=defaultdict(list)
    for row in points:
        resource=row.get('labels',{}).get('resource','')
        if resource in ('guard_export_index_keys','guard_export_readers') and legacy.finite(row['value']):
            gauges[(row['endpoint'],json.dumps(row['labels'],sort_keys=True))].append(row)
    for (endpoint,labels),items in sorted(gauges.items()):
        items.sort(key=lambda row:row['timestamp_ns'])
        axes[1].plot([(row['timestamp_ns']-origin)/1e9 for row in items],[row['value'] for row in items],label=endpoint+' '+labels)
    for ax,ylabel in zip(axes,['Allocated / total native Guard slots (includes FILLING)','Native export-index keys / held readers (separate series)']):
        ax.set(xlabel='Seconds since first retained Guard observation',ylabel=ylabel)
        if ax.lines:ax.legend(fontsize=6)
        else:ax.text(.5,.5,'Unavailable: no matched native gauges',ha='center',transform=ax.transAxes)
    fig.savefig(output/'guard-cache-utilization.png',dpi=140);plt.close(fig)


def discover(root):
    candidates=measured_runs(root)
    if len(candidates)>1:raise ValueError('Ambiguous measured Guard populations')
    return candidates[0] if candidates else None


def analyze(root,output,plots=True):
    root=root.resolve();output=output.resolve()
    if output==root or root in output.parents or output in root.parents:
        raise ValueError('Analysis must be outside raw input tree')
    output.mkdir(parents=True,exist_ok=True);used=set();issues=[];run=discover(root)
    scope=capture_inventory(root)
    canaries=separate_canary_evidence(root,used)
    requests=[];samples=[];stages=[];outcomes=[];cohort={};bounds=None;data={};owners={}
    if run:
        class PairRoot:
            def __truediv__(self,key):
                if key!='kvcr':raise ValueError(key)
                return run.parents[3]
        data=legacy.backend_data(PairRoot(),'kvcr',used,source_directory=run)
        issues.extend(data['issues']);bounds=data['phase_bounds_ns'];replay=run/'replay-measured'
        requests,cohort=ref.profiling_requests(replay/'profile_export.jsonl','kvcr',issues)
        requests=normalized_requests(requests)
        if (replay/'profile_export.jsonl').is_file():used.add(replay/'profile_export.jsonl')
        data['server_metrics']=reconcile(replay/'server_metrics_export.json',data['server_metrics'],bounds,used,issues)
        outcomes=ref.kvcr_source_outcomes(data,used);owners=data.get('agent_ownership',{})
        samples=[dict(row,case='kvcr-single') for row in data['samples']+ref.direct_local_samples(data,used)];stages=data['stages']
        issues.extend(x for x in data['issues'] if x not in issues)
    else:issues.append('No measured Guard results; no phase or native transfer evidence invented')
    paths=sorted({p.resolve() for p in root.glob('**/monitor-raw*.jsonl')})
    points,intervals,rates,nic,gpus,errors=obs.read_monitor(root,paths,bounds,used,issues)
    if bounds is None:issues.append('No verified profiling bounds; monitor observations are unscoped')
    scoped=[row for row in points if bounds is not None and bounds[0]<=row['timestamp_ns']<=bounds[1]]
    violations=[];pins=[];live=[];claims=[];schedule=[];publications=[];query_rows=[];guard_events=[]
    capture_files=sorted(p for directory in scope['selected_directories'] for p in Path(directory).glob('*.jsonl'))
    for path in capture_files:
        used.add(path)
        for row in legacy.read_jsonl(path,issues):
            if row.get('event')=='forbidden_source_pin':violations.append(row)
            if row.get('stage') in ('pin_submission','source_pin','pin_install','pin_wait'):pins.append(row)
            if row.get('event')=='progress_schedule':schedule.append(row)
            stamp=row.get('wall_ns')
            in_phase=bounds and stamp is not None and bounds[0]<=stamp<=bounds[1]
            if in_phase and row.get('event')=='query_outcomes':query_rows.append(row)
            if in_phase and row.get('guard_event') in ('ready','miss','contention','held','native_cleanup') and row.get('event')=='stage':guard_events.append(row)
            if row.get('stage')=='guard_export_ready':
                publications.append(dict(legacy.reference(row),block_keys_hex=row.get('block_keys_hex'),
                    export_index_present=row.get('guard_export_index_present'),native_deposit_completion_ns=None,
                    native_to_export_ready_seconds=None,correlation_status='No exact native deposit completion key/time correlation; caller commit remains required'))
            if 'guard' in str(row.get('stage','')) or row.get('stage') in ('source_acquisition','source_release','source_progress'):
                stamp=row.get('wall_ns')
                if bounds and stamp is not None and bounds[0]<=stamp<=bounds[1]:claims.append(row)
    for path in measured_rank_paths(root):
        used.add(path);live.append(json.loads(path.read_text()))
    if {row.get('rank') for row in live if row.get('guard_only_sources') is True}!=set(range(8)):
        issues.append('Missing active Option A proofs for all eight measured ranks')
    if violations or pins:issues.append('Forbidden source pin events observed')
    if not paths:issues.append('No independent monitor capture')
    if not samples:issues.append('No positive measured native RDMA samples; smoke evidence is separate')
    occupancy=[dict(row,scope='native exporter gauge; no configured occupancy denominator') for row in scoped
        if any(term in (row['metric']+' '+str(row['labels'])).lower() for term in ('occupancy','capacity','resident','free_bytes','used_bytes','cache_usage','slots','export_index','export_held','export_readers','cleanup_pending'))]
    if not occupancy:issues.append('No native occupancy gauges; configured Guard124 GiB/zero host bytes is not measured occupancy')
    ownership=ref.transfer_ownership(samples,owners,data.get('process_ownership'))
    if any(not row.get('ownership_verified',row.get('distinct_processes_verified')) for row in ownership):issues.append('Transfer ownership must be reviewed; ambiguous owners not inferred')
    server=data.get('server_metrics',{})
    for row in server.get('cache_rates',[]):
        if 'Guard' in row.get('cache','') or 'secondary' in row.get('cache','').lower():
            row['residency_proof']=False
            row['interpretation']='Secondary lookup may include REMOTE_G2 advisory hints; not a Guard-resident READY hit rate'
    sdk=obs.export_worker_series(run/'replay-measured',bounds,used,issues) if run else []
    sdk_rows,sdk_rates=obs.sdk_intervals(sdk,server.get('phase_reconciliation'))
    native_samples=samples;samples=native_samples+source_clock_samples(native_samples)
    original_datasets=legacy.DATASETS
    try:
        legacy.DATASETS=('KVCR native','KVCR source operation')+tuple(dict.fromkeys(r['dataset'] for r in native_samples if r.get('local_native')))
        summary,cdfs,bins,bin_cdfs,exact=legacy.distributions(samples)
        byte_bins,byte_cdfs=legacy.byte_distributions(samples)
    finally:legacy.DATASETS=original_datasets
    query_counts=defaultdict(int)
    for row in query_rows:
        for key,count in row['query_status_tier_counts'].items():query_counts[key]+=count
    query_total=sum(query_counts.values());ready=miss=0
    for row in guard_events:
        if row.get('guard_event')=='ready':ready+=row.get('guard_event_count') or 0
        if row.get('guard_event')=='miss':miss+=row.get('guard_event_count') or 0
    cache_rates=[dict(metric=key,numerator=count,denominator=query_total,
        rate=count/query_total if query_total else None,scope='KVCR query attempts by exact QueryStatus / CacheTier; REMOTE_G2 is advisory') for key,count in sorted(query_counts.items())]
    cache_rates.append(dict(metric='Remote Guard READY export fraction',numerator=ready,denominator=ready+miss,
        rate=ready/(ready+miss) if ready+miss else None,scope='Guard source requested keys classified ready/miss, not vLLM token or primary chunk rate'))
    if not query_rows:issues.append('No exact QueryStatus/CacheTier observations: LOCAL_G2 READY and REMOTE_G2 advisory rates unavailable')
    profile=data.get('profile') or {};seconds=(bounds[1]-bounds[0])/1e9 if bounds else None
    tokens=sum(row['output_sequence_length'] for row in requests if legacy.finite(row.get('output_sequence_length')))
    throughput=[dict(completed=len(requests),phase_seconds=seconds,output_tokens=tokens,
        output_tokens_per_second=tokens/seconds if seconds and requests else None,
        sdk_requests_per_second=profile.get('request_throughput',{}).get('avg'),
        sdk_input_tokens_per_second=profile.get('input_token_throughput',{}).get('avg'),
        sdk_output_tokens_per_second=profile.get('output_token_throughput',{}).get('avg'),
        scope='Completed profiling rows / full retained phase bounds; SDK aggregates separately retained')]
    tables={'separate-canary-evidence':canaries,'requests':requests,'transfer-samples':samples,'transfer-summary':summary,'transfer-cdf':cdfs,
        'transfer-object-bins':bins,'transfer-object-bin-cdf':bin_cdfs,'transfer-exact-bytes':exact,
        'transfer-byte-bins':byte_bins,'transfer-byte-bin-cdf':byte_cdfs,
        'source-operation-outcomes':outcomes,'stage-samples':stages,'claim-progress-events':claims,
        'progress-scheduling':schedule,'deposit-export-ready':publications,'throughput-summary':throughput,
        'query-outcomes':query_rows,'guard-source-events':guard_events,'tier-cache-rates':cache_rates,
        'forbidden-source-pin-events':violations+pins,'option-a-live-ranks':live,'transfer-ownership':ownership,
        'server-cache-rates':server.get('cache_rates',[]),'worker-samples':scoped,'worker-intervals':intervals,
        'server-selected-series':server.get('series',[]),'request-outcomes':[data.get('request_outcomes',{})],
        'worker-cache-rates':rates,'worker-sdk-intervals':sdk_rows,'worker-sdk-cache-rates':sdk_rates,
        'cache-occupancy-gauges':occupancy,'rnic-intervals':nic,'gpu-samples':gpus,'monitor-errors':errors}
    tables['guard-slot-utilization']=guard_slot_utilization(scoped)
    latency=[]
    for metric in ('ttft_seconds','tpot_seconds'):
        latency.extend(dict(metric=metric,value=v,cdf=p) for v,p in legacy.ecdf(row[metric] for row in requests))
    tables['latency-cdf']=latency
    for name,rows in tables.items():legacy.write_csv(output/(name+'.csv'),obs.clean(rows))
    stage_groups=defaultdict(list)
    for row in stages:stage_groups[(row.get('stage'),row.get('origin'))].append(row.get('seconds'))
    stage_stats=[dict(stage=stage,origin=origin,**legacy.stats(values)) for (stage,origin),values in stage_groups.items()]
    legacy.write_csv(output/'stage-summary.csv',stage_stats)
    report=dict(scope='KVCR Guard-only READY-subset diagnostic evidence; not a reproduction audit',
        status='incomplete' if issues or data.get('capture_valid') is not True else 'evidence_available',
        phase_bounds_ns=bounds,capture_scope=scope,separate_canary_evidence=canaries,process_ownership=data.get('process_ownership'),direct_native_accounting=data.get('direct_native_accounting'),request_cohort=cohort,request_outcomes=data.get('request_outcomes',{}),profile=data.get('profile'),
        capture_valid=data.get('capture_valid'),capture_coverage=data.get('coverage'),source_pin_event_count=len(violations)+len(pins),
        phase_reconciliation=server.get('phase_reconciliation'),counts={name:len(rows) for name,rows in tables.items()},
        latency_summary={name:legacy.stats(row[name] for row in requests) for name in ['ttft_seconds','tpot_seconds']},issues=list(dict.fromkeys(issues)),
        limitations=['ZERO framework host cache; Guard 124 GiB/rank; direct GPU endpoints; no framework source GET fallback',
            'Router inventory excludes primary-only CPU entries; GPU events unchanged; stale hints explicitly miss',
            'GPU/external token, Guard local query and remote READY key rates have distinct denominators; framework host cache is N/A; hints do not establish residency',
            'Native NIXL duration differs from source-operation and claim/progress clocks; stages may overlap',
            'Missing occupancy/callback/rank/timing evidence is unavailable, never zero',
            'Whole-port RNIC traffic includes background; IB words converted to bytes; same-node HCA path',
            'Client TTFT/TPOT cohort is completed profiling rows; warmup/errors/cancellations remain separate'])
    report['limitations'].append('PUT/deposit still needs caller commit before Guard READY publication; exact native-completion-to-publication delay is unavailable without matching key/timestamps')
    native_proof=run.parents[3]/'phases/prepare/logs/guard-ready-rdma.json' if run else None
    if native_proof and native_proof.is_file():
        used.add(native_proof);report['native_semantic_canary']=json.loads(native_proof.read_text())
    elif any(r.get('original_verified') is True for r in canaries):
        report['native_semantic_canary']=dict(scope='Separate original preparation receipts; not measured populations',receipts=canaries)
    else:report['issues'].append('Native semantic canary proof unavailable in collected tree; zero runtime callback records alone is not hard proof')
    if report['issues']:report['status']='incomplete'
    legacy.write_json(output/'report.json',obs.clean(report))
    def table(rows,columns):
        if not rows:return 'Unavailable; inspect evidence gaps.\n'
        return '| '+' | '.join(columns)+' |\n| '+' | '.join('---' for _ in columns)+' |\n'+ '\n'.join('| '+' | '.join(str(row.get(c)) for c in columns)+' |' for row in rows)+'\n'
    latency_stats=[dict(metric=key,**value) for key,value in report['latency_summary'].items()]
    text='# KVCR Guard Option A analysis\n\nStatus: '+report['status']+'\n\n'
    text+='## Client latency and throughput\n\n'+table(latency_stats,['metric','count','mean','p50','p90','p99'])+'\n'
    text+=table(throughput,['completed','phase_seconds','output_tokens','output_tokens_per_second','sdk_requests_per_second','sdk_input_tokens_per_second'])+'\n'
    text+='## Cache rates and occupancy\n\n'+table(cache_rates,['metric','numerator','denominator','rate','scope'])+'\n'
    text+=table(server.get('cache_rates',[]),['cache','hits','queries','hit_rate','interpretation'])+'\n'
    text+=f"Guard matched slot snapshots: {len(tables['guard-slot-utilization'])}; raw occupancy gauges: {len(occupancy)}. Allocated includes FILLING; export READY is separate.\n\n"
    text+='## Transfer clocks\n\n'+table(summary,['dataset','metric','count','mean','p50','p90','p99'])+'\n'
    text+='## Claim, progress and control stages\n\n'+table(stage_stats,['stage','origin','count','mean','p50','p90','p99'])+'\n'
    text+='## Interpretation\n\n'+'\n'.join('- '+x for x in report['limitations'])+'\n\n## Evidence gaps\n\n'+'\n'.join('- '+x for x in report['issues'])+'\n'
    (output/'report.md').write_text(text)
    used.update(p for p in Path(__file__).parent.rglob('*') if p.is_file() and '__pycache__' not in p.parts)
    legacy.write_json(output/'input-hashes.json',[dict(path=str(p),sha256=legacy.digest(p)) for p in sorted(used)])
    if plots:
        obs.plots(output,requests,samples,intervals,rates,nic,gpus)
        guard_utilization_plot(output,tables['guard-slot-utilization'],scoped)
    return report


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--root',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True);p.add_argument('--no-plots',action='store_true');args=p.parse_args()
    result=analyze(args.root,args.output,not args.no_plots)
    print(json.dumps(dict(status=result['status'],issues=result['issues'])))


if __name__=='__main__':main()
