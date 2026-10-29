#!/usr/bin/env python3
"""Offline three-case AgentX analysis. Missing evidence produces an incomplete audit.

Only collected files are read. --validation-only labels historical fixture
validation and can never create a passing AgentX completion audit.
"""
import argparse
from collections import Counter, defaultdict
from functools import lru_cache
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import sys
import time

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from server_metrics_phase import reconcile as reconcile_server_metrics
CASES = ('mooncake', 'kvcr-single', 'kvcr-dual')
LABELS = {'mooncake': 'Mooncake', 'kvcr-single': 'KVCR single RNIC', 'kvcr-dual': 'KVCR dual RNIC'}
CASE_COLORS = {'mooncake':'C0','kvcr-single':'C1','kvcr-dual':'C2'}
METRICS = ('bytes', 'blocks_or_objects', 'seconds', 'GBps')


@lru_cache(maxsize=1)
def legacy():
    path = ROOT / 'vendor/scripts/analyze-paired.py'
    spec = importlib.util.spec_from_file_location('agentx_frozen_analysis', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def read(path, default=None):
    return json.loads(path.read_text()) if path.exists() else default


def request_seconds(row, metric):
    if metric == 'time_per_output_token':
        value, unit = legacy().request_tpot(row), row.get('request_latency_unit')
    else:
        value, unit = row.get(metric), row.get(metric + '_unit')
    scale = {'s': 1, 'sec': 1, 'seconds': 1, 'ms': .001, 'us': 1e-6, 'ns': 1e-9}.get(unit)
    return value * scale if legacy().finite(value) and scale is not None else None


def operation_clock_samples(samples):
    result=[]
    for row in samples:
        kvcr=row['case']!='mooncake'
        seconds=row.get('source_operation_seconds') if kvcr else row.get('seconds')
        seconds=seconds if legacy().finite(seconds) and seconds>=0 else None
        scope='KVCR source operation wall time' if kvcr else 'whole Store GET API time'
        label=LABELS[row['case']] if kvcr else row.get('dataset',row['series'])
        result.append(dict(row,seconds=seconds,
            GBps=row['bytes']/seconds/1e9 if seconds is not None and seconds>0 else None,
            timing_scope=scope,series=label+' / '+scope))
    return result


def direct_process_ownership(run, used, issues, owners):
    """Direct workers own KVCR; historical host-staged ownership is unchanged."""
    paths=sorted(Path(run).glob('option-a-rank-*.json'))
    proofs=[]
    for path in paths:
        used.add(path)
        proofs.append((path,read(path,{})))
    if not any(row.get('direct_gpu_guard') is True for _,row in proofs):
        return dict(mode='historical',verified=None,rank_pids={},rank_agents={},
                    pid_source='Captured identity_ready; historical distinct-process semantics')
    errors=[];rank_pids={};rank_agents={}
    for path,row in proofs:
        rank,pid=row.get('rank'),row.get('pid')
        if (type(rank) is not int or type(pid) is not int or pid<=0
                or path.stem != f'option-a-rank-{rank}' or str(rank) in rank_pids):
            errors.append('Invalid/duplicate direct rank/PID proof: '+str(path));continue
        rank_pids[str(rank)]=pid
        if not (row.get('direct_gpu_guard') is True and row.get('primary_host_data_bytes')==0
                and row.get('framework_region_mem_types')==['VRAM']
                and row.get('gpu_region',{}).get('mem_type')=='VRAM'
                and row.get('gpu_region',{}).get('size')==1228800
                and row.get('guard_pool_bytes')==124*1024**3
                and row.get('guard_only_sources') is True and row.get('guard_only_source_proof') is True
                and row.get('opportunistic_query') is False and row.get('source_pin_callback_calls')==0):
            errors.append(f'Direct rank {rank} is not zero-host/VRAM/Guard124/strict')
        agents=[agent for agent,rows in owners.items() if pid in {r.get('pid') for r in rows}]
        if len(agents)!=1 or {r.get('pid') for r in owners.get(agents[0],[])}!={pid}:
            errors.append(f'Direct rank {rank} worker PID {pid} lacks a unique captured agent')
        else:rank_agents[str(rank)]=agents[0]
    if set(rank_pids)!=set(map(str,range(8))) or len(set(rank_pids.values()))!=8:
        errors.append('Direct worker mapping requires exactly eight ranks and eight distinct actual worker PIDs')
    issues.extend(errors)
    return dict(mode='direct',verified=not errors,rank_pids=rank_pids,rank_agents=rank_agents,errors=errors,
                pid_source='option-a-rank + captured identity_ready; never EngineCore log inference')


def direct_local_samples(data,used):
    """Local copies deliberately have one PID; do not apply a remote-only filter."""
    proof=data.get('process_ownership',{})
    if proof.get('mode')!='direct' or proof.get('verified') is not True:return []
    rows=data.get('_direct_native_rows',[]);samples=[];rejected=[]
    for row in rows:
        if row.get('status')!='success':continue
        direction=(row.get('source_mem_type'),row.get('destination_mem_type'))
        operation={('VRAM','DRAM'):'PUT',('DRAM','VRAM'):'GET'}.get(direction)
        size=row.get('bytes');seconds=row.get('native_seconds')
        source,target=row.get('source_agent'),row.get('target_agent')
        pids={r.get('pid') for r in data['agent_ownership'].get(source,[])}
        if (operation is None or not source or source!=target or pids!={row.get('pid')}
                or row.get('pid') not in proof['rank_pids'].values()
                or row.get('native_handle_released') is not True or row.get('bytes_source')!='native_telemetry'
                or not legacy().finite(size) or size<=0 or size%1228800 or row.get('descriptor_bytes')!=size
                or not legacy().finite(seconds) or seconds<=0):
            rejected.append(legacy().reference(row));continue
        samples.append(dict(legacy().sample(row,f'KVCR local {operation} native',size,size//1228800,
            seconds,'Released local GPU/Guard SDK transfer duration; not wire bandwidth'),
            local_native=True,operation=operation,source_operation_seconds=None,bytes_source='native_telemetry'))
    data['direct_native_accounting']=dict(observed=len(rows),retained=len(samples),rejected_successful=rejected,
        status_counts=dict(Counter(r.get('status','unknown') for r in rows)))
    if rejected:data['issues'].append(f'{len(rejected)} successful direct native rows rejected; see direct_native_accounting')
    return samples


def kvcr_source_outcomes(data, used):
    """Keep no-payload outcomes separate from actual transfers and errors."""
    bounds=data['phase_bounds_ns'];rows=[];seen=set();owners=defaultdict(list)
    data['agent_ownership']=owners
    data['_direct_native_rows']=[]
    if bounds is None:return rows
    for path in sorted((Path(data['source_directory'])/'nixl-transfers').glob('*.jsonl')):
        used.add(path)
        for row in legacy().read_jsonl(path,data['issues']):
            if row.get('event')=='identity_ready'and row.get('agent')and row.get('pid')is not None:
                owners[row['agent']].append(legacy().reference(row))
            if row.get('event')=='native_transfer_terminal' and bounds[0]<=row.get('wall_ns',-1)<=bounds[1]:
                data['_direct_native_rows'].append(row)
            if row.get('event')!='source_nixl_terminal':continue
            stamp=row.get('end_wall_ns',row.get('wall_ns'))
            if stamp is None or not bounds[0]<=stamp<=bounds[1]:continue
            identity=tuple(row.get(k)for k in ('session','pid','sequence'))
            if all(v is not None for v in identity):
                if identity in seen:continue
                seen.add(identity)
            size=row.get('bytes');native=row.get('native')or{}
            payload=('positive'if size>0 else 'zero')if legacy().finite(size)and size>=0 else'unavailable'
            rows.append(dict(legacy().reference(row),status=row.get('status'),result=row.get('result'),
                reason=row.get('reason'),reasons=json.dumps(row.get('reasons',[]),sort_keys=True),
                bytes=size,blocks=row.get('blocks'),requested_blocks=row.get('requested_blocks'),
                payload_class=payload,native_submitted=native.get('submitted'),
                native_state=native.get('xfer_state'),native_seconds=row.get('native_seconds'),
                source_operation_seconds=row.get('source_operation_seconds')))
    data['process_ownership']=direct_process_ownership(Path(data['source_directory']),used,data['issues'],owners)
    return rows


def transfer_ownership(samples, owners, process_proof=None):
    result=[]
    for row in samples:
        source={r['pid']for r in owners.get(row['source_agent'],[])if r.get('pid')is not None}
        target={r['pid']for r in owners.get(row['target_agent'],[])if r.get('pid')is not None}
        distinct=len(source)==1 and len(target)==1 and source!=target
        direct=bool(process_proof and process_proof.get('mode')=='direct')
        local=direct and row.get('local_native') is True
        verified=(process_proof.get('verified') is True and len(source)==1 and source==target
                  and source=={row.get('pid')}) if local else distinct
        if direct and not local:verified=verified and process_proof.get('verified') is True and source=={row.get('pid')}
        result.append(dict(legacy().reference(row),bytes=row['bytes'],
            source_pids=sorted(source),target_pids=sorted(target),
            distinct_processes_verified=distinct,ownership_verified=verified,
            ownership_scope='local native copy in one verified direct worker' if local else 'distinct captured source/target processes'))
    return result


def matched_kvcr_sizes(samples):
    rows=[]
    sizes={r['bytes']for r in samples if r['case']=='kvcr-single'}&{
           r['bytes']for r in samples if r['case']=='kvcr-dual'}
    for size in sorted(sizes):
        record=dict(bytes=size)
        for case,prefix in (('kvcr-single','single'),('kvcr-dual','dual')):
            selected=[r for r in samples if r['case']==case and r['bytes']==size]
            record[prefix+'_samples']=len(selected)
            for metric in ('seconds','GBps'):
                stats=legacy().stats(r.get(metric)for r in selected)
                record.update({prefix+'_'+stat+'_'+metric:stats[stat]for stat in ('mean','p50','p90','p99')})
        single=record['single_p50_GBps'];dual=record['dual_p50_GBps']
        record['dual_over_single_p50_GBps']=dual/single if single and dual is not None else None
        rows.append(record)
    return rows


def profiling_requests(path, backend, issues):
    """The pinned streaming JSONL exporter emits warmup as well as profiling."""
    requests=[];phases=Counter();errors=0;missing_latency=0
    if path.exists():
        for row in legacy().read_jsonl(path,issues):
            meta=row.get('metadata',{});phase=meta.get('benchmark_phase')
            phases[phase or 'unknown']+=1
            if phase!='profiling':continue
            if row.get('error') or row.get('error_details'):
                errors+=1;continue
            metrics=row.get('metrics',{})
            if not legacy().finite(metrics.get('time_to_first_token',{}).get('value')):
                missing_latency+=1;continue
            request=dict(legacy().reference(row),backend=backend,
                request_id=meta.get('x_request_id'),conversation_id=meta.get('conversation_id'),
                turn_index=meta.get('turn_index'),benchmark_phase=phase,phase_index=meta.get('phase_index'))
            for metric in (*legacy().REQUEST_METRICS,'usage_prompt_cache_read_tokens'):
                request[metric]=metrics.get(metric,{}).get('value')
                request[metric+'_unit']=metrics.get(metric,{}).get('unit')
            requests.append(request)
    if phases['unknown']:
        issues.append(f'{phases["unknown"]} request rows lack metadata.benchmark_phase; excluded: {path}')
    return requests,dict(rows_by_phase=dict(phases),profiling_errors=errors,
        profiling_missing_ttft=missing_latency,usable_profiling_latencies=len(requests),
        request_cohort_scope='metadata.benchmark_phase == profiling; warmup and unknown phases excluded')


def api_cache_samples(requests):
    rows=[];coverage=dict(completed_requests=len(requests),usable_cache_records=0,
        missing_cache_field=0,invalid_cache_counts=0,invalid_units=0)
    for request in requests:
        prompt=request.get('input_sequence_length');cached=request.get('usage_prompt_cache_read_tokens')
        if prompt is None or cached is None:
            coverage['missing_cache_field']+=1;continue
        if request.get('input_sequence_length_unit')!='tokens' or request.get('usage_prompt_cache_read_tokens_unit')!='tokens':
            coverage['invalid_units']+=1;continue
        if not all(legacy().finite(v) and v>=0 for v in (prompt,cached)) or cached>prompt:
            coverage['invalid_cache_counts']+=1;continue
        rows.append(dict(legacy().reference(request),request_id=request['request_id'],
            prompt_tokens=prompt,cached_prompt_tokens=cached,uncached_prompt_tokens=prompt-cached,
            ttft_seconds=request_seconds(request,'time_to_first_token'),
            accounting_source='Server usage_prompt_cache_read_tokens; total cached prefix, no tier attribution'))
    coverage['usable_cache_records']=len(rows)
    return rows,coverage


def cache_diagnostics(rows,case,metrics):
    correlations=[];bins=[]
    if rows:
        from scipy.stats import spearmanr
        for metric in metrics:
            xs=[r[metric] for r in rows];ys=[r['ttft_seconds'] for r in rows]
            rho=float(spearmanr(xs,ys).statistic) if len(set(xs))>1 and len(set(ys))>1 else None
            correlations.append(dict(case=case,metric=metric,paired_requests=len(rows),spearman_rho=rho))
    edges=(0,1024,16384,65536,131072,262144,524288,math.inf)
    for lo,hi in zip(edges,edges[1:]):
        selected=[r['ttft_seconds'] for r in rows if lo<=r['uncached_prompt_tokens']<hi]
        bins.append(dict(case=case,min_uncached_tokens=lo,
            max_uncached_tokens_exclusive=hi if math.isfinite(hi) else None,unit='seconds',**legacy().stats(selected)))
    return correlations,bins


def queue_samples(path, bounds, duration):
    out = dict(rows=[], unavailable_samples=0, issues=[])
    if bounds is not None and path.exists():
        # Restrict to the configured send window; drain/export are separate.
        end = min(bounds[1], bounds[0] + duration * 10**9)
        for row in legacy().read_jsonl(path, out['issues']):
            utc = row.get('utc')
            if (row.get('phase') != 'measured' or not legacy().finite(utc)
                    or not bounds[0] <= utc * 10**9 < end):
                continue
            if row.get('metrics_available') is not True or not legacy().finite(row.get('queue_depth')):
                out['unavailable_samples'] += 1
                continue
            out['rows'].append(dict(elapsed_seconds=utc-bounds[0]/1e9,
                queue_depth=row['queue_depth'], memory_current_bytes=row.get('memory_current_bytes'),
                oom=row.get('oom'), utc=utc))
    out['queue'] = legacy().stats(r['queue_depth'] for r in out['rows'])
    out['queue']['max'] = max((r['queue_depth'] for r in out['rows']), default=None)
    out['scope'] = 'Unweighted health observations within profiling send window; unavailable samples excluded'
    return out


def mooncake_gets(data, used):
    """GET classification keeps local, remote and unknown bytes separately."""
    rows, issues, seen = [], [], set()
    bounds = data['phase_bounds_ns']
    for path in sorted((Path(data['source_directory']) / 'store-events').glob('*.jsonl')):
        used.add(path)
        for row in legacy().read_jsonl(path, issues):
            stamp = row.get('end_wall_ns', row.get('wall_ns'))
            if (row.get('event') != 'store_operation' or row.get('operation') != 'get'
                    or bounds is None or stamp is None or not bounds[0] <= stamp <= bounds[1]):
                continue
            key = tuple(row.get(k) for k in ('session', 'pid', 'sequence'))
            if all(v is not None for v in key):
                if key in seen:
                    continue
                seen.add(key)
            resolved = legacy().telemetry().resolve_get(row, data['endpoint_process_map'])
            rows.append({**legacy().reference(row), 'get_class': resolved['get_class'],
                'seconds': row.get('seconds'), 'failed_objects': row.get('failed_objects'),
                **{f'{kind}_successful_{unit}': resolved[f'{kind}_successful_{unit}']
                   for kind in ('local', 'remote', 'unknown') for unit in ('bytes', 'objects')}})
    return rows, issues


def request_cache_samples(data, backend, used):
    """Join completed client requests through logged IDs, never time proximity."""
    home=Path(data['source_directory']);bounds=data['phase_bounds_ns']
    mapping=defaultdict(dict);admissions=defaultdict(list);issues=[];seen=set()
    coverage=dict(completed_requests=len(data['requests']),matched=0,missing_frontend=0,
        ambiguous_frontend=0,missing_admission=0,ambiguous_admission=0,invalid_counts=0,
        admitted_records=0,scope='Completed profiling responses; unique frontend ID bridge and non-preempted scheduler admission')
    ansi=re.compile(r'\x1b\[[0-?]*[ -/]*[@-~]')
    uuid=r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}'
    frontend=home/'frontend.log'
    if frontend.exists():
        used.add(frontend)
        with frontend.open() as stream:
            for number,line in enumerate(stream,1):
                if 'request received' not in line:continue
                line=ansi.sub('',line)
                client=re.search(r'\bx_request_id="([^"]+)"',line)
                server=re.search(r'\brequest_id=("?)('+uuid+r')\1',line)
                if client and server:
                    mapping[client[1]].setdefault(server[2],number)
    directory=home/('store-events' if backend=='mooncake' else 'nixl-transfers')
    for path in sorted(directory.glob('*.jsonl')):
        used.add(path)
        for row in legacy().read_jsonl(path,issues):
            stamp=row.get('wall_ns')
            if (row.get('event')!='external_prefix_accounting' or not row.get('admitted_requests')
                    or not row.get('resolved') or row.get('preempted') or bounds is None
                    or stamp is None or not bounds[0]<=stamp<=bounds[1]):continue
            key=tuple(row.get(k) for k in ('session','pid','sequence'))
            if all(v is not None for v in key):
                if key in seen:continue
                seen.add(key)
            identifier=row.get('request_id','')
            suffix=re.fullmatch('('+uuid+r')-[0-9a-f]{8}',identifier)
            identifier=suffix[1] if suffix else identifier
            admissions[identifier].append(row);coverage['admitted_records']+=1
    result=[]
    for request in data['requests']:
        candidates=mapping.get(request.get('request_id'),{})
        if len(candidates)!=1:
            coverage['ambiguous_frontend' if candidates else 'missing_frontend']+=1;continue
        identifier=next(iter(candidates));native=admissions.get(identifier,[])
        if len(native)!=1:
            coverage['ambiguous_admission' if native else 'missing_admission']+=1;continue
        row=native[0]
        prompt,gpu,external=(row.get(k) for k in ('prompt_tokens','gpu_cached_tokens','external_cached_tokens'))
        if not all(legacy().finite(v) and v>=0 for v in (prompt,gpu,external)) or gpu+external>prompt:
            coverage['invalid_counts']+=1;continue
        result.append(dict(legacy().reference(row),request_id=request['request_id'],
            server_request_id=identifier,native_request_id=row['request_id'],
            frontend_source_file=str(frontend),frontend_source_line=candidates[identifier],
            ttft_seconds=request_seconds(request,'time_to_first_token'),prompt_tokens=prompt,
            gpu_cached_tokens=gpu,external_cached_tokens=external,uncached_prompt_tokens=prompt-gpu-external))
    coverage['matched']=len(result)
    coverage['match_fraction']=len(result)/len(data['requests']) if data['requests'] else None
    return result,coverage,issues


def request_cache_plot(output,rows,validation_only=False,stem='request-cache-ttft',
                       title='Completed requests: verified frontend to scheduler ID bridge'):
    os.environ.setdefault('MPLCONFIGDIR','/tmp/agentx-matplotlib')
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    fig,axes=plt.subplots(1,2,figsize=(13,5),layout='constrained')
    for ax,metric in zip(axes,('prompt_tokens','uncached_prompt_tokens')):
        for case in CASES:
            selected=[r for r in rows if r['case']==case and legacy().finite(r['ttft_seconds']) and r['ttft_seconds']>0]
            if selected:ax.scatter([r[metric] for r in selected],[r['ttft_seconds'] for r in selected],
                s=7,alpha=.35,label=LABELS[case],color=CASE_COLORS[case],rasterized=True)
        ax.set(xlabel=metric.replace('_',' '),ylabel='Client TTFT (s)',yscale='log')
        ax.set_xscale('symlog',linthresh=1024)
        ax.set_xlim(left=0);ax.grid(alpha=.25)
    if rows:axes[0].legend(fontsize=8)
    fig.suptitle(('HISTORICAL FIXTURE VALIDATION ONLY\n' if validation_only else '')+
        title)
    for extension in ('png','pdf'):fig.savefig(output/(stem+'.'+extension),dpi=170)
    plt.close(fig)


def completion_checks(run, config, backends):
    """Evidence gates, not a claim based merely on a green driver or plots."""
    checks = []
    def add(name, passed, evidence=None):
        checks.append(dict(requirement=name, passed=bool(passed), evidence=evidence))
    context = read(run / 'collection-context.json', {})
    add('Actual driver launch', read(run / 'launch.json', {}).get('driver_pid'))
    add('Driver terminal exit 0', context.get('status', {}).get('driver_exit') == '0')
    cache = read(run / 'native-cache-preparation.json', {}) or {}
    add('Pinned native AgentX cache actually built and adopted', cache.get('state') == 'complete'
        and cache.get('native_manager_lookup_hit') is True
        and cache.get('dataset_revision') == config['dataset_revision'])
    integrity = read(run / 'integrity-verification.json', {})
    for case in CASES:
        backend = 'mooncake' if case == 'mooncake' else 'kvcr'
        pair = run / 'cases' / case / 'pair'
        home = pair / backend
        data = backends.get(case, {})
        state = read(home / 'state.json', {})
        add(f'{case}: complete controller and closed artifacts',
            state.get('state') == 'complete' and state.get('artifact_ready') is True)
        hashes = read(home / 'artifact-hashes.json', {})
        valid_hashes = bool(hashes)
        if valid_hashes:
            for relative, expected in hashes.items():
                path = home / relative
                if not path.is_file() or not path.resolve().is_relative_to(home.resolve()) or legacy().digest(path) != expected:
                    valid_hashes = False
                    break
        add(f'{case}: authenticated payload integrity reverified',
            integrity.get(case, {}).get('verified') is True and valid_hashes)
        seconds = data.get('phase_seconds')
        add(f'{case}: full profiling duration', legacy().finite(seconds)
            and config['duration_seconds']-5 <= seconds <= config['duration_seconds']+config['grace_seconds']+90, seconds)
        add(f'{case}: lossless capture protocol', data.get('capture_valid') is True,
            data.get('issues'))
        cfg = read(pair / 'controller.json', {})
        add(f'{case}: common frozen experiment config', cfg.get('experiment') == config)
        phase = home / 'phases/measured'
        command = read(phase / 'replay-command.json', [])
        from runtime.commands import replay_command
        expected = replay_command(config, cfg.get('agentx_assets', '/missing'),
            Path(cfg.get('pair_dir', '/missing')) / backend / 'phases/measured/results'
            / ('mooncake' if backend == 'mooncake' else 'guard') / 'replay-measured')
        add(f'{case}: exact AgentX replay command', command == expected)
        probe = read(home / 'phases/smoke/probe.json', {})
        from sys import path as module_path
        vendor_path = str(ROOT / 'vendor/b200-dsv4pro')
        if vendor_path not in module_path:
            module_path.insert(0, vendor_path)
        from paired.common import validate_probe
        try:
            validate_probe(probe)
            probe_valid = True
        except (ValueError, TypeError, KeyError):
            probe_valid = False
        add(f'{case}: actual eight-rank prefix and transport probe', probe_valid)
        if backend == 'kvcr':
            for phase_name in ('smoke', 'measured'):
                proof = read(home / f'phases/{phase_name}/closed-transport-proof.json', {})
                devices = config['dual_rnics'] if case == 'kvcr-dual' else [config['single_rnic']]
                ranks = proof.get('ranks', [])
                add(f'{case}: {phase_name} closed SGL rail evidence', proof.get('verified') is True
                    and proof.get('logs_closed') is True and proof.get('devices') == devices
                    and sorted(r.get('rank', -1) for r in ranks) == list(range(8)))
            ownership=data.get('transfer_ownership',{})
            add(f'{case}: measured transfer endpoints have distinct verified process owners',
                data.get('capture_valid') is True and ownership.get('verified') is True,ownership)
        add(f'{case}: usable request latency and throughput exports', bool(data.get('requests'))
            and legacy().finite((data.get('profile') or {}).get('output_token_throughput', {}).get('avg')))
        phase_counts=data.get('request_phase_accounting',{}).get('rows_by_phase',{})
        add(f'{case}: explicit profiling-only request cohort',
            phase_counts.get('profiling',0)>0 and phase_counts.get('unknown',0)==0)
        add(f'{case}: request cohort matches SDK completed TTFT count',
            bool(data.get('requests')) and len(data['requests']) ==
            (data.get('profile') or {}).get('time_to_first_token',{}).get('count'))
        api=data.get('api_cache_accounting',{})
        add(f'{case}: valid API-reported prompt cache token coverage',
            bool(data.get('requests')) and api.get('usable_cache_records')==len(data['requests']))
        add(f'{case}: verified server-metric warmup reconciliation',
            data.get('server_metrics',{}).get('phase_reconciliation',{}).get('verified') is True)
        add(f'{case}: measured queue observations', data.get('queue', {}).get('queue', {}).get('count', 0) > 0)
        add(f'{case}: GPU observations', data.get('gpu_samples_count', 0) > 0)
        if (pair/'supplemental-gpu.jsonl').exists():
            add(f'{case}: authenticated supplemental GPU capture',
                integrity.get(case,{}).get('supplemental_gpu_verified') is True)
        add(f'{case}: cache lookup telemetry', any(legacy().finite(r.get('hit_rate'))
            for r in data.get('server_metrics', {}).get('cache_rates', [])))
    pod = read(run / 'pod-final.json', {})
    ready = any(c.get('type') == 'Ready' and c.get('status') == 'True'
                for c in pod.get('status', {}).get('conditions', []))
    add('Experiment pod retained Running/Ready at final collection',
        pod.get('status', {}).get('phase') == 'Running' and ready,
        dict(name=pod.get('metadata', {}).get('name'), collection_utc=context.get('utc')))
    add('Reproduction development guide', (ROOT / 'development-guide.md').is_file())
    add('Requested comparison plots', all((run/'analysis'/name).is_file() for name in
        ('transfer-cdfs.png', 'bandwidth-by-size.png', 'latency-cdfs.png', 'queue.png', 'gpu.png')))
    return checks


def plots(output, samples, requests, queues, gpu, validation_only=False):
    os.environ.setdefault('MPLCONFIGDIR', '/tmp/agentx-matplotlib')
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.ticker import LogLocator, LogFormatterSciNotation
    plt.rcParams.update({'font.size': 9, 'figure.constrained_layout.use': True})
    def save(fig, name):
        if validation_only:
            title = fig._suptitle.get_text() if fig._suptitle else ''
            fig.suptitle('HISTORICAL FIXTURE VALIDATION ONLY' + ('\n'+title if title else ''))
        fig.savefig(output / (name+'.png'), dpi=170)
        fig.savefig(output / (name+'.pdf'))
        plt.close(fig)
    groups = sorted({r['series'] for r in samples})
    colors = {row['series']:CASE_COLORS[row['case']] for row in samples}
    fig, axes = plt.subplots(2, 2, figsize=(12, 8))
    titles = ('Remote payload bytes', 'KVCR blocks / Mooncake Store objects',
              'Native NIXL / whole GET duration (s)', 'Native NIXL / whole GET effective GB/s')
    for ax, metric, title in zip(axes.flat, METRICS, titles):
        for group in groups:
            xy = list(legacy().ecdf(r.get(metric) for r in samples if r['series']==group))
            if xy:
                ax.step(*zip(*xy), where='post', label=group, color=colors[group])
        ax.set(xlabel=title, ylabel='CDF', ylim=(0, 1.01))
        ax.set_xscale('symlog', linthresh=1e-6 if metric=='seconds' else 1)
        positive = [r[metric] for r in samples if legacy().finite(r.get(metric)) and r[metric]>0]
        if positive:
            has_zero = any(r.get(metric)==0 for r in samples)
            if not has_zero:
                ax.set_xscale('log')
                if metric=='GBps':
                    ax.xaxis.set_minor_locator(LogLocator(subs=(2,5)))
                    ax.xaxis.set_minor_formatter(LogFormatterSciNotation(labelOnlyBase=False,
                        minor_thresholds=(float('inf'),float('inf'))))
            ax.set_xlim(left=0 if has_zero else min(positive)*.8, right=max(positive)*1.2)
        ax.grid(alpha=.25)
    if groups:
        axes[0, 0].legend(fontsize=7)
    fig.suptitle('Cross-worker transfers: remote bytes only; timing scopes differ')
    save(fig, 'transfer-cdfs')
    edges = legacy().BYTE_EDGES
    fig, axes = plt.subplots(2, 4, figsize=(16, 8))
    for ax, lo, hi in zip(axes.flat, edges, edges[1:]):
        for group in groups:
            xy = list(legacy().ecdf(r['GBps'] for r in samples if r['series']==group and lo <= r['bytes'] < hi))
            if xy:
                ax.step(*zip(*xy), where='post', label=group, color=colors[group])
        ax.set(title=f'{lo/2**20:g}–{hi/2**20:g} MiB', xlabel='Effective GB/s', ylabel='CDF', ylim=(0,1.01))
        if not ax.lines:
            ax.text(.5,.5,'No samples',ha='center',va='center',transform=ax.transAxes,color='0.4')
        ax.grid(alpha=.25)
    legends = {}
    for ax in axes.flat:
        handles, labels = ax.get_legend_handles_labels()
        legends.update(zip(labels,handles))
    if legends:
        fig.legend(list(legends.values()),list(legends),loc='outside lower center',ncol=2,fontsize=7)
    save(fig, 'bandwidth-by-size')
    operations=operation_clock_samples(samples)
    operation_series=sorted({r['series'] for r in operations})
    operation_colors={r['series']:CASE_COLORS[r['case']] for r in operations}
    fig,axes=plt.subplots(1,2,figsize=(14,5))
    for ax,metric in zip(axes,('seconds','GBps')):
        for label in operation_series:
            xy=list(legacy().ecdf(r[metric] for r in operations if r['series']==label))
            if xy:ax.step(*zip(*xy),where='post',label=label,color=operation_colors[label])
        ax.set(xlabel='Source/GET operation '+('seconds' if metric=='seconds' else 'effective GB/s'),
               ylabel='CDF',ylim=(0,1.01),xscale='log')
        ax.grid(alpha=.25)
    handles,labels=axes[0].get_legend_handles_labels()
    if handles:fig.legend(handles,labels,loc='outside lower center',fontsize=7,ncol=2)
    fig.suptitle('Operation clocks: source operation and Store GET boundaries differ')
    save(fig,'source-operation-cdfs')
    fig, axes = plt.subplots(1, 3, figsize=(14, 4))
    for ax, metric in zip(axes, ('time_to_first_token','time_per_output_token','request_latency')):
        for case in CASES:
            xy = list(legacy().ecdf(request_seconds(r, metric) for r in requests if r['case']==case))
            if xy:
                ax.step(*zip(*xy), where='post', label=LABELS[case],color=CASE_COLORS[case])
        ax.set(xlabel=metric+' (s)', ylabel='CDF', ylim=(0,1.01), xscale='symlog')
        values=[request_seconds(r,metric) for r in requests]
        positive=[v for v in values if legacy().finite(v) and v>0]
        if positive:
            zero=any(v==0 for v in values)
            ax.set_xscale('symlog',linthresh=1e-6) if zero else ax.set_xscale('log')
            ax.set_xlim(left=0 if zero else min(positive)*.8,right=max(positive)*1.2)
        ax.grid(alpha=.25)
    if requests:
        axes[0].legend(fontsize=7)
    save(fig, 'latency-cdfs')
    fig, axes = plt.subplots(1, 2, figsize=(12, 4))
    for case, data in queues.items():
        rows = data['rows']
        if rows:
            axes[0].plot([r['elapsed_seconds'] for r in rows], [r['queue_depth'] for r in rows], label=LABELS[case],color=CASE_COLORS[case])
            xy = list(legacy().ecdf(r['queue_depth'] for r in rows))
            axes[1].step(*zip(*xy), where='post', label=LABELS[case],color=CASE_COLORS[case])
    axes[0].set(xlabel='Profiling send-window elapsed (s)', ylabel='Waiting requests, sum across ranks')
    axes[1].set(xlabel='Waiting requests', ylabel='CDF',ylim=(0,1.01))
    if any(q['rows'] for q in queues.values()):
        axes[0].legend(fontsize=7)
    save(fig, 'queue')
    fig, axes = plt.subplots(1, 2, figsize=(12, 4))
    for case in CASES:
        for ax, metric in zip(axes, ('gpu_percent', 'used_MiB')):
            xy = list(legacy().ecdf(r.get(metric) for r in gpu if r['case']==case))
            if xy:
                ax.step(*zip(*xy), where='post', label=LABELS[case],color=CASE_COLORS[case])
            ax.set(xlabel=metric+' (all ranks, unweighted observations)', ylabel='CDF',ylim=(0,1.01))
    if gpu:
        axes[0].legend(fontsize=7)
    save(fig, 'gpu')


def analyze(run, validation_only=False):
    output = run / 'analysis';output.mkdir(parents=True, exist_ok=True)
    manifest = read(run / 'manifest.json', {})
    config = manifest.get('experiment', read(ROOT / 'config/experiment.json'))
    used = {Path(__file__).resolve(), ROOT/'server_metrics_phase.py', ROOT/'vendor/b200-dsv4pro/scripts/analyze-paired.py',
        ROOT/'vendor/b200-dsv4pro/mooncake/cache_telemetry.py',ROOT/'runtime/commands.py',run/'manifest.json'}
    used.update((ROOT/'reference/pin-wait-interpretation.md',
        ROOT/'vendor/b200-dsv4pro/telemetry/upstream/remote_fw_dram.py',
        ROOT/'vendor/b200-dsv4pro/telemetry/upstream/types.py'))
    used.update((run/'supplemental-monitor-source').glob('*.py'))
    backends, samples, requests, gpu, stages, gets = {}, [], [], [], [], []
    cache_requests=[];cache_request_coverage=[];cache_request_bins=[];cache_correlations=[]
    api_cache_requests=[];api_cache_coverage=[];api_cache_bins=[];api_cache_correlations=[];api_native_checks=[]
    exports, cache, histograms, outcomes, hca, queue_rows, request_summary = [], [], [], [], [], [], []
    workload_summary=[];source_outcomes=[];ownership_rows=[]
    for case in CASES:
        backend = 'mooncake' if case == 'mooncake' else 'kvcr'
        pair = run / 'cases' / case / 'pair'
        data = legacy().backend_data(pair, backend, used)
        if backend=='kvcr':
            source_outcomes.extend(dict(row,case=case)for row in kvcr_source_outcomes(data,used))
        admission = (data.get('exists', {}) if data.get('capture_valid') is True
                     and data.get('exists',{}).get('admitted_requests',0)>0 else {})
        independent = {metric: admission[key] for metric,key in (
            ('vllm:prefix_cache_queries','prompt_tokens'),
            ('vllm:prefix_cache_hits','gpu_cached_tokens'),
            ('vllm:external_prefix_cache_queries','external_prefix_token_queries'),
            ('vllm:external_prefix_cache_hits','external_prefix_token_matches')) if key in admission}
        data['server_metrics'] = reconcile_server_metrics(
            Path(data['source_directory'])/'replay-measured/server_metrics_export.json',
            data['server_metrics'], data['phase_bounds_ns'], used, data['issues'], independent)
        data['requests'],data['request_phase_accounting']=profiling_requests(
            Path(data['source_directory'])/'replay-measured/profile_export.jsonl',backend,data['issues'])
        for row in data.pop('samples'):
            samples.append(dict(row, case=case, series=LABELS[case]+' / '+row['dataset']))
        if backend=='kvcr':
            owned=transfer_ownership([r for r in samples if r['case']==case],data.get('agent_ownership',{}))
            ownership_rows.extend(dict(row,case=case)for row in owned)
            data['transfer_ownership']=dict(samples=len(owned),verified=all(r['distinct_processes_verified']for r in owned))
        case_requests = data['requests']
        joined,coverage,join_issues=request_cache_samples(data,backend,used)
        data['issues'].extend(join_issues)
        data['request_cache_join']=coverage
        cache_request_coverage.append(dict(case=case,**coverage))
        cache_requests.extend(dict(row,case=case) for row in joined)
        correlations,bins=cache_diagnostics(joined,case,('prompt_tokens','uncached_prompt_tokens','gpu_cached_tokens','external_cached_tokens'))
        cache_correlations.extend(correlations);cache_request_bins.extend(bins)
        api_rows,api_coverage=api_cache_samples(case_requests)
        api_cache_requests.extend(dict(row,case=case) for row in api_rows)
        api_cache_coverage.append(dict(case=case,**api_coverage))
        correlations,bins=cache_diagnostics(api_rows,case,('prompt_tokens','uncached_prompt_tokens','cached_prompt_tokens'))
        api_cache_correlations.extend(correlations);api_cache_bins.extend(bins)
        native_by_id={r['request_id']:r for r in joined};compared=mismatched=0
        for row in api_rows:
            native=native_by_id.get(row['request_id'])
            if native is not None:
                compared+=1
                mismatched+=row['prompt_tokens']!=native['prompt_tokens'] or row['cached_prompt_tokens']!=native['gpu_cached_tokens']+native['external_cached_tokens']
        api_native_checks.append(dict(case=case,compared=compared,mismatched=mismatched))
        data['api_cache_accounting']=dict(api_coverage,native_compared=compared,native_mismatched=mismatched)
        if mismatched:data['issues'].append(f'{mismatched} API/native per-request cache accounting mismatches')
        requests.extend(dict(r, case=case) for r in case_requests)
        legacy().write_csv(output / f'{case}-request-samples.csv', case_requests)
        legacy().write_csv(output / f'{case}-failures.csv', data.pop('failures'))
        stage_rows = data.pop('stages')
        legacy().write_csv(output / f'{case}-stage-samples.csv', stage_rows)
        grouped = defaultdict(list)
        for row in stage_rows:
            grouped[(row['origin'],row['stage'],row['status'])].append(row)
        for (origin,stage,status), rows in grouped.items():
            stages.append(dict(case=case, origin=origin, stage=stage, status=status,
                observations=len(rows), calls=sum(r.get('calls') or 0 for r in rows),
                **legacy().stats(r.get('seconds') for r in rows)))
        del stage_rows, grouped
        data['queue'] = queue_samples(pair / backend / 'health.jsonl', data['phase_bounds_ns'], config['duration_seconds'])
        queue_rows.extend(dict(r, case=case) for r in data['queue']['rows'])
        bounds = data['phase_bounds_ns']
        supplemental = pair / 'supplemental-gpu.jsonl'
        gpu_path = supplemental if supplemental.exists() else pair / backend / 'phases/measured/logs/gpu.jsonl'
        if gpu_path.exists():
            used.add(gpu_path)
        else:
            data['issues'].append(f'GPU observations missing: {gpu_path}')
        for row in legacy().read_jsonl(gpu_path, data['issues']) if gpu_path.exists() else ():
            if bounds is None or not bounds[0] <= row.get('wall_ns',0) < min(bounds[1],bounds[0]+config['duration_seconds']*1e9):
                continue
            for values in row.get('rows', []):
                if len(values) != len(row.get('fields', [])):
                    continue
                item = dict(case=case, wall_ns=row['wall_ns'])
                for field,value in zip(row['fields'],values):
                    try:
                        item[field] = float(value)
                    except ValueError:
                        item[field] = None
                gpu.append(item)
        data['gpu_samples_count'] = sum(r['case']==case for r in gpu)
        stamps=sorted({r['wall_ns'] for r in gpu if r['case']==case})
        data['gpu_coverage'] = dict(source=str(gpu_path), samples=len(stamps),
            first_valid_elapsed_seconds=(stamps[0]-bounds[0])/1e9 if stamps and bounds else None,
            last_valid_elapsed_seconds=(stamps[-1]-bounds[0])/1e9 if stamps and bounds else None,
            max_observed_gap_seconds=max(((b-a)/1e9 for a,b in zip(stamps,stamps[1:])),default=None),
            scope='Unavailable intervals remain missing; no imputation or duplicate native/supplemental weighting')
        if backend == 'mooncake':
            gets, issues = mooncake_gets(data, used)
            data['issues'].extend(issues)
        for metric in ('time_to_first_token','time_per_output_token','inter_token_latency','request_latency'):
            request_summary.append(dict(case=case, metric=metric, unit='s',
                **legacy().stats(request_seconds(r,metric) for r in case_requests)))
        for metric in ('input_sequence_length','output_sequence_length','usage_prompt_cache_read_tokens'):
            values=[r[metric] for r in case_requests if r.get(metric+'_unit')=='tokens' and legacy().finite(r.get(metric))]
            workload_summary.append(dict(case=case,metric=metric,unit='tokens',
                total=sum(values) if values else None,**legacy().stats(values)))
        for metric,value in (data['profile'] or {}).items():
            if isinstance(value,dict) and any(k in value for k in ('avg','p50','count')):
                exports.append(dict(value, case=case, metric=metric))
        cache.extend(dict(r,case=case) for r in data['server_metrics']['cache_rates'])
        histograms.extend(dict(r,case=case) for r in data['server_metrics']['request_histograms'])
        hca.extend(dict(r,case=case) for r in data['hca']['summary'])
        outcomes.append(dict(data['request_outcomes'],case=case, usable_requests=len(case_requests),
            **data['request_phase_accounting'],
            operation_counts=data['operation_counts']))
        backends[case] = data
    summaries, cdfs, bins, bin_cdfs, exact = [], [], [], [], []
    for series in sorted({r['series'] for r in samples}):
        rows = [r for r in samples if r['series']==series]
        for metric in METRICS:
            key = dict(series=series,metric=metric)
            summaries.append({**key,**legacy().stats(r.get(metric) for r in rows)})
            cdfs.extend(dict(key,value=v,cdf=p) for v,p in legacy().ecdf(r.get(metric) for r in rows))
        for lo,hi in zip(legacy().BYTE_EDGES,legacy().BYTE_EDGES[1:]):
            key = dict(series=series,min_bytes_inclusive=lo,max_bytes_exclusive=hi if math.isfinite(hi) else None)
            selected = [r for r in rows if lo <= r['bytes'] < hi]
            for metric in ('seconds','GBps'):
                bins.append(dict(key,metric=metric,**legacy().stats(r.get(metric) for r in selected)))
                bin_cdfs.extend(dict(key,metric=metric,value=v,cdf=p)
                    for v,p in legacy().ecdf(r.get(metric) for r in selected))
        for size in sorted({r['bytes'] for r in rows}):
            selected = [r for r in rows if r['bytes']==size]
            for metric in ('seconds','GBps'):
                exact.append(dict(series=series,bytes=size,metric=metric,
                    **legacy().stats(r.get(metric) for r in selected)))
    get_classes = []
    observed_classes={r['get_class'] for r in gets}
    if backends.get('mooncake',{}).get('capture_valid') is True:
        observed_classes.update(('pure_remote','mixed','local_only','unknown','failed'))
    for kind in sorted(observed_classes):
        rows = [r for r in gets if r['get_class']==kind]
        get_classes.append(dict(get_class=kind,operations=len(rows),
            **{f'{k}_successful_{u}':sum(r[f'{k}_successful_{u}'] for r in rows)
               for k in ('local','remote','unknown') for u in ('bytes','objects')}))
    operation_samples=operation_clock_samples(samples)
    throughput=[]
    for case, data in backends.items():
        profile=data.get('profile') or {};seconds=data.get('phase_seconds')
        tokens=sum(r.get('output_sequence_length') or 0 for r in data['requests'])
        outcome=data.get('request_outcomes',{})
        throughput.append(dict(case=case,completed=len(data['requests']),sent=outcome.get('sent_credits'),
            cancelled=outcome.get('cancelled_credits'),request_errors=outcome.get('reported_errors'),
            output_tokens=tokens if data['requests'] else None,phase_seconds=seconds,
            output_tokens_per_second=tokens/seconds if data['requests'] and seconds else None,
            sdk_input_tokens_per_second=profile.get('input_token_throughput',{}).get('avg'),
            sdk_output_tokens_per_second=profile.get('output_token_throughput',{}).get('avg'),
            sdk_requests_per_second=profile.get('request_throughput',{}).get('avg')))
    operation_summary=[];operation_cdfs=[]
    for label in sorted({r['series'] for r in operation_samples}):
        rows=[r for r in operation_samples if r['series']==label]
        for metric in ('seconds','GBps'):
            operation_summary.append(dict(series=label,metric=metric,
                **legacy().stats(r[metric] for r in rows)))
            operation_cdfs.extend(dict(series=label,metric=metric,value=v,cdf=p)
                for v,p in legacy().ecdf(r[metric] for r in rows))
    source_summary=[];groups=defaultdict(list)
    source_keys=('case','status','result','payload_class','native_submitted','native_state','reason')
    for row in source_outcomes:groups[tuple(row[k]for k in source_keys)].append(row)
    for key,rows in groups.items():
        source_summary.append(dict(zip(source_keys,key),operations=len(rows),
            bytes=sum(r['bytes']for r in rows if legacy().finite(r['bytes'])),
            blocks=sum(r['blocks']for r in rows if legacy().finite(r['blocks'])),
            **legacy().stats(r['source_operation_seconds']for r in rows)))
    matched_sizes=matched_kvcr_sizes(samples)
    comparisons=[]
    for row in throughput:
        case=row['case']
        if not row['completed']:continue
        item=dict(case=case,completed=row['completed'],output_tokens_per_second=row['output_tokens_per_second'])
        for metric,name in (('time_to_first_token','ttft_seconds'),('time_per_output_token','tpot_seconds')):
            found=next(r for r in request_summary if r['case']==case and r['metric']==metric)
            for stat in ('mean','p50','p90','p99'):item[name+'_'+stat]=found[stat]
        uncached=[r['uncached_prompt_tokens']for r in api_cache_requests if r['case']==case]
        item['mean_remaining_prompt_tokens']=legacy().stats(uncached)['mean']
        for metric,name in (('vllm:request_prefill_time_seconds','server_prefill_interval_mean_seconds'),
                            ('vllm:request_queue_time_seconds','server_queue_mean_seconds')):
            item[name]=next((r['mean_seconds']for r in histograms if r['case']==case and r['metric']==metric),None)
        comparisons.append(item)
    slow_requests=[r for case in CASES for r in sorted(
        (r for r in cache_requests if r['case']==case),key=lambda r:r['ttft_seconds'],reverse=True)[:10]]
    outputs = {'transfer-samples': samples, 'transfer-summary': summaries, 'transfer-cdf': cdfs,
        'source-operation-samples':operation_samples,'source-operation-summary':operation_summary,
        'source-operation-cdf':operation_cdfs,
        'kvcr-source-outcome-samples':source_outcomes,'kvcr-source-outcome-summary':source_summary,
        'kvcr-transfer-ownership':ownership_rows,
        'matched-kvcr-size-summary':matched_sizes,'performance-comparison':comparisons,
        'bandwidth-size-summary': bins, 'bandwidth-size-cdf': bin_cdfs, 'exact-size-summary': exact,
        'request-latency-summary':request_summary,'e2e-profile-metrics':exports,'throughput-summary':throughput,'cache-hit-rates':cache,
        'request-workload-summary':workload_summary,
        'request-cache-samples':cache_requests,'request-cache-coverage':cache_request_coverage,
        'request-cache-ttft-bins':cache_request_bins,'request-cache-correlations':cache_correlations,
        'request-cache-slowest':slow_requests,
        'request-api-cache-samples':api_cache_requests,'request-api-cache-coverage':api_cache_coverage,
        'request-api-cache-correlations':api_cache_correlations,'request-api-cache-ttft-bins':api_cache_bins,
        'request-api-native-crosscheck':api_native_checks,
        'server-histograms':histograms,'request-outcomes':outcomes,'stage-summary':stages,
        'queue-samples':queue_rows,'queue-summary':[dict(case=c,**d['queue']['queue'],
            unavailable_samples=d['queue']['unavailable_samples']) for c,d in backends.items()],
        'gpu-samples':gpu,'hca-summary':hca,'mooncake-get-samples':gets,'mooncake-get-classes':get_classes,
        'gpu-coverage':[dict(case=c,**d['gpu_coverage']) for c,d in backends.items()],
        'host-store-key-rates':[dict(case=c,**d['exists']) for c,d in backends.items()]}
    for name,rows in outputs.items():
        legacy().write_csv(output / (name+'.csv'),rows)
    plots(output,samples,requests,{c:d['queue'] for c,d in backends.items()},gpu,validation_only)
    request_cache_plot(output,cache_requests,validation_only)
    request_cache_plot(output,api_cache_requests,validation_only,stem='request-api-cache-ttft',
        title='Completed requests: server-reported total cached prefix tokens')
    checks = completion_checks(run,config,backends)
    if validation_only:
        checks.append(dict(requirement='Actual AgentX run, not fixture validation',passed=False))
    complete = all(r['passed'] for r in checks)
    audit = dict(complete=complete,validation_only=validation_only,utc=time.time(),checks=checks)
    legacy().write_json(output/'completion-audit.json',audit)
    legacy().write_json(output/'summary.json',dict(experiment=config,validation_only=validation_only,
        backends={c:{k:v for k,v in d.items() if k not in ('requests','queue')} for c,d in backends.items()}))
    inputs = [dict(path=str(p),sha256=legacy().digest(p),bytes=p.stat().st_size) for p in sorted(used) if p.is_file()]
    legacy().write_json(output/'analysis-inputs.json',inputs)
    def table(rows, columns):
        return '\n'.join(legacy().table(rows, columns))
    state = 'FIXTURE VALIDATION ONLY — historical observations, not AgentX results' if validation_only else (
        'Complete measured AgentX comparison' if complete else 'INCOMPLETE — diagnostic artifacts; AgentX completion unproven')
    gpu_notice = ('The original Mooncake GPU sampler exited during startup; the '
        'recovery cannot reconstruct its missing initial samples. Native '
        'cache/transfer/request telemetry is independent.'
        if (run/'supplemental-gpu-launch.json').exists() else '')
    findings=[]
    baseline=next((r for r in comparisons if r['case']=='mooncake'),None)
    for row in comparisons:
        case=row['case']
        if baseline and case!='mooncake' and baseline['output_tokens_per_second']:
            findings.append(f"{LABELS[case]} delivered {row['output_tokens_per_second']:.2f} output tokens/s "
                f"({row['output_tokens_per_second']/baseline['output_tokens_per_second']:.3f}× Mooncake in this run). "
                f"Its completed requests have mean TTFT {row['ttft_seconds_mean']:.3f}s versus "
                f"{baseline['ttft_seconds_mean']:.3f}s, and mean remaining prompt "
                f"{row['mean_remaining_prompt_tokens']:,.1f} versus {baseline['mean_remaining_prompt_tokens']:,.1f} tokens. "
                "These are observed closed-loop cohorts; neither ratio isolates the connector's causal effect.")
        corr={r['metric']:r['spearman_rho']for r in api_cache_correlations if r['case']==case}
        if all(legacy().finite(corr.get(k))for k in ('uncached_prompt_tokens','prompt_tokens')):
            findings.append(f"{LABELS[case]}: TTFT rank correlation is {corr['uncached_prompt_tokens']:.3f} "
                f"with remaining prompt tokens and {corr['prompt_tokens']:.3f} with full prompt tokens. "
                "This supports examining cache reuse and remaining prefill alongside transport; it does not measure GPU kernel time.")
        if case!='mooncake':
            payload=[r for r in samples if r['case']==case]
            outcomes_for_case=[r for r in source_outcomes if r['case']==case]
            if outcomes_for_case:
                zeros=sum(r['payload_class']=='zero'for r in outcomes_for_case)
                findings.append(f"{LABELS[case]}: {len(payload)} positive-byte cross-agent transfers "
                    f"carried {sum(r['bytes']for r in payload):,} bytes; {zeros}/{len(outcomes_for_case)} "
                    "source terminal outcomes carried zero bytes. Zero-payload partial results are outside the transfer CDF, "
                    "while their control-path time remains in source outcome/stage statistics. "
                    "Improve useful remote reuse and investigate source availability/pin outcomes before assuming more RNICs will improve TTFT.")
                if len(payload)<100:
                    findings.append(f"{LABELS[case]} has only {len(payload)} positive payload samples: "
                        "its p90/p99 are order statistics of a sparse cohort, not stable network-tail estimates.")
    pin_rows=[r for r in stages if r['origin']=='duration'and r['stage']=='framework_pin_wait']
    source_clock_rows=[r for r in stages if r['origin']=='source_terminal_clock'and r['stage']=='source_operation_seconds']
    control_rows=pin_rows+source_clock_rows
    dual=next((r for r in comparisons if r['case']=='kvcr-dual'),None)
    single=next((r for r in comparisons if r['case']=='kvcr-single'),None)
    if dual and single and single['output_tokens_per_second']:
        findings.append(f"Dual versus single KVCR: observed output-token throughput ratio "
            f"{dual['output_tokens_per_second']/single['output_tokens_per_second']:.3f}×; "
            f"mean TTFT {dual['ttft_seconds_mean']:.3f}s versus {single['ttft_seconds_mean']:.3f}s. "
            f"There are {len(matched_sizes)} exact payload sizes present in both transfer cohorts. "
            "The size-matched table controls bytes only, not source/target rank, SGL layout or contention; "
            "sample counts and different completed trace turns still limit attribution.")
    interpretation='\n\n'.join(findings)or'No completed measured request cohort is collected yet.'
    report = f'''# AgentX DSV4-Pro: Mooncake, single-RNIC KVCR and dual-RNIC KVCR

**{state}.** See [completion audit](completion-audit.json) for every missing gate.
The three cases run sequentially on the same retained eight-B200 pod. The common
serving stack retains the validated DSpark model and speculative configuration;
it differs from the older vLLM/MTP configuration in the reference IX runbook.
All 393 source traces are available; concurrency 72 and a one-hour closed-loop
window do not imply that all source turns complete or that case request cohorts match.

## End-to-end performance and queueing

{table(throughput,('case','sent','completed','cancelled','request_errors','phase_seconds','sdk_input_tokens_per_second','output_tokens_per_second','sdk_requests_per_second'))}

Throughput uses profiling including drain/cancellation completion time, rather
than only the 3,600-second send window. [Throughput accounting](throughput-summary.csv)
retains completed token totals, the measured denominator and original SDK rates.
Input throughput counts reported input tokens including cached prefix tokens;
it is not an executed-prefill-token or GPU-compute throughput measurement.

{table(workload_summary,('case','metric','unit','count','mean','p50','p90','p99'))}

Input/output token length distributions describe the completed workload mix.
Closed-loop cases can complete different trace turns and output lengths, so
request throughput alone is not an output-token capacity comparison.
Exact totals are in [workload accounting](request-workload-summary.csv).

{table(request_summary,('case','metric','count','mean','p50','p90','p99'))}

Latency units in this table are seconds. TPOT is calculated per completed request
as (latency − TTFT)/(output tokens − 1); ITL is reported separately. Throughput,
raw exporter statistics and units are in [profile metrics](e2e-profile-metrics.csv).
CDF statistics use nearest-rank empirical percentiles; the SDK's interpolated
percentiles are preserved separately in the profile metrics export.
Failures, cancellations and usable response counts are in [request outcomes](request-outcomes.csv).
Incomplete requests do not enter latency CDFs and must not be hidden when comparing cases.
The streaming JSONL exporter contains warmup records too; only rows explicitly
tagged `metadata.benchmark_phase=profiling` enter these request distributions.
Unknown-phase rows are excluded and make the completion audit fail.

![Latency CDFs](latency-cdfs.png)
![Queue time series and CDF](queue.png)

Queue observations cover the configured profiling send window, excluding warmup
and drain. Missing observations are unavailable, not zero. See [queue statistics](queue-summary.csv)
and [server queue/prefill histograms](server-histograms.csv); histogram cohorts
and completed response cohorts may differ.
Some pinned server-export series repeat warmup cumulative values at profiling
start. Series whose exported window already starts inside profiling are retained
without subtraction after slice/total and nonzero-boundary checks. Other server
counters and histograms subtract a matched warmup offset only when
each endpoint/worker's first nonzero profiling slice exactly proves that offset
(or an additional first-slice residual is reconciled against independently
instrumented admission totals),
the slice sums match the exported total, and nonzero slices fit the phase bounds.
Unverified series remain unavailable. Raw exports are retained unchanged.
This validates the phase offset; it cannot reconstruct unobservable counter resets.
For already scoped counters, the SDK aggregate includes the pre-window reference
contribution that timeslices omit at their first boundary. That nonnegative
contribution is retained separately; interval continuity and phase bounds remain
checked. The per-series evidence records the value and method explicitly.

{table(histograms,('case','metric','count','mean_seconds'))}

The vLLM prefill histogram spans first scheduling to first token, including any
intervening preemption or cache wait. It is not a measurement of GPU compute
alone. Compare these observations with queue lengths and transfer timings before
attributing a long TTFT to an entrance backlog or to RDMA.

## Request-level cache and TTFT diagnosis

{table(api_cache_coverage,('case','completed_requests','usable_cache_records','missing_cache_field','invalid_cache_counts','invalid_units'))}

Server-reported `usage_prompt_cache_read_tokens` provides total cached prefix
tokens for each completed request. Subtracting it from server-reported input
tokens gives a comparable remaining-prompt measure across all three backends.
This field does not split GPU, host and remote tiers, or measure executed GPU
kernels; tier attribution continues to use independent counters and native data.
Missing/invalid fields remain unavailable, while a reported zero is retained.

![TTFT versus API-reported remaining prompt](request-api-cache-ttft.png)

{table(api_cache_correlations,('case','metric','paired_requests','spearman_rho'))}

{table([r for r in api_cache_bins if r['count']],('case','min_uncached_tokens','max_uncached_tokens_exclusive','count','mean','p50','p90','p99'))}

Bin values are client TTFT in seconds. See [API cache samples](request-api-cache-samples.csv),
[coverage](request-api-cache-coverage.csv), [correlations](request-api-cache-correlations.csv)
and [size-conditioned latency statistics](request-api-cache-ttft-bins.csv).

### Independent scheduler admission cross-check

{table(api_native_checks,('case','compared','mismatched'))}

{table(cache_request_coverage,('case','completed_requests','matched','match_fraction','ambiguous_frontend','ambiguous_admission','invalid_counts'))}

The join uses the frontend's explicit client-ID to server-ID mapping and the
exact scheduler request ID (including removal of the known vLLM hash suffix).
Only completed profiling responses with a unique, non-preempted admission enter
this analysis. Ambiguous or missing matches are excluded without timestamp
guessing. Mooncake scheduler instrumentation provides this detail; if a backend
does not emit these admission records, its request-level cache breakdown remains
unavailable here. Aggregate cache counters and transfer observations are separate.

![TTFT versus total and uncached prompt tokens](request-cache-ttft.png)

{table(cache_correlations,('case','metric','paired_requests','spearman_rho'))}

{table([r for r in cache_request_bins if r['count']],('case','min_uncached_tokens','max_uncached_tokens_exclusive','count','mean','p50','p90','p99'))}

The bin values are client TTFT in seconds. Uncached prompt tokens are prompt
minus GPU-cached minus external-cached tokens at admission; this is remaining
prefill work, not a measured GPU kernel duration or RDMA payload size. Rank
correlation is descriptive and does not isolate a causal effect. Inspect
[request-level evidence](request-cache-samples.csv), [join coverage](request-cache-coverage.csv)
and [ten slowest joined requests per case](request-cache-slowest.csv) before
attributing tails to transport. Zero external cached tokens means no external
prefix reuse was credited, not that no metadata/cache lookup was attempted.
This completed-response cohort excludes requests without a completed latency;
retain cancellation counts when interpreting its tail. Aggregate cache counters
cover admitted requests, including those later canceled.

## Tiered cache hits and Mooncake GET locality

{table(cache,('case','cache','endpoint','hits','queries','unit','hit_rate'))}

KVCR allocates 25 GiB/rank to framework primary host cache and 100 GiB/rank to
guard storage. Mooncake allocates 100 GiB/rank Store plus 25 GiB/rank transfer
buffer; that buffer is not an additional framework host cache. GPU counters use
tokens, KVCR host/guard counters use chunks, and Store lookup counters use keys.
Guard hits include remote-fetchable hints. External-prefix counters describe all
connector sources and are not an independent extra cache tier. Unavailable or
inconsistent counters remain null; counter reset correction is not assumed.
External-prefix token queries count the prompt remaining after the GPU prefix
lookup. Its conditional hit rate must not be added to the GPU-prefix hit rate;
the two denominators differ. Store key and framework chunk rates also have
different units and lookup populations.

{table(get_classes,('get_class','operations','local_successful_bytes','remote_successful_bytes','unknown_successful_bytes'))}

Store lookup hit rates are in [host Store key rates](host-store-key-rates.csv).
GET locality fractions describe served data, not lookup hit rates. Pure remote,
mixed, local-only, unknown and failed GETs remain separate in the raw classification.
Zero-operation categories are shown only after the Mooncake capture is validated.
They mean no such instrumented GET API batch was observed; no bandwidth estimate
or CDF exists for an empty category. A missing/uncollected capture is not zero.

## Cross-worker transfer distributions and effective bandwidth

**Mooncake remote GET byte and object counts exclude local portions.** Mixed GET
effective bandwidth uses remote successful bytes divided by the whole GET API
duration; that duration still includes metadata and local work. KVCR uses native
NIXL xferDuration. These clocks must not be interpreted as identical RDMA timing.
Counts mean logical KVCR blocks versus Mooncake Store objects; they are not
interchangeable physical blocks. Zero-byte lookups are excluded from transfer CDFs.
KVCR source/target agent IDs are independently mapped to process owners using
startup `identity_ready` events, including their file/line provenance in the
summary's agent ownership map. Every measured payload must have unique, distinct
process owners for the completion audit to pass; different agent names alone
are insufficient. See [per-transfer ownership checks](kvcr-transfer-ownership.csv).
KVCR successful partial reuse retains actual transferred bytes/blocks and the
requested block count; Mooncake transfer CDFs exclude failed GET batches. Keep
these completion cohorts separate when attributing a performance difference.

{table(summaries,('series','metric','count','mean','p50','p90','p99'))}

![Cross-worker CDFs](transfer-cdfs.png)
![Effective bandwidth CDFs conditional on remote byte size](bandwidth-by-size.png)

Raw [transfer samples](transfer-samples.csv), [CDF points](transfer-cdf.csv),
[size-conditioned statistics](bandwidth-size-summary.csv), [conditional CDFs](bandwidth-size-cdf.csv)
and [exact-size cohorts](exact-size-summary.csv) retain mean/p50/p90/p99 and sample
counts. Size-mix differences and sparse cohorts prevent causal speedup claims
from unconditional averages. PDF versions of every plot are also provided.

## Control-path cost, errors and utilization

See [operation timing statistics](stage-summary.csv) and each case's failure CSV.
The complete KVCR source-operation clock is also shown separately from native
NIXL time. It is never inferred from native time when absent. Mooncake retains
whole GET API time and remote-only byte numerators. These API boundaries differ;
this view helps inspect control-path cost without claiming identical RDMA clocks.

{table(operation_summary,('series','metric','count','mean','p50','p90','p99'))}

![Source operation / GET API CDFs](source-operation-cdfs.png)

See [operation-clock samples](source-operation-samples.csv),
[statistics](source-operation-summary.csv) and [CDF points](source-operation-cdf.csv).
Pin-result waits, metadata observations and native transfer time have different
origins and may overlap; their sums are not an additive request critical path.
Framework pin misses are not automatically RDMA errors. Source terminal errors,
Store return codes and HTTP failures retain their separate accounting.

![GPU utilization and memory](gpu.png)

GPU samples are unweighted five-second observations across ranks.
GPU coverage and missing initial intervals are explicit in [coverage](gpu-coverage.csv);
only the independent supplemental stream is used when present. {gpu_notice}
DRAM samples
are retained in [queue/health data](queue-samples.csv) and raw memory-watch logs.
[HCA summaries](hca-summary.csv) are whole-node context, potentially including
background and loopback traffic; they cannot attribute per-operation wire bytes.
All workers/guards share a node, so this comparison does not establish inter-node
wire bandwidth. Dual-rail proof comes from closed SGL PUT protocol tables and
actual positive-byte cross-agent transfers, not the configured environment alone.

## Interpretation and optimization follow-up

These findings use collected profiling cohorts only. Any uncollected case remains
absent from this comparison; the completion audit determines whether all three
cases are final. [Compact comparison data](performance-comparison.csv) retain
the exact values behind the following table and narrative.

{table(comparisons,('case','completed','output_tokens_per_second','ttft_seconds_mean','ttft_seconds_p99','tpot_seconds_mean','mean_remaining_prompt_tokens','server_queue_mean_seconds','server_prefill_interval_mean_seconds'))}

{interpretation}

### Source outcomes and pin/wait cost

{table(source_summary,('case','status','result','payload_class','native_submitted','native_state','reason','operations','bytes','blocks','mean','p50','p90','p99'))}

The timing statistics in this table are complete source-operation seconds,
including zero-payload outcomes. `native_submitted` unavailable means no such
field was emitted, not an inferred false. `FINISHED` plus partial/success is a
terminal source outcome, not evidence of an RNIC error. Compare actual bytes and
blocks with requested blocks in [all source outcomes](kvcr-source-outcome-samples.csv).
The [grouped outcome summary](kvcr-source-outcome-summary.csv) preserves empty
payloads and unavailable payloads as distinct categories, with source provenance.

{table(control_rows,('case','origin','stage','status','observations','count','mean','p50','p90','p99'))}

Framework `pin_wait` failure is the instrumented framework outcome; it does not
by itself identify an RDMA failure. Compare it with native submission/state and
the retained reason fields. The [fixed-version source review](../../../reference/pin-wait-interpretation.md)
explains the callback/handle distinction and the completed single-RNIC evidence;
it does not claim an unobserved adapter failure cause. Stage observations can overlap or describe the same
operation at different boundaries; their sums must not be treated as a request
critical path. The source-operation CDF above includes positive payloads only,
whereas this outcome table also exposes work that never delivered a payload.

### Exact-size single/dual KVCR comparison

{table(matched_sizes[:32],('bytes','single_samples','dual_samples','single_p50_seconds','dual_p50_seconds','single_p50_GBps','dual_p50_GBps','dual_over_single_p50_GBps'))}

Only exact byte sizes occurring in both KVCR native-transfer cohorts enter this
table (first 32 sizes shown); [the full CSV](matched-kvcr-size-summary.csv)
retains all sizes and mean/p50/p90/p99. An empty table means no matched measured
sizes are available, not zero bandwidth or no multi-rail capability. Sparse
cohorts cannot establish a stable striping speedup. Selected two-rail protocol
weights also do not measure per-operation bytes on each rail.

The end-to-end priority is useful cache reuse and remaining-prefill latency,
then source availability and pin/control latency, with matched-size native
transfers used to evaluate the RNIC path. Mooncake GET locality and API timing
should guide separate metadata/local-copy/remote-transport work: a mixed batch's
whole API clock cannot establish an RDMA advantage over KVCR's native clock.
The DRAM budgets match, but persistent capacity differs: KVCR primary+guard
is 125 GiB/rank while Mooncake Store is 100 GiB/rank plus a nonpersistent
25 GiB transfer buffer. Whole-node HCA counters and same-node traffic cannot
establish inter-node link saturation. One run per case supports descriptive
comparison; repeated controlled runs would be needed for a causal performance claim.

Reproduce with [development guide](../../../development-guide.md). Original
captures remain immutable; [analysis input hashes](analysis-inputs.json) identify
the files used for this report. Pods are retained.
'''
    (output/'report.md').write_text(report)
    print(json.dumps(dict(output=str(output),complete=complete,validation_only=validation_only,
        failed_checks=sum(not r['passed'] for r in checks))))
    return complete


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run',type=Path)
    parser.add_argument('--validation-only',action='store_true')
    args = parser.parse_args()
    raise SystemExit(0 if analyze(args.run.resolve(),args.validation_only) else 1)


if __name__ == '__main__':
    main()
