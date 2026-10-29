#!/usr/bin/env python3
"""Offline direct GPU/Guard supplement. No raw/base writes, GPU or remote access."""
import argparse
from collections import Counter, defaultdict
import csv
import importlib.util
import json
import math
import os
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import compact_format as fmt
from opt_lineage import correlate
from capture_scope import capture_inventory, metadata_parents, measured_rank_paths, separate_canary_evidence
import reference_agentx as ref

spec = importlib.util.spec_from_file_location('direct_opt_clocks', HERE/'analyze-opt.py')
opt = importlib.util.module_from_spec(spec)
spec.loader.exec_module(opt)
legacy = ref.legacy()
PAGE = 1_228_800
GIB = 1024**3
METRICS = ('bytes', 'blocks', 'seconds', 'GBps')


def serial(value):
    if isinstance(value, bytes): return value.hex()
    if isinstance(value, dict): return {str(k): serial(v) for k,v in value.items()}
    if isinstance(value, (tuple,list)): return [serial(v) for v in value]
    if isinstance(value, float) and not math.isfinite(value): return None
    return value


def read_csv(path):
    path = Path(path)
    if not path.exists(): return []
    with path.open() as stream: return list(csv.DictReader(stream))


def inside(stamp, bounds):
    return bounds is not None and legacy.finite(stamp) and bounds[0] <= stamp <= bounds[1]


def job_id(row):
    return (row.get('pid'), row.get('session'), row.get('agent'), row.get('job_id'))


def key_hex(key):
    if not isinstance(key, bytes): raise ValueError('Binary journal key is not bytes')
    return key.hex()


def join_jobs(events, records, bounds, capture_valid):
    """Exact per-process/job joins. Absence of remote work is used only in closed captures."""
    index = defaultdict(lambda: defaultdict(list))
    remote = defaultdict(list)
    target_keys = {}
    for r in records:
        if r.get('event') in ('direct_gpu_submit','direct_gpu_api','direct_gpu_result'):
            index[job_id(r)][r['event']].append(r)
        elif r.get('event') == 'target_get':
            target_keys[(r.get('pid'),r.get('session'),r.get('agent'),r.get('operation_key'))] = r
    for r in events:
        if r.get('event') == 'target_terminal':
            handle = r.get('target_op')
            if handle is None and isinstance(r.get('op_id'), (list, tuple)) and len(r['op_id']) == 2: handle = r['op_id'][1]
            remote[(r.get('pid'),r.get('session'),r.get('agent'),handle)].append(r)
    jobs, key_rows, issues = [], [], []
    unknown_requests=set()
    # Pre-start failures have no target handle. Use actor/request and key-set
    # containment only when exactly one disjoint API batch can own the attempt.
    for owner,targets in list(remote.items()):
        if owner[3] is not None:continue
        for target in targets:
            full=target_keys.get((*owner[:3],target.get('operation_key')))
            candidates=[]
            for identity,chain in index.items():
                if identity[:3] != owner[:3] or len(chain['direct_gpu_submit']) != 1:continue
                submit=chain['direct_gpu_submit'][0]
                if submit.get('request_id') != target.get('request_id'):continue
                if full and full.get('keys'):
                    for api in chain['direct_gpu_api']:
                        if set(full['keys']).issubset(api['keys']):candidates.append((identity,api['op_handle']))
            if len(candidates)==1:
                identity,handle=candidates[0]
                remote[(*identity[:3],handle)].append(target)
            else:
                unknown_requests.add((*owner[:3],target.get('request_id')))
                issues.append('Unmatched/ambiguous pre-start remote target; path remains unknown')
    seen = set()
    for terminal in events:
        if terminal.get('event') != 'direct_gpu_terminal' or not inside(terminal.get('wall_ns'), bounds): continue
        identity = job_id(terminal)
        try:
            if identity in seen: raise ValueError('duplicate job terminal')
            seen.add(identity)
            chain = index[identity]
            if len(chain['direct_gpu_submit']) != 1 or len(chain['direct_gpu_result']) != 1:
                raise ValueError('missing/duplicate full-key submit/result')
            submit, result = chain['direct_gpu_submit'][0], chain['direct_gpu_result'][0]
            if not inside(submit.get('wall_ns'),bounds):
                raise ValueError('job submission outside measured bounds; boundary job excluded')
            keys, gpu = submit['keys'], submit['gpu_indices']
            if not keys or len(keys) != len(gpu) or len(set(keys)) != len(keys):
                raise ValueError('invalid full-key/physical-index cardinality')
            if result['keys'] != keys or result['gpu_indices'] != gpu:
                raise ValueError('result changed submitted key/index mapping')
            good, bad = result['successful_indices'], result['failed_indices']
            if (any(type(i) is not int or not 0 <= i < len(keys) for i in good+bad)
                    or len(set(good+bad)) != len(good)+len(bad)
                    or set(good+bad) != set(range(len(keys)))):
                raise ValueError('result success/failure indices do not partition submitted keys')
            if (terminal.get('requested_blocks') != len(keys)
                    or terminal.get('successful_blocks') != len(good)
                    or terminal.get('failed_blocks') != len(bad)
                    or terminal.get('successful_key_bytes') != len(good)*PAGE):
                raise ValueError('terminal counters do not match binary result partition/page geometry')
            if terminal.get('kind') != submit.get('kind') or result.get('kind') != submit.get('kind'):
                raise ValueError('job kind changed')
            api_keys, remote_requested, remote_success = [], set(), set()
            for api in chain['direct_gpu_api']:
                if api.get('kind') != submit.get('kind') or len(api['keys']) != len(api['gpu_indices']):
                    raise ValueError('invalid API key/index batch')
                mapping = dict(zip(keys,gpu))
                if any(k not in mapping or mapping[k] != i for k,i in zip(api['keys'],api['gpu_indices'])):
                    raise ValueError('API changed GPU physical block mapping')
                api_keys.extend(api['keys'])
                for target in remote[(identity[0],identity[1],identity[2],api['op_handle'])]:
                    full = target_keys.get((identity[0],identity[1],identity[2],target.get('operation_key')))
                    if full is None: raise ValueError('remote target terminal has no full-key journal')
                    requested = full['keys']
                    if not set(requested).issubset(api['keys']): raise ValueError('remote keys outside API batch')
                    remote_requested.update(requested)
                    indices = target.get('completed_indices', []) if target.get('status') in ('success','partial') else []
                    if len(set(indices)) != len(indices) or any(type(i) is not int or not 0 <= i < len(requested) for i in indices):
                        raise ValueError('invalid remote subset indices')
                    remote_success.update(requested[i] for i in indices)
            if len(api_keys) != len(keys) or set(api_keys) != set(keys):
                raise ValueError('API batches do not cover submitted keys exactly once')
            success_keys = {keys[i] for i in good}
            if not remote_success.issubset(success_keys): raise ValueError('remote/API success disagreement')
            # A failed remote request cannot silently become a local success.
            if (remote_requested & success_keys) != remote_success:
                raise ValueError('remote result subset disagrees with successful job keys')
            path = 'unknown'
            if capture_valid and (*identity[:3],submit.get('request_id')) not in unknown_requests:
                path = 'mixed' if remote_requested and remote_requested != set(keys) else 'remote' if remote_requested else 'local'
            start, end = terminal.get('first_api_ns'), terminal.get('cleanup_complete_ns')
            api_seconds = (end-start)/1e9 if legacy.finite(start) and legacy.finite(end) and end >= start else None
            item = dict(serial(terminal), path=path, first_api_to_cleanup_seconds=api_seconds,
                        remote_successful_blocks=len(remote_success),
                        remote_successful_bytes=len(remote_success)*PAGE,
                        local_successful_blocks=len(success_keys-remote_requested) if capture_valid else None,
                        local_successful_bytes=len(success_keys-remote_requested)*PAGE if capture_valid else None,
                        api_batches=len(chain['direct_gpu_api']), chain_valid=True,
                        payload_scope='Successful API key payload, not a wire-byte observation')
            jobs.append(item)
            for i,(key,physical) in enumerate(zip(keys,gpu)):
                key_rows.append(dict(pid=identity[0],session=identity[1],agent=identity[2],job_id=identity[3],
                    kind=submit['kind'],request_id=submit.get('request_id'),key_index=i,key=key_hex(key),
                    gpu_index=physical,success=i in good,path='remote' if key in remote_requested else 'local' if path!='unknown' else 'unknown'))
        except (KeyError,TypeError,ValueError) as error:
            issues.append(f'GPU job {identity}: {error}')
    for identity,chain in index.items():
        if identity not in seen and any(inside(r.get('wall_ns'),bounds) for r in chain['direct_gpu_submit']):
            issues.append(f'GPU job {identity}: submitted in profiling without terminal; unfinished, not inferred failed')
    return jobs, key_rows, issues


def page_bin(size):
    if size==0:return '0'
    pages = size/PAGE
    for upper,label in ((1,'1'),(4,'2-4'),(16,'5-16'),(64,'17-64'),(256,'65-256'),(1024,'257-1024')):
        if pages <= upper: return label
    return '>1024'


def distributions(samples, metrics=METRICS):
    groups = defaultdict(list)
    for r in samples: groups[r['dataset']].append(r)
    tables = {name:[] for name in ('transfer-summary','transfer-cdf','size-bin-summary','size-bin-cdf')}
    for dataset, rows in sorted(groups.items()):
        bins = defaultdict(list)
        for row in rows: bins[page_bin(row['bytes'])].append(row)
        for metric in metrics:
            tables['transfer-summary'].append(dict(dataset=dataset,metric=metric,**legacy.stats(r.get(metric) for r in rows)))
            tables['transfer-cdf'].extend(dict(dataset=dataset,metric=metric,value=v,cdf=p)
                for v,p in legacy.ecdf(r.get(metric) for r in rows))
            for label, selected in bins.items():
                tables['size-bin-summary'].append(dict(dataset=dataset,page_bin=label,page_bytes=PAGE,
                    metric=metric,**legacy.stats(r.get(metric) for r in selected)))
                tables['size-bin-cdf'].extend(dict(dataset=dataset,page_bin=label,page_bytes=PAGE,
                    metric=metric,value=v,cdf=p) for v,p in legacy.ecdf(r.get(metric) for r in selected))
    return tables


def sample(row, dataset, size, blocks, seconds, scope, **extra):
    if not (legacy.finite(size) and size > 0 and legacy.finite(seconds) and seconds > 0): return None
    return dict(legacy.reference(row), dataset=dataset,bytes=size,blocks=blocks,seconds=seconds,
                GBps=size/seconds/1e9,timing_scope=scope,**extra)


def transfer_samples(events, jobs, bounds):
    samples, native, stages, outcomes, issues = [], [], [], [], []
    def add(*args,**kwargs):
        item = sample(*args,**kwargs)
        if item is not None: samples.append(item)
    for row in events:
        if not inside(row.get('end_wall_ns',row.get('wall_ns')),bounds): continue
        event = row.get('event')
        if event == 'native_transfer_terminal':
            native.append(row)
            if row.get('source_agent') != row.get('target_agent'):
                issues.append('Generic native record has distinct agents; not classified as a local copy'); continue
            direction = (row.get('source_mem_type'),row.get('destination_mem_type'))
            operation = {('VRAM','DRAM'):'PUT',('DRAM','VRAM'):'GET'}.get(direction)
            if operation is None: issues.append('Local native memory direction is not GPU/Guard'); continue
            if row.get('status') != 'success': continue
            size = row.get('bytes')
            if (row.get('native_handle_released') is not True or row.get('bytes_source') != 'native_telemetry'
                    or not legacy.finite(size) or size <= 0 or size % PAGE or row.get('descriptor_bytes') != size):
                issues.append('Local native record lacks released actual SDK page bytes'); continue
            for clock,seconds in [('native SDK',row.get('native_seconds')),('host submit-to-release',row.get('host_submit_to_release_seconds'))]:
                add(row,f'Local {operation} {clock}',size,size//PAGE,seconds,
                    'Released same-agent native handle; SDK transfer duration' if clock=='native SDK' else 'Host submission start through successful SDK handle release',
                    operation=operation,path='local',bytes_source='native_telemetry')
                if not legacy.finite(seconds) or seconds <= 0: issues.append(f'Local {operation}: missing positive {clock} clock')
        elif event == 'source_nixl_terminal':
            outcomes.append(dict(row,outcome_group=opt.outcome_group(row)))
            stamps = row.get('timing_ns',{})
            for name,start,end in opt.STAGES:
                stages.append(dict(legacy.reference(row),stage=name,wall_seconds=opt.duration(stamps,start,end),cpu_seconds=None))
            stages.append(dict(legacy.reference(row),stage='handling_cpu',wall_seconds=None,
                cpu_seconds=row['source_handling_cpu_ns']/1e9 if legacy.finite(row.get('source_handling_cpu_ns')) else None))
            if row.get('result') != 'success': continue
            if not row.get('source_agent') or not row.get('target_agent') or row['source_agent']==row['target_agent']:
                issues.append('Remote source terminal lacks distinct source/target attribution'); continue
            size,blocks,indices=row.get('bytes'),row.get('blocks'),row.get('completed_indices',[])
            requested=row.get('requested_blocks')
            if legacy.finite(size) and size > 0 and (type(blocks) is not int or blocks <= 0
                    or size != blocks*PAGE or len(indices) != blocks or len(set(indices)) != blocks
                    or not isinstance(requested,int) or any(type(i) is not int or not 0 <= i < requested for i in indices)):
                issues.append('Remote source bytes/count/indices do not describe the actual completed subset');continue
            start = row.get('start_monotonic_seconds')
            core = opt.duration(dict(start=round(start*1e9) if legacy.finite(start) else None,
                                     end=stamps.get('core_source_write_ns')),'start','end')
            seconds = (row.get('native_seconds'),row.get('source_operation_seconds'),core,
                       opt.duration(stamps,'source_receive_ns','cleanup_done_ns'))
            for clock,elapsed in zip(opt.CLOCKS,seconds):
                add(row,'Remote '+clock,row.get('bytes'),row.get('blocks'),elapsed,
                    'Actual selected READY-subset bytes; '+clock,operation='GET',path='remote',bytes_source=row.get('bytes_source'))
    for row in jobs:
        if row['kind'] != 'load': continue  # Resident-success PUT is not a DMA observation.
        for clock,seconds in [('job end-to-end',row.get('seconds')),('after-first-API-return-to-cleanup',row.get('first_api_to_cleanup_seconds'))]:
            clock_scope = ('Whole GPU job clock' if clock == 'job end-to-end' else
                           'Interval after the first public API returns through cleanup; excludes the first synchronous call')
            add(row,'Direct GPU GET '+clock,row['successful_key_bytes'],row['successful_blocks'],seconds,
                'Successful GET API payload / '+clock_scope+'; not wire bandwidth',operation='GET',path=row['path'])
            add(row,f'Direct GPU GET {row["path"]} '+clock,row['successful_key_bytes'],row['successful_blocks'],seconds,
                'Path-classified effective GET rate / '+clock_scope+'; not native wire bandwidth',operation='GET',path=row['path'])
            if row['path']=='mixed':
                for path in ('local','remote'):
                    add(row,f'Direct GPU GET mixed {path} portion '+clock,row[path+'_successful_bytes'],
                        row[path+'_successful_blocks'],seconds,'Portion payload / '+clock_scope+'; includes other mixed-job work',
                        operation='GET',path='mixed-'+path)
    return samples, native, stages, outcomes, issues


def load_capture(root, used, issues):
    """Whole-file protocol validation, before selecting a measured population."""
    events, records, validations = [], [], []
    directories = [Path(p) for p in capture_inventory(root)['selected_directories']]
    used.add(HERE/'capture_scope.py')
    for directory in directories:
        receipt = None
        for parent in metadata_parents(directory):
            path = parent/'capture-validity.json'
            if path.is_file():
                used.add(path); receipt=json.loads(path.read_text()); break
        try:
            if not receipt or receipt.get('valid') is not True: raise ValueError('Missing successful closed measured capture receipt')
            expected = receipt.get('expected_identities',receipt.get('identities',()))
            pids = set(receipt.get('expected_pids',()))
            if not expected or not pids: raise ValueError('Closed receipt lacks expected identities/PIDs')
            validation = fmt.validate_capture(directory,expected)
            captured = {int(r['pid']) for r in validation['files']}
            if pids-captured: raise ValueError('Missing expected PID captures: '+str(sorted(pids-captured)))
            validation.update(expected_pids=sorted(pids),expected_identities=expected)
            validations.append(validation)
        except (fmt.CaptureInvalid,ValueError,KeyError) as error:
            validations.append(dict(valid=False,directory=str(directory),error=str(error)))
            issues.append('Capture protocol: '+str(error))
        for path in sorted(directory.glob('source-*.jsonl')):
            used.add(path)
            try:
                with path.open() as stream:
                    events.extend(dict(json.loads(line),source_file=str(path),source_line=i) for i,line in enumerate(stream,1))
            except (OSError,ValueError) as error: issues.append(f'{path}: {error}')
        for path in sorted(directory.glob('keys-*.bin')):
            used.add(path)
            try: records.extend(fmt.iter_key_journal(path))
            except fmt.CaptureInvalid as error: issues.append(str(error))
    if not directories: issues.append('No measured closed captures; canary/smoke cannot supply profiling populations')
    return events,records,validations,bool(validations) and all(v['valid'] for v in validations)


def rank_proof(root,used):
    rows=[]
    for path in measured_rank_paths(root):
        used.add(path);rows.append(json.loads(path.read_text()))
    valid=(len(rows)==8 and {r.get('rank') for r in rows}==set(range(8))
        and len({r.get('pid') for r in rows})==8
        and all(r.get('primary_host_data_bytes')==0 and r.get('direct_gpu_guard') is True
                and r.get('framework_region_mem_types')==['VRAM']
                and r.get('gpu_region',{}).get('mem_type')=='VRAM'
                and r.get('gpu_region',{}).get('size')==PAGE
                and r.get('guard_pool_bytes')==124*GIB
                and r.get('source_pin_callback_calls')==0
                and r.get('guard_only_sources') is True and r.get('guard_only_source_proof') is True
                and r.get('opportunistic_query') is False for r in rows))
    return dict(verified=valid,observed=rows,expected_ranks=8,
                guard_data_gib=992,framework_host_data_bytes=0 if valid else None,
                scope='Exactly eight measured runtime rank proofs, typed VRAM, canonical page size and zero framework CPU payload')


def lineage_tables(records,bounds,issues):
    if bounds is None: return [],[],dict(numerator=None,denominator=None,rate=None)
    try: lineage=correlate(records,bounds)
    except ValueError as error: issues.append('Lineage: '+str(error));lineage=[]
    gets=[r for r in records if r.get('event')=='source_get' and inside(r.get('wall_ns'),bounds)]
    requested=sum(len(r.get('keys',())) for r in gets)
    ready=sum(len(r.get('ready_indices',())) for r in gets)
    ready_times={}
    for r in sorted((r for r in records if r.get('event')=='ready'),key=lambda r:r.get('export_sequence',0)):
        ready_times[(r.get('agent'),r.get('incarnation'),r.get('key'),r.get('generation'))]=r.get('transition_ns')
    withdrawals=[]
    for r in records:
        if r.get('event')=='withdraw' and inside(r.get('wall_ns'),bounds):
            start=ready_times.get((r.get('agent'),r.get('incarnation'),r.get('key'),r.get('generation')))
            withdrawals.append(dict(serial(r),ready_to_withdraw_seconds=opt.duration(
                dict(start=start,end=r.get('transition_ns')),'start','end')))
    get_times={(r.get('agent'),r.get('incarnation'),r.get('operation_key'),r.get('wall_ns')):r.get('monotonic_ns') for r in gets}
    for r in lineage:
        r['withdrawal_age_seconds']=opt.duration(dict(start=r.get('prior_transition_ns'),
            end=get_times.get((r.get('agent'),r.get('incarnation'),r.get('operation_key'),r.get('wall_ns')))),
            'start','end') if r['classification'].startswith('withdrawn:') else None
    return lineage,withdrawals,dict(numerator=ready if gets else None,denominator=requested if gets else None,
        rate=ready/requested if requested else None,scope='Requested source keys with READY export claims, not GPU token cache hits or successful GETs')


def hazard_tables(events,bounds):
    active=set();selected=[]
    for r in sorted(events,key=lambda r:r.get('wall_ns',0)):
        if r.get('event')!='direct_gpu_hazard' or bounds is None or r.get('wall_ns',math.inf)>bounds[1]: continue
        key=(r.get('pid'),r.get('session'),r.get('agent'),r.get('op_handle'))
        if r.get('state')=='uncertain':active.add(key)
        elif r.get('state')=='quiesced':active.discard(key)
        if inside(r.get('wall_ns'),bounds):selected.append(r)
    return selected,dict(uncertain_events=sum(r.get('state')=='uncertain' for r in selected),
        quiesced_events=sum(r.get('state')=='quiesced' for r in selected),
        unresolved_at_phase_end=len(active) if bounds else None,
        scope='All pre/end-boundary hazard transitions establish end-of-phase holds; later cleanup is not counted inside profiling')


def configuration(root,used):
    # Prefer immutable per-run manifests over a mutable top-level config.
    for path in (root/'manifest.json',root/'raw/manifest.json'):
        if path.is_file():
            used.add(path);data=json.loads(path.read_text())
            if isinstance(data.get('experiment'),dict):return data['experiment'],str(path)
    paths=sorted(root.glob('**/bundle/config/experiment.json'))
    paths+=sorted(root.glob('**/bundle/config/agentx-experiment.json'))
    if (root/'config/experiment.json').exists(): paths.insert(0,root/'config/experiment.json')
    if not paths:return {},None
    path=paths[0];used.add(path)
    return json.loads(path.read_text()),str(path)


def mooncake_baseline(used):
    # Local evidence only; prefer the recovered v2 result of the requested single-RNIC r3.
    roots=[p/name for p in HERE.parents for name in ('mooncake-agentx','local/mooncake-agentx')]
    home=next((p for p in roots if p.is_dir()),None)
    if home is None:return dict(available=False,reason='Local Mooncake AgentX r3 evidence unavailable'),[],{}
    candidates=sorted(home.glob('runs/*1g124g-r3/analysis-v*/report.json'))
    if not candidates: candidates=sorted(home.glob('runs/*1g124g-r3/analysis/report.json'))
    if not candidates:return dict(available=False,reason='No recovered r3 report'),[],{}
    path=candidates[-1];used.add(path);data=json.loads(path.read_text())
    tables={}
    for name in ('transfer-samples','transfer-summary','latency-cdf','cache-rates','server-cache-rates','throughput-summary'):
        file=path.parent/(name+'.csv')
        if file.exists():used.add(file);tables[name]=read_csv(file)
    for file in (home/'report.md',path.parent/'report.md',path.parent/'input-hashes.json',path.parent.parent/'raw-integrity.json'):
        if file.exists():used.add(file)
    config,config_path=configuration(path.parent.parent/'raw',used)
    samples=[]
    for row in tables.get('transfer-samples',[]):
        def number(field):
            try:return float(row[field])
            except (KeyError,TypeError,ValueError):return None
        item=sample(row,row.get('dataset','Mooncake API GET'),number('bytes'),number('blocks_or_objects'),number('seconds'),
                    'Remote-only successful bytes / whole Store GET API duration; mixed calls include self work',operation='GET',path='remote-API')
        if item:samples.append(item)
    return dict(available=True,run=path.parent.parent.name,report=str(path),analysis_directory=str(path.parent),
        raw_directory=str(path.parent.parent/'raw'),configuration_source=config_path,status=data.get('status'),limitations=data.get('limitations'),
        latency_summary=data.get('latency_summary'),throughput=data.get('throughput'),
        capacity=dict(store_gib=992,client_buffer_gib=8,framework_host_cache='N/A'),
        scope='Most recent requested single-RNIC Mooncake AgentX r3; recovered analysis-v2 preferred; no raw workload rerun'),samples,config


def compare_configuration(direct,moon):
    fields=('concurrency','random_seed','dataset_repository','dataset_revision','dataset_loader','dataset_entries',
            'scenario','model','model_path','tokenizer_revision','aiperf_commit','ranks','duration_seconds','grace_seconds','single_rnic',
            'trajectory_start_min_ratio','trajectory_start_max_ratio','trace_idle_gap_cap_seconds',
            'warmup_requests_per_lane','router_session_affinity_ttl_seconds','session_timeout_seconds')
    return [dict(field=k,direct=direct.get(k),mooncake=moon.get(k),
                 equal=direct.get(k)==moon.get(k) if k in direct and k in moon else None) for k in fields]


def write_tables(output,tables):
    for name,rows in tables.items():legacy.write_csv(output/(name+'.csv'),serial(rows))


def plot(output,tables,fixture_validation=False):
    os.environ.setdefault('MPLCONFIGDIR',str(output/'.mpl-cache'))
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    def save(fig,name):
        fig.suptitle(('SYNTHETIC DIRECT FIXTURE — NOT BENCHMARK\n' if fixture_validation else '')+
                     'SDK / source / GPU-job clocks differ; effective GB/s is not pure wire bandwidth',fontsize=10)
        fig.savefig(output/(name+'.png'),dpi=140);fig.savefig(output/(name+'.pdf'))
        plt.close(fig)
    datasets=sorted({r['dataset'] for r in tables['transfer-cdf']})
    for prefix,selected in [('all',datasets),('local-native',[d for d in datasets if d.startswith('Local ')]),
                            ('remote-source',[d for d in datasets if d.startswith('Remote ')]),
                            ('gpu-jobs',[d for d in datasets if d.startswith('Direct GPU')]),
                            ('comparison',[d for d in datasets if d.startswith('Mooncake ') or d=='Direct GPU GET remote after-first-API-return-to-cleanup'])]:
        fig,axes=plt.subplots(1,4,figsize=(17,4),layout='constrained')
        for ax,metric in zip(axes,METRICS):
            for dataset in selected:
                rows=[r for r in tables['transfer-cdf'] if r['dataset']==dataset and r['metric']==metric]
                if rows:ax.step([r['value'] for r in rows],[r['cdf'] for r in rows],where='post',label=dataset)
            ax.set(xlabel=metric,ylabel='CDF',ylim=(0,1))
            if ax.lines:ax.legend(fontsize=5)
            else:ax.text(.5,.5,'Unavailable',transform=ax.transAxes,ha='center')
        save(fig,prefix+'-cdf')
    labels=('1','2-4','5-16','17-64','65-256','257-1024','>1024')
    for label in labels:
        fig,axes=plt.subplots(1,4,figsize=(17,4),layout='constrained')
        for ax,metric in zip(axes,METRICS):
            for dataset in datasets:
                rows=[r for r in tables['size-bin-cdf'] if r['dataset']==dataset and r['metric']==metric and r['page_bin']==label]
                if rows:ax.step([r['value'] for r in rows],[r['cdf'] for r in rows],where='post',label=dataset)
            ax.set(title=f'{label} pages ({PAGE:,} B/page)',xlabel=metric,ylabel='CDF',ylim=(0,1))
            if ax.lines:ax.legend(fontsize=5)
        name='pages-'+label.replace('>','over-')+'-cdf'
        save(fig,name)


    fig,axes=plt.subplots(1,4,figsize=(17,4),layout='constrained')
    for ax,quantile in zip(axes,('mean','p50','p90','p99')):
        for dataset in datasets:
            rows={r['page_bin']:r for r in tables['size-bin-summary'] if r['dataset']==dataset and r['metric']=='GBps'}
            values=[rows.get(label,{}).get(quantile) for label in labels]
            if any(v is not None for v in values):ax.plot(labels,values,'o-',label=dataset)
        ax.set(title=quantile,xlabel=f'Actual payload pages ({PAGE:,} bytes)',ylabel='Effective GB/s')
        ax.tick_params(axis='x',rotation=45)
        if ax.lines:ax.legend(fontsize=5)
    save(fig,'size-bin-bandwidth-quantiles')
    fig,axes=plt.subplots(1,2,figsize=(12,4),layout='constrained')
    for ax,metric in zip(axes,('seconds','first_api_to_cleanup_seconds')):
        for dataset in sorted({r['dataset'] for r in tables['job-cdf']}):
            rows=[r for r in tables['job-cdf'] if r['dataset']==dataset and r['metric']==metric]
            if rows:ax.step([r['value'] for r in rows],[r['cdf'] for r in rows],where='post',label=dataset)
        ax.set(xlabel='after-first-API-return-to-cleanup seconds' if metric == 'first_api_to_cleanup_seconds' else metric,ylabel='CDF',ylim=(0,1))
        if ax.lines:ax.legend(fontsize=6)
    save(fig,'gpu-job-clocks-cdf')


def analyze(root,base,output,plots=True):
    root,base,output=(Path(p).resolve() for p in (root,base,output))
    if any(output==p or p in output.parents or output in p.parents for p in (root,base)):
        raise ValueError('Output must be disjoint from raw and base trees')
    issues=[];used=set()
    report_path=base/'report.json';used.add(report_path)
    baseline=json.loads(report_path.read_text());bounds=baseline.get('phase_bounds_ns')
    if not (isinstance(bounds,list) and len(bounds)==2 and all(legacy.finite(v) for v in bounds) and bounds[0]<bounds[1]):
        bounds=None;issues.append('No measured phase bounds; native canary is not a model benchmark')
    events,records,validations,valid=load_capture(root,used,issues)
    ranks=rank_proof(root,used)
    if not ranks['verified']:issues.append('Missing/invalid exact eight-rank zero-host/VRAM/Guard124 proof')
    jobs,keys,chain_issues=join_jobs(events,records,bounds,valid and ranks['verified']);issues.extend(chain_issues)
    samples,native,stages,outcomes,sample_issues=transfer_samples(events,jobs,bounds);issues.extend(sample_issues)
    lineage,withdrawals,yield_summary=lineage_tables(records,bounds,issues)
    hazards,hazard_summary=hazard_tables(events,bounds)
    if hazard_summary['unresolved_at_phase_end']:issues.append('GPU physical hazards remain held at profiling end')
    measured=[r for r in events if inside(r.get('end_wall_ns',r.get('wall_ns')),bounds)]
    queries=Counter();aggregates=[]
    for row in events:
        if row.get('event') not in ('progress_schedule','query_outcomes'):continue
        if inside(row.get('interval_start_ns'),bounds) and inside(row.get('interval_end_ns'),bounds):
            aggregates.append(row)
            if row['event']=='query_outcomes':queries.update(row.get('query_status_tier_counts',{}))
    histogram_rows,histogram_summary=opt.histogram_tables(aggregates)
    query_total=sum(queries.values())
    confirmed_queries=(ranks['verified'] and all(r.get('remote_query_mode')=='source_confirmed_ready'
        for r in ranks['observed']))
    query_scope=('Source-confirmed Guard READY observation, not a lease or successful transfer; pending is not a hit'
        if confirmed_queries else 'KVCR query attempt; remote hint is not READY residency')
    tiers=[]
    for name,count in sorted(queries.items()):
        tier='Guard local' if 'LOCAL_G2' in name or ' / DRAM' in name else 'remote hint (advisory)' if 'REMOTE_G2' in name else 'other/unknown'
        if confirmed_queries and 'REMOTE_G2' in name:
            tier='remote Guard probe pending' if name.startswith('FETCHING') else 'remote Guard READY observation'
        tiers.append(dict(metric=name,tier=tier,numerator=count,denominator=query_total,
                          rate=count/query_total if query_total else None,scope=query_scope))
    server_rates_path=base/'server-cache-rates.csv'
    if server_rates_path.exists():
        used.add(server_rates_path)
        for row in read_csv(server_rates_path):
            name=row.get('cache','')
            if 'host' in name.lower():continue
            tiers.append(dict(metric=name,tier='GPU' if 'GPU' in name else 'external lookup (may include remote hints)',
                numerator=row.get('hits'),denominator=row.get('queries'),rate=row.get('hit_rate'),
                unit=row.get('unit'),scope=row.get('definition'),source=str(server_rates_path)))
    tiers.append(dict(metric='Framework host cache',tier='N/A',numerator=None,denominator=None,rate=None,
                      scope='Zero framework host data allocation; no host-cache hit denominator'))
    config,config_source=configuration(root,used)
    moon,moon_samples,moon_config=mooncake_baseline(used)
    if not moon['available']:issues.append(moon['reason'])
    if not config:issues.append('Collected direct experiment configuration missing; equal-trace comparison is unverified')
    comparisons=compare_configuration(config,moon_config)
    samples+=moon_samples if bounds is not None else []
    canaries=separate_canary_evidence(root,used)
    tables={'separate-canary-evidence':canaries,'transfer-samples':samples,'local-native-transfers':native,'direct-gpu-jobs':jobs,
        'direct-gpu-keys':keys,'direct-gpu-journal':[serial(r) for r in records if r.get('event') in ('direct_gpu_submit','direct_gpu_api','direct_gpu_result') and inside(r.get('wall_ns'),bounds)],'per-request':[], 'source-outcomes':outcomes,
        'source-stages':stages,'key-lineage':lineage,'withdrawals':withdrawals,
        'source-get-keys':[serial(r) for r in records if r.get('event')=='source_get' and inside(r.get('wall_ns'),bounds)],
        'gpu-hazards':hazards,'tier-cache-rates':tiers,'progress-scheduling':[r for r in aggregates if r['event']=='progress_schedule'],
        'progress-histograms':histogram_rows,'progress-histogram-summary':histogram_summary,
        'configuration-comparison':comparisons,'rank-proofs':ranks['observed']}
    groups=defaultdict(list)
    for r in jobs:groups[(r.get('pid'),r.get('agent'),r.get('request_id'),r['kind'])].append(r)
    for (pid,agent,request,kind),rows in groups.items():
        tables['per-request'].append(dict(pid=pid,agent=agent,request_id=request,kind=kind,jobs=len(rows),
            successful_blocks=sum(r['successful_blocks'] for r in rows),failed_blocks=sum(r['failed_blocks'] for r in rows),
            successful_key_bytes=sum(r['successful_key_bytes'] for r in rows),
            scope='Observed jobs per request/kind; job durations overlap and are not summed into request latency'))
    tables.update(distributions(samples))
    job_samples=[dict(r,dataset='Direct GPU '+('GET' if r['kind']=='load' else 'PUT API (not DMA)')+' '+r['path'],
                      bytes=r['successful_key_bytes']) for r in jobs]
    job_tables=distributions(job_samples,('seconds','first_api_to_cleanup_seconds','successful_key_bytes',
                                       'requested_blocks','successful_blocks','failed_blocks'))
    for old,new in [('transfer-summary','job-summary'),('transfer-cdf','job-cdf'),
                    ('size-bin-summary','job-size-bin-summary'),('size-bin-cdf','job-size-bin-cdf')]:
        tables[new]=job_tables[old]
    request_ready=defaultdict(lambda:[0,0])
    for r in records:
        if r.get('event')=='source_get' and inside(r.get('wall_ns'),bounds):
            k=(r.get('agent'),r.get('incarnation'),r.get('request_id'),r.get('request_correlation'))
            request_ready[k][0]+=len(r.get('ready_indices',()))
            request_ready[k][1]+=len(r.get('keys',()))
    tables['request-ready-yield']=[dict(agent=k[0],incarnation=k[1],request_id=k[2],request_correlation=k[3],
        numerator=v[0],denominator=v[1],rate=v[0]/v[1] if v[1] else None) for k,v in request_ready.items()]
    tables['latency-comparison']=[dict(dataset='Direct Guard',metric=k,**v)
        for k,v in (baseline.get('latency_summary') or {}).items()]
    tables['latency-comparison'] += [dict(dataset='Mooncake r3',metric=k,**v)
        for k,v in (moon.get('latency_summary') or {}).items()]
    throughput_path=base/'throughput-summary.csv'
    tables['throughput-comparison']=[dict(r,dataset='Direct Guard') for r in read_csv(throughput_path)]
    if throughput_path.exists():used.add(throughput_path)
    if moon.get('throughput'):tables['throughput-comparison'].append(dict(moon['throughput'],dataset='Mooncake r3'))
    stage_groups=defaultdict(list)
    for r in stages:
        for metric in ('wall_seconds','cpu_seconds'):stage_groups[(r['stage'],metric)].append(r.get(metric))
    tables['source-stage-summary']=[dict(stage=stage,metric=metric,**legacy.stats(values)) for (stage,metric),values in stage_groups.items()]
    source_proof=dict(verified=valid and bool(outcomes) and all(r.get('guard_only_sources') is True and r.get('has_framework_pins') is False
        and r.get('initial_caller_handoff')=='absent_by_guard_only_progress_path' for r in outcomes),operations=len(outcomes))
    violations=[r for r in measured if r.get('event') in ('forbidden_source_pin','capture_error') or r.get('framework_host_data_bytes',0)!=0]
    if violations:issues.append('Measured no-host/source-pin invariant violated')
    elif not source_proof['verified']:issues.append('Measured source/capture evidence unavailable or invalid; canaries cannot provide benchmark invariant proof')
    if not jobs:issues.append('No fully joined measured direct GPU jobs')
    base_links={}
    for path in sorted(base.iterdir()):
        if path.is_file():used.add(path);base_links[path.name]=os.path.relpath(path,output)
    output.mkdir(parents=True,exist_ok=True)
    # Keep all existing client/worker/cache plots and tables as immutable linked evidence.
    limitations=[*opt.REPORT_SCOPE_WARNINGS,
        'Direct Guard has 992 GiB requested cache data, zero framework CPU payload buffers; GPU KV allocation is separate. Host-cache rates are N/A.',
        'Mooncake r3 has 992 GiB Store data plus 8 GiB client staging/transfer buffers, not a framework host cache. Capacity budgets differ by those buffers.',
        'SDK native duration is an SDK clock, not pure wire bandwidth. Host submit-to-release includes polling and cleanup. Full source clocks retain old-opt definitions and may overlap.',
        'Direct GET job end-to-end is IOJob.result transfer_time (construction through result creation), including CUDA readiness and actor/API waits. first_api_ns is recorded after the first API returns; after-first-API-return-to-cleanup excludes readiness and that first synchronous call. The historical CSV field first_api_to_cleanup_seconds retains this meaning. Remote-subset timestamps do not establish a whole mixed-job API start. These are effective payload rates.',
        'PUT success includes already resident keys. PUT API successful_key_bytes is never substituted for native bytes; only actual released native SDK samples establish PUT DMA.',
        'Mixed GET portion rates use the whole mixed-job denominator; Mooncake mixed remote samples likewise include local/self work in the whole Store API call. Neither is an isolated NIC rate.',
        'Native/terminal metric populations use completion wall timestamps inside measured bounds. Aggregate intervals must fit wholly; direct job submissions must also be inside. Canary/smoke are excluded.',
        'Exact key lineage uses export-sequence history, including prior READY/withdraw mutations outside profiling. Histories are unavailable as proof if capture validation fails.',
        'Full qualified raw key bytes and result indices remain authoritative. HMA recovery callbacks or suppressed flat invalid IDs are not converted into successful per-key reads.',
        'Local path inference requires closed complete direct-mode/no-G3 capture and a valid runtime rank contract; unmatched remote attempts remain unknown.',
        'GPU token cache, Guard local query attempts, remote query observations and source READY key yield have different denominators. No rates are added. '+query_scope,
        'Base TTFT/TPOT/throughput and worker/cache/GPU/RNIC time series are preserved by links. No missing metrics, clocks or cache denominators are filled with zeros.',
        'Offline full-key correlation retains journals in memory; full benchmark analysis can require substantial RAM.',
        'This supplement does not certify end-to-end benchmark completion; hardware primitive success does not establish model or AgentX performance.'
    ]
    report=dict(fixture_validation_only=baseline.get('fixture_validation_only',False),status='incomplete' if issues else 'evidence_available',scope='Measured direct GPU/Guard supplement; not benchmark completion certification',
        query_semantics=query_scope,page_bytes=PAGE,phase_bounds_ns=bounds,separate_canary_evidence=canaries,capture_scope=capture_inventory(root),capture_valid=valid if validations else None,capture_validation=validations,rank_proof=ranks,
        guard_only_source_proof=source_proof,ready_yield=yield_summary,lineage_counts=dict(Counter(r['classification'] for r in lineage)),
        hazard_summary=hazard_summary,job_counts=dict(Counter(r['kind']+':'+r['path'] for r in jobs)),
        error_counts=dict(source_outcomes=dict(Counter(r['outcome_group'] for r in outcomes)),failed_job_keys=sum(r['failed_blocks'] for r in jobs),invariant_violations=len(violations)),
        host_cache=dict(framework_data_bytes=0 if ranks['verified'] else None,rate=None,denominator=None,status='N/A'),
        direct_capacity=dict(guard_data_gib=992,framework_host_buffer_gib=0,observed_verified=ranks['verified']),
        base_status=baseline.get('status'),base_outputs=base_links,base_latency_summary=baseline.get('latency_summary'),
        mooncake=moon,configuration_source=config_source,configuration_comparison=comparisons,
        equal_trace_configuration=bool(config and moon_config) and all(r['equal'] is True for r in comparisons),
        counts={k:len(v) for k,v in tables.items()},limitations=limitations,issues=list(dict.fromkeys(issues)))
    report['evidence_status']=report['status']
    if report['fixture_validation_only']:report['status']='fixture_validation_only'
    write_tables(output,tables)
    legacy.write_json(output/'report.json',serial(report))
    def table(rows,columns):
        if not rows:return 'Unavailable.\n'
        return '| '+' | '.join(columns)+' |\n| '+' | '.join('---' for _ in columns)+' |\n'+ '\n'.join('| '+' | '.join(str(r.get(c)) for c in columns)+' |' for r in rows)+'\n'
    text='# Direct GPU ↔ Guard analysis\n\nStatus: '+report['status']+'\n\n'
    text+='Zero framework host cache; Guard 124 GiB/rank × 8 = 992 GiB requested data. GPU KV memory is separate.\n\n'
    for title,rows,columns in [('Transfer clock populations',tables['transfer-summary'],['dataset','metric','count','mean','p50','p90','p99']),
        ('Client latency comparison',tables['latency-comparison'],['dataset','metric','count','mean','p50','p90','p99']),
        ('Client throughput comparison',tables['throughput-comparison'],['dataset','completed','phase_seconds','output_tokens_per_second','sdk_requests_per_second']),
        ('Source READY yield',[yield_summary],['numerator','denominator','rate','scope']),
        ('GPU hazards',[hazard_summary],['uncertain_events','quiesced_events','unresolved_at_phase_end']),
        ('Tier denominators',tiers,['metric','tier','numerator','denominator','rate']),
        ('Configuration equality',comparisons,['field','direct','mooncake','equal'])]:
        text+='## '+title+'\n\n'+table(rows,columns)+'\n'
    text+='## Mooncake comparison\n\n```json\n'+json.dumps(serial(moon),indent=2)+'\n```\n\n'
    text+='## Preserved base evidence\n\n'+'\n'.join(f'- [{name}]({link})' for name,link in base_links.items())+'\n\n'
    text+='## Scope and limits\n\n'+'\n'.join('- '+s for s in limitations)+'\n\n'
    text+='## Evidence gaps\n\n'+('\n'.join('- '+s for s in report['issues']) or 'None observed.')+'\n'
    (output/'report.md').write_text(text)
    if plots:plot(output,tables,report['fixture_validation_only'])
    used.update([Path(__file__),HERE/'compact_format.py',HERE/'opt_lineage.py',HERE/'analyze-opt.py',HERE/'reference_agentx.py',HERE/'observations.py',Path(legacy.__file__)])
    hashes=[dict(path=str(p.resolve()),sha256=legacy.digest(p)) for p in sorted(used)]
    legacy.write_json(output/'input-hashes.json',hashes);legacy.write_csv(output/'input-hashes.csv',hashes)
    return report


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('root','base','output'):parser.add_argument('--'+name,type=Path,required=True)
    parser.add_argument('--no-plots',action='store_true')
    args=parser.parse_args();result=analyze(args.root,args.base,args.output,not args.no_plots)
    print(json.dumps(dict(status=result['status'],issues=result['issues'])))
