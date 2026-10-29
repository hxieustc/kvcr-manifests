#!/usr/bin/env python3
"""Analyze an extracted/shared paired PVC root, without contacting any pods.

python analyze-paired.py --root PAIR_ROOT [--output PAIR_ROOT/analysis]
Inputs: kvcr/phases/measured/results/guard and
mooncake/phases/measured/results/mooncake, each with replay-measured exports.
Requires ijson and matplotlib. Missing evidence is reported, never reconstructed.
Invalid capture protocols exit 1 after writing labeled diagnostic artifacts.
Optional hardware input: measured/**/hca-port-usage/prometheus-raw.json.
"""
import argparse
from collections import Counter, defaultdict
import csv
from functools import lru_cache
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import re

DATASETS = ('KVCR native', 'Mooncake pure remote', 'Mooncake mixed remote portion')
METRICS = ('bytes', 'blocks_or_objects', 'seconds', 'GBps')
BINS = ((1, 16), (17, 32), (33, 64), (65, 128), (129, 256),
        (257, 512), (513, 1024), (1025, math.inf))
BYTE_EDGES = (0, 2**20, 4 * 2**20, 16 * 2**20, 64 * 2**20,
              256 * 2**20, 1024 * 2**20, 4096 * 2**20, math.inf)
REQUEST_METRICS = ('time_to_first_token', 'inter_token_latency', 'request_latency',
                   'input_sequence_length', 'output_sequence_length')


@lru_cache(maxsize=1)
def telemetry():
    path = Path(__file__).resolve().parents[1] / 'mooncake/cache_telemetry.py'
    spec = importlib.util.spec_from_file_location('paired_cache_telemetry', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def finite(value):
    return isinstance(value, (float, int)) and not isinstance(value, bool) and math.isfinite(value)


def stats(values):
    values = sorted(v for v in values if finite(v))
    return {'count': len(values), 'mean': sum(values) / len(values) if values else None,
            **{f'p{q}': values[math.ceil(q * len(values) / 100) - 1] if values else None
               for q in (50, 90, 99)}}


def ecdf(values):
    counts = Counter(v for v in values if finite(v))
    total = sum(counts.values())
    cumulative = 0
    for value, count in sorted(counts.items()):
        cumulative += count
        yield value, cumulative / total


def write_csv(path, rows, fields=None):
    fields = fields or list(dict.fromkeys(key for row in rows for key in row)) or ['unavailable']
    with path.open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: json.dumps(v, sort_keys=True) if isinstance(v, (dict, list)) else v
                             for k, v in row.items()})


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + '\n')


def digest(path):
    sha = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b''):
            sha.update(block)
    return sha.hexdigest()


def reference(row):
    return {key: row.get(key) for key in ('source_file', 'source_line', 'pid', 'rank',
            'session', 'sequence', 'operation_key', 'source_agent', 'target_agent', 'end_wall_ns', 'wall_ns')}


def sample(row, dataset, size, blocks, seconds, scope):
    seconds = seconds if finite(seconds) and seconds >= 0 else None
    return {**reference(row), 'dataset': dataset, 'bytes': size,
            'blocks_or_objects': blocks, 'count_unit': 'KVCR blocks' if dataset == DATASETS[0] else 'Store objects',
            'seconds': seconds, 'timing_scope': scope,
            'GBps': size / seconds / 1e9 if finite(size) and size >= 0 and seconds is not None and seconds > 0 else None}


def kvcr_sample(row):
    if (row.get('result') != 'success' or row.get('status', 'success') not in ('success', 'partial')
            or not finite(row.get('bytes')) or row['bytes'] <= 0
            or not row.get('source_agent') or not row.get('target_agent')
            or row['source_agent'] == row['target_agent']):
        return None
    return {**sample(row, DATASETS[0], row.get('bytes'), row.get('blocks'),
                     row.get('native_seconds'), 'native NIXL xferDuration'),
            'source_operation_seconds': row.get('source_operation_seconds'),
            'post_seconds': row.get('post_seconds'), 'bytes_source': row.get('bytes_source'),
            'completion_status': row.get('status', 'success'), 'requested_blocks': row.get('requested_blocks')}


def request_tpot(row):
    latency, ttft, tokens = (row.get(k) for k in ('request_latency', 'time_to_first_token', 'output_sequence_length'))
    if (all(finite(v) for v in (latency, ttft, tokens)) and tokens > 1 and latency >= ttft
            and row.get('request_latency_unit') is not None
            and row.get('request_latency_unit') == row.get('time_to_first_token_unit')):
        return (latency - ttft) / (tokens - 1)
    return None


def moon_sample(row, endpoints):
    if row.get('failed_objects') != 0:
        return None
    resolved = telemetry().resolve_get(row, endpoints)
    kind = resolved['get_class']
    if kind not in ('pure_remote', 'mixed'):
        return None
    if (sum(resolved[k + '_successful_bytes'] for k in ('local', 'remote', 'unknown'))
            != row.get('successful_bytes') or
            sum(resolved[k + '_successful_objects'] for k in ('local', 'remote', 'unknown'))
            != row.get('objects')):
        return None
    dataset = DATASETS[1] if kind == 'pure_remote' else DATASETS[2]
    return {**sample(row, dataset, resolved['remote_successful_bytes'],
                     resolved['remote_successful_objects'], row.get('seconds'),
                     'whole Store GET API (metadata + completion; includes self work in mixed batches)'),
            'self_bytes': resolved['local_successful_bytes'],
            'batch_successful_bytes': row.get('successful_bytes'),
            'batch_objects': row.get('objects'),
            'metadata_observation_seconds': row.get('metadata_observation_seconds')}


def read_jsonl(path, issues):
    with path.open() as stream:
        for number, line in enumerate(stream, 1):
            try:
                row = json.loads(line)
                if not isinstance(row, dict):
                    raise ValueError('Expected JSON object')
            except ValueError as error:
                issues.append(f'{path}:{number}: {error}')
                continue
            yield {**row, 'source_file': str(path), 'source_line': number}


def request_outcomes(text):
    """Retain literal phase counters; grace cancellations are not HTTP errors."""
    result = {}
    sending = re.findall(
        r'Phase profiling \(profiling\) sending complete \| sent=([\d,]+), completed=([\d,]+), '
        r'in_flight=([\d,]+) \| sessions: sent=([\d,]+), completed=([\d,]+)', text)
    complete = re.findall(
        r'Phase profiling \(profiling\) complete \| completed=([\d,]+), cancelled=([\d,]+), '
        r'errors=([\d,]+) \| sessions: completed=([\d,]+), cancelled=([\d,]+) '
        r'\| elapsed=([\d.]+)s \| grace_period_timeout=(True|False)', text)
    if sending:
        result.update(zip(('sent_credits', 'completed_at_sending_end', 'in_flight_at_sending_end',
                           'sent_sessions', 'completed_sessions_at_sending_end'),
                          (int(v.replace(',', '')) for v in sending[-1])))
    if complete:
        row = complete[-1]
        result.update(zip(('completed_credits', 'cancelled_credits', 'reported_errors',
                           'completed_sessions', 'cancelled_sessions'),
                          (int(v.replace(',', '')) for v in row[:5])))
        result.update(elapsed_seconds=float(row[5]), grace_period_timeout=row[6] == 'True')
    return result


def load_exports(replay, used, issues):
    profile_path = replay / 'profile_export_aiperf.json'
    profile = None
    if profile_path.exists():
        used.add(profile_path)
        profile = json.loads(profile_path.read_text())
    else:
        issues.append(f'Missing AIPerf profile: {profile_path}')
    server = replay / 'server_metrics_export.json'
    bounds = None
    if server.exists():
        import ijson
        used.add(server)
        with server.open('rb') as stream:
            summary = next(ijson.items(stream, 'summary', use_float=True), {})
        phase = summary.get('phase_time_ranges', {}).get('profiling', {})
        if phase.get('start_ns') is not None and phase.get('end_ns') is not None:
            bounds = int(phase['start_ns']), int(phase['end_ns'])
            if bounds[1] <= bounds[0]:
                issues.append(f'Invalid profiling window: {server}')
                bounds = None
    if bounds is None:
        issues.append(f'No profiling bounds; telemetry excluded: {replay}')
    return profile, bounds


def server_metrics(path, backend, used, issues):
    """Stream one exported metric at a time; keep endpoints separate throughout."""
    result = {'series': [], 'cache_rates': [], 'request_histograms': [], 'queue_gauges': []}
    cache_definitions = (
        ('GPU prefix', 'vllm:prefix_cache', None, 'tokens', 'GPU prefix lookup'),
        ('Framework primary host', 'vllm:kv_offload_tiering_chunk', '0:primary', 'chunks', 'Primary host tier'),
        ('Guard/KVCR secondary tier', 'vllm:kv_offload_tiering_chunk', '1:kvcr', 'chunks',
         'Includes local guard hits and remote-fetchable hints; not pure local residency'),
        ('External prefix', 'vllm:external_prefix_cache', None, 'tokens', 'All external connector sources'))
    counters = {prefix + suffix for _, prefix, _, _, _ in cache_definitions for suffix in ('_hits', '_queries')}
    histograms = {'vllm:request_queue_time_seconds', 'vllm:request_prefill_time_seconds'}
    gauges = {'vllm:num_requests_waiting', 'vllm:num_requests_running'}
    if path.exists():
        import ijson
        used.add(path)
        with path.open('rb') as stream:
            for name, metric in ijson.kvitems(stream, 'metrics', use_float=True):
                if name not in counters | histograms | gauges:
                    continue
                for series in metric.get('series', []):
                    row = {'backend': backend, 'metric': name, 'type': metric.get('type'),
                           'endpoint': series.get('endpoint_url'), 'labels': series.get('labels', {}),
                           'stats': series.get('stats', {})}
                    result['series'].append(row)
                    if name in gauges:
                        result['queue_gauges'].append(row)
    endpoints = sorted({r['endpoint'] for r in result['series'] if r['endpoint'] is not None}) or [None]
    for endpoint in endpoints:
        rows = [r for r in result['series'] if r['endpoint'] == endpoint]
        for label, prefix, tier, unit, definition in cache_definitions:
            totals, coverage = [], []
            for suffix in ('_hits', '_queries'):
                selected = [r for r in rows if r['metric'] == prefix + suffix and r['type'] == 'counter'
                            and (tier is None or r['labels'].get('tier') == tier)]
                valid = [r for r in selected if finite(r['stats'].get('total'))]
                totals.append(sum(r['stats']['total'] for r in valid) if valid and len(valid) == len(selected) else None)
                coverage.append({json.dumps(r['labels'], sort_keys=True) for r in valid})
            hits, queries = totals
            paired = bool(coverage[0]) and coverage[0] == coverage[1]
            valid_rate = paired and finite(hits) and finite(queries) and 0 <= hits <= queries and queries > 0
            result['cache_rates'].append({'backend': backend, 'endpoint': endpoint, 'cache': label,
                'hits': hits, 'queries': queries, 'unit': unit, 'hit_rate': hits / queries if valid_rate else None,
                'matching_series_coverage': paired, 'definition': definition,
                'scope': 'AIPerf exported counter totals; endpoints separate; reset correction not assumed'})
        for name in sorted(histograms):
            selected = [r for r in rows if r['metric'] == name and r['type'] == 'histogram']
            valid = [r for r in selected if finite(r['stats'].get('sum')) and finite(r['stats'].get('count'))
                     and r['stats']['count'] >= 0 and r['stats']['sum'] >= 0]
            complete = bool(valid) and len(valid) == len(selected)
            count = sum(r['stats']['count'] for r in valid) if complete else None
            total = sum(r['stats']['sum'] for r in valid) if complete else None
            result['request_histograms'].append({'backend': backend, 'endpoint': endpoint, 'metric': name,
                'count': count, 'sum_seconds': total,
                'mean_seconds': total / count if count is not None and count > 0 else None,
                'series': len(selected), 'scope': 'sum of exported histogram sums / sum of counts'})
    return result


def hca_metrics(phase, bounds, backend, used, issues):
    """Read retained node-exporter responses only; no network or extrapolation."""
    result = {'available': False, 'intervals': [], 'summary': [],
              'scope': 'Whole-node HCA ports including background traffic; not benchmark-only utilization'}
    paths = sorted(phase.rglob('hca-port-usage/prometheus-raw.json'))
    if not paths or bounds is None:
        return result
    # Distinct exporter instances/devices/ports stay distinct; duplicate raw
    # responses must not multiply the same port's observations.
    series_by_key, conflicts = {}, set()
    for path in paths:
        used.add(path)
        raw = json.loads(path.read_text())
        for series in raw.get('data', {}).get('result', []):
            labels = series.get('metric', {})
            key = json.dumps(labels, sort_keys=True)
            if key in series_by_key and series_by_key[key].get('values') != series.get('values'):
                conflicts.add(key)
            else:
                series_by_key[key] = series
    for key in conflicts:
        issues.append(f'Conflicting duplicate HCA series excluded: {key}')
        del series_by_key[key]
    capacities = defaultdict(list)
    def port_key(labels):
        return tuple(labels.get(k) for k in ('instance', 'device', 'port'))
    def points(series):
        return [(float(t), float(v)) for t, v in series.get('values', [])
                if math.isfinite(float(t)) and math.isfinite(float(v))]
    start, end = bounds[0] / 1e9, bounds[1] / 1e9
    for series in series_by_key.values():
        labels = series['metric']
        if labels.get('__name__') == 'node_infiniband_rate_bytes_per_second':
            capacities[port_key(labels)].extend(v for t, v in points(series) if start <= t <= end)
    names = {'node_infiniband_port_data_received_bytes_total': 'rx',
             'node_infiniband_port_data_transmitted_bytes_total': 'tx'}
    for series in series_by_key.values():
        labels = series['metric']
        name = labels.get('__name__')
        if name not in names:
            continue
        cap = capacities[port_key(labels)]
        capacity = cap[0] if cap and len(set(cap)) == 1 and cap[0] > 0 else None
        resets = 0
        intervals = []
        observed = points(series)
        for (t0, v0), (t1, v1) in zip(observed, observed[1:]):
            if t0 < start or t1 > end or t1 <= t0:
                continue
            if v1 < v0:
                resets += 1
                continue
            rate = (v1 - v0) / (t1 - t0)
            intervals.append({'backend': backend, 'instance': labels.get('instance'),
                'device': labels.get('device'), 'port': labels.get('port'), 'direction': names[name],
                'labels': labels, 'start_unix': t0, 'end_unix': t1, 'bytes': v1 - v0,
                'bytes_per_second': rate, 'link_capacity_bytes_per_second': capacity,
                'link_usage_percent': rate / capacity * 100 if capacity else None})
        duration = sum(r['end_unix'] - r['start_unix'] for r in intervals)
        size = sum(r['bytes'] for r in intervals) if intervals else None
        result['intervals'].extend(intervals)
        result['summary'].append({'backend': backend, 'labels': labels, 'direction': names[name],
            'observed_bytes': size, 'observed_seconds': duration if intervals else None,
            'mean_bytes_per_second': size / duration if duration else None,
            'link_capacity_bytes_per_second': capacity,
            'mean_link_usage_percent': size / duration / capacity * 100 if duration and capacity else None,
            'counter_resets_excluded': resets, 'intervals': len(intervals)})
    result['available'] = bool(result['intervals'])
    return result


def endpoint_map(run, used, issues):
    path = run / 'worker.log'
    owners = defaultdict(set)
    if path.exists():
        used.add(path)
        with path.open(errors='replace') as stream:
            for line in stream:
                match = re.search(r'\b(\d+)\s+transfer_engine_impl\.cpp:\d+\].*?listening on (\S+)', line)
                if match:
                    owners[match[2]].add(int(match[1]))
    conflicts = [endpoint for endpoint, pids in owners.items() if len(pids) != 1]
    if conflicts:
        issues.append(f'Conflicting endpoint PID evidence excluded: {conflicts}')
    return {endpoint: next(iter(pids)) for endpoint, pids in owners.items() if len(pids) == 1}


def backend_data(root, backend, used, source_directory=None):
    mode = 'guard' if backend == 'kvcr' else 'mooncake'
    run = Path(source_directory) if source_directory is not None else root / backend / 'phases/measured/results' / mode
    replay = run / 'replay-measured'
    issues = []
    profile, bounds = load_exports(replay, used, issues)
    phase_log = root / backend / 'phases/measured/logs/replay.log'
    outcomes = request_outcomes(phase_log.read_text(errors='replace')) if phase_log.exists() else {}
    if phase_log.exists():
        used.add(phase_log)
    server = server_metrics(replay / 'server_metrics_export.json', backend, used, issues)
    hca = hca_metrics(root / backend / 'phases/measured', bounds, backend, used, issues)
    endpoints = endpoint_map(run, used, issues) if backend == 'mooncake' else {}
    helper = telemetry()
    samples, stages, failures, exists_events = [], [], [], []
    kinds, observed = Counter(), Counter()
    outside = missing_time = duplicate = 0
    seen = set()
    files = sorted((run / ('nixl-transfers' if backend == 'kvcr' else 'store-events')).glob('*.jsonl'))
    if not files:
        issues.append(f'No measured telemetry files: {run}')
    coverage = []
    for path in files:
        used.add(path)
        starts = ends = 0
        first_event = last_event = footer = None
        records = 0
        sequences = []
        file_issues = []
        for row in read_jsonl(path, file_issues):
            event = row.get('event')
            first_event = first_event or event
            last_event = event
            records += 1
            if event == 'capture_end':
                footer = row
            starts += event == 'capture_start'
            ends += event == 'capture_end'
            if event == 'capture_error' or (event == 'capture_end' and
                    (row.get('valid') is False or row.get('dropped') or row.get('unflushed')
                     or row.get('error') or row.get('instrumentation_errors') or row.get('lost')
                     or row.get('errors') or row.get('pending_bytes'))):
                file_issues.append(f'Capture reports loss/error at {path}:{row["source_line"]}')
            if row.get('sequence') is not None:
                sequences.append(row['sequence'])
            if all(row.get(k) is not None for k in ('session', 'pid', 'sequence')):
                key = (row['session'], row['pid'], row['sequence'])
                if key in seen:
                    duplicate += 1
                    continue
                seen.add(key)
            stamp = row.get('end_wall_ns', row.get('wall_ns'))
            if stamp is None:
                missing_time += 1
                continue
            if bounds is None or not bounds[0] <= stamp <= bounds[1]:
                outside += 1
                continue
            observed[event] += 1
            if event == 'source_nixl_terminal':
                status = row.get('status', row.get('result', 'unknown'))
                kinds[f'KVCR source {status}'] += 1
                item = kvcr_sample(row)
                if item is not None:
                    samples.append(item)
                if row.get('result') != 'success' or status not in ('success', 'partial'):
                    failures.append({**reference(row), 'backend': backend, 'event': event,
                                     'status': status, 'reason': row.get('reason'),
                                     'requested_blocks': row.get('requested_blocks'),
                                     'native': row.get('native'), 'reasons': row.get('reasons'),
                                     **{k: row.get(k) for k in ('native_seconds', 'post_seconds',
                                         'source_operation_seconds', 'lifecycle_seconds')}})
                for stage in ('native_seconds', 'post_seconds', 'source_operation_seconds', 'lifecycle_seconds'):
                    stages.append({**reference(row), 'backend': backend, 'origin': 'source_terminal_clock',
                                   'stage': stage, 'status': status, 'seconds': row.get(stage), 'calls': 1})
            elif event == 'store_operation':
                failed = row.get('failed_objects')
                status = 'failed' if row.get('exception_type') or (failed is not None and failed > 0) else 'success' if failed == 0 else 'unknown'
                operation = row.get('operation', 'unknown')
                kinds[f'Mooncake {operation} {status}'] += 1
                if operation == 'get':
                    resolved = helper.resolve_get(row, endpoints)
                    kinds[f'Mooncake GET class {resolved["get_class"]}'] += 1
                    item = moon_sample(row, endpoints) if failed is not None else None
                    if item is not None:
                        samples.append(item)
                if status != 'success':
                    failures.append({**reference(row), 'backend': backend, 'event': event,
                                     'operation': operation, 'status': status,
                                     'failed_objects': failed, 'missing_results': row.get('missing_results'),
                                     'seconds': row.get('seconds'),
                                     'metadata_observation_seconds': row.get('metadata_observation_seconds'),
                                     'exception_type': row.get('exception_type'),
                                     'exception_message': row.get('exception_message'),
                                     'error_return_codes': row.get('error_return_codes')})
                for stage, value in [('Store ' + operation + ' API', row.get('seconds')),
                                     ('replica observation outside API', row.get('metadata_observation_seconds'))]:
                    stages.append({**reference(row), 'backend': backend, 'origin': 'store_operation',
                                   'stage': stage, 'status': status, 'seconds': value, 'calls': 1})
            elif event in ('store_exists', 'external_prefix_accounting'):
                exists_events.append(row)
                if event == 'store_exists':
                    stages.append({**reference(row), 'backend': backend, 'origin': event,
                                   'stage': 'Store exists API', 'status': 'exception' if row.get('exception_type') else 'returned',
                                   'seconds': row.get('seconds'), 'calls': 1})
            if event in ('stage', 'duration'):
                stages.append({**reference(row), 'backend': backend, 'origin': event,
                               'stage': row.get('stage'), 'status': row.get('status'),
                               'seconds': row.get('duration_seconds'), 'calls': 1})
            # Terminal summaries and individual stage events overlap. Keep separate
            # origins; their totals and quantiles must never be combined.
            if event in ('source_nixl_terminal', 'target_terminal', 'operation_released'):
                for stage, value in row.get('stage_summary', {}).items():
                    stages.append({**reference(row), 'backend': backend, 'origin': event + '_summary',
                                   'stage': stage, 'status': row.get('status'),
                                   'seconds': value.get('active_seconds'), 'calls': value.get('count'),
                                   'unavailable_count': value.get('unavailable_count')})
        protocol = backend == 'kvcr' or starts > 0
        if protocol:
            if starts != 1 or ends != 1:
                file_issues.append(f'Expected capture_start/end once; observed {starts}/{ends}: {path}')
            if sequences != list(range(1, records + 1)):
                file_issues.append(f'Sequence gap/reorder in {path}')
            if first_event != 'capture_start' or last_event != 'capture_end' or not footer or footer.get('valid') is not True:
                file_issues.append(f'Capture must start with capture_start and finish with valid capture_end: {path}')
            if backend == 'mooncake' and footer and (footer.get('errors') != 0 or footer.get('lost') != 0
                    or footer.get('events_before_footer') != records - 1):
                file_issues.append(f'Mooncake footer loss/error/event-count mismatch: {path}')
            with path.open('rb') as stream:
                if path.stat().st_size:
                    stream.seek(-1, os.SEEK_END)
                    if stream.read(1) != b'\n':
                        file_issues.append(f'Capture stream lacks terminal newline: {path}')
        issues.extend(file_issues)
        coverage.append({'file': str(path), 'capture_starts': starts, 'capture_ends': ends,
                         'issues': file_issues, 'footer_protocol': protocol,
                         'valid': False if file_issues else True if protocol else None})
    exists = helper.summarize_cache(exists_events)
    exists['observed_exists_calls'] = observed['store_exists']
    requests = []
    request_path = replay / 'profile_export.jsonl'
    if request_path.exists():
        used.add(request_path)
        for row in read_jsonl(request_path, issues):
            metrics, meta = row.get('metrics', {}), row.get('metadata', {})
            # Failed/unfinished rows do not enter latency distributions.
            if row.get('error') or row.get('error_details') or not finite(metrics.get('time_to_first_token', {}).get('value')):
                continue
            request = {'backend': backend, **reference(row), 'request_id': meta.get('x_request_id'),
                       'conversation_id': meta.get('conversation_id'), 'turn_index': meta.get('turn_index')}
            for metric in REQUEST_METRICS:
                request[metric] = metrics.get(metric, {}).get('value')
                request[metric + '_unit'] = metrics.get(metric, {}).get('unit')
            requests.append(request)
    else:
        issues.append(f'Missing request-level export: {request_path}')
    return {'source_directory': str(run), 'profile': profile, 'request_outcomes': outcomes, 'phase_bounds_ns': bounds,
            'phase_seconds': (bounds[1] - bounds[0]) / 1e9 if bounds else None,
            'endpoint_process_map': endpoints, 'outside_phase_events': outside,
            'missing_timestamp_events': missing_time, 'duplicate_events_excluded': duplicate,
            'observed_events': dict(observed), 'operation_counts': dict(kinds), 'exists': exists,
            'coverage': coverage, 'issues': issues, 'server_metrics': server, 'hca': hca,
            'capture_valid': False if any(c['valid'] is False for c in coverage) else
                             True if coverage and all(c['valid'] is True for c in coverage) else None,
            'samples': samples, 'stages': stages, 'failures': failures, 'requests': requests}


def distributions(samples):
    summary, cdfs, by_bin, bin_cdfs, exact = [], [], [], [], []
    for dataset in DATASETS:
        rows = [r for r in samples if r['dataset'] == dataset]
        for metric in METRICS:
            summary.append({'dataset': dataset, 'metric': metric, **stats(r[metric] for r in rows)})
            cdfs.extend({'dataset': dataset, 'metric': metric, 'value': v, 'cdf': p}
                        for v, p in ecdf(r[metric] for r in rows))
        for lo, hi in BINS:
            label = f'{lo}-{int(hi)}' if math.isfinite(hi) else f'{lo}+'
            selected = [r for r in rows if finite(r['blocks_or_objects']) and lo <= r['blocks_or_objects'] <= hi]
            for metric in METRICS:
                key = {'dataset': dataset, 'blocks_or_objects_bin': label, 'metric': metric}
                by_bin.append({**key, **stats(r[metric] for r in selected)})
                bin_cdfs.extend({**key, 'value': v, 'cdf': p} for v, p in ecdf(r[metric] for r in selected))
        for size in sorted({r['bytes'] for r in rows if finite(r['bytes'])}):
            for metric in ('seconds', 'GBps'):
                exact.append({'dataset': dataset, 'bytes': size, 'metric': metric,
                              **stats(r[metric] for r in rows if r['bytes'] == size)})
    return summary, cdfs, by_bin, bin_cdfs, exact


def byte_distributions(samples):
    """Bandwidth conditional on actual remote payload bytes, independent of objects."""
    summary, cdfs = [], []
    for dataset in DATASETS:
        for lo, hi in zip(BYTE_EDGES, BYTE_EDGES[1:]):
            label = f'{lo // 2**20}-{int(hi) // 2**20} MiB' if math.isfinite(hi) else f'{lo // 2**20}+ MiB'
            key = {'dataset': dataset, 'byte_bin': label, 'min_bytes_inclusive': lo,
                   'max_bytes_exclusive': int(hi) if math.isfinite(hi) else None}
            selected = [r for r in samples if r['dataset'] == dataset and finite(r.get('bytes'))
                        and lo <= r['bytes'] < hi]
            summary.append({**key, 'metric': 'GBps', **stats(r['GBps'] for r in selected)})
            cdfs.extend({**key, 'GBps': v, 'cdf': p} for v, p in ecdf(r['GBps'] for r in selected))
    return summary, cdfs


def plots_for(samples, requests, by_bin, byte_bins, output):
    os.environ.setdefault('MPLCONFIGDIR', str(output / '.mpl-cache'))
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    def save(fig, path):
        fig.tight_layout()
        for suffix in ('png', 'pdf'):
            fig.savefig(path.with_suffix('.' + suffix), dpi=160)
        plt.close(fig)

    labels = {'bytes': 'Remote payload bytes', 'blocks_or_objects': 'KVCR blocks / remote Store objects',
              'seconds': 'Seconds (native NIXL vs whole GET API)', 'GBps': 'GB/s (decimal; timing scopes differ)'}
    def cdf_plot(ax, metric, rows, label):
        points = list(ecdf(r.get(metric) for r in rows))
        if points:
            ax.step([v for v, _ in points], [p for _, p in points], where='post', label=label)
        ax.set(xlabel=labels.get(metric, metric), ylabel='Empirical CDF', ylim=(0, 1.02))
        ax.grid(alpha=.25)

    cross = output / 'cross-worker'
    for metric in METRICS:
        fig, ax = plt.subplots(figsize=(9, 5))
        for dataset in DATASETS:
            rows = [r for r in samples if r['dataset'] == dataset]
            count = sum(finite(r.get(metric)) for r in rows)
            cdf_plot(ax, metric, rows, f'{dataset} (n={count})')
        if ax.lines:
            ax.legend(fontsize=8)
        else:
            ax.text(.5, .5, 'No measured samples available', transform=ax.transAxes, ha='center')
        save(fig, cross / (metric + '-cdf'))
    fig, axes = plt.subplots(2, 4, figsize=(17, 8))
    for ax, (lo, hi) in zip(axes.flat, BINS):
        for dataset in DATASETS:
            rows = [r for r in samples if r['dataset'] == dataset and finite(r['blocks_or_objects'])
                    and lo <= r['blocks_or_objects'] <= hi]
            cdf_plot(ax, 'GBps', rows, f'{dataset} (n={sum(finite(r["GBps"]) for r in rows)})')
        ax.set_title(f'{lo}-{int(hi)}' if math.isfinite(hi) else f'{lo}+')
        if ax.lines:
            ax.legend(fontsize=6)
    save(fig, cross / 'bandwidth-cdfs-by-size-bin')
    fig, axes = plt.subplots(1, 3, figsize=(15, 4.5))
    for ax, dataset in zip(axes, DATASETS):
        rows = [r for r in by_bin if r['dataset'] == dataset and r['metric'] == 'GBps']
        for key in ('mean', 'p50', 'p90', 'p99'):
            ax.plot(range(len(rows)), [r[key] if r[key] is not None else math.nan for r in rows], 'o-', label=key)
        ax.set_xticks(range(len(rows)), [r['blocks_or_objects_bin'] for r in rows], rotation=45)
        ax.set(title=dataset, ylabel='GB/s', xlabel='Blocks / remote Store objects')
        ax.legend(fontsize=7)
        ax.grid(alpha=.25)
    save(fig, cross / 'bandwidth-by-size-bin')
    fig, axes = plt.subplots(2, 4, figsize=(17, 8))
    for ax, (lo, hi) in zip(axes.flat, zip(BYTE_EDGES, BYTE_EDGES[1:])):
        for dataset in DATASETS:
            rows = [r for r in samples if r['dataset'] == dataset and finite(r.get('bytes'))
                    and lo <= r['bytes'] < hi]
            cdf_plot(ax, 'GBps', rows, f'{dataset} (n={sum(finite(r["GBps"]) for r in rows)})')
        ax.set_title(f'[{lo}, {int(hi)}) bytes' if math.isfinite(hi) else f'{lo}+ bytes')
        if ax.lines:
            ax.legend(fontsize=6)
    save(fig, cross / 'bandwidth-cdfs-by-byte-bin')
    fig, axes = plt.subplots(1, 3, figsize=(15, 4.5))
    for ax, dataset in zip(axes, DATASETS):
        rows = [r for r in byte_bins if r['dataset'] == dataset]
        for key in ('mean', 'p50', 'p90', 'p99'):
            ax.plot(range(len(rows)), [r[key] if r[key] is not None else math.nan for r in rows], 'o-', label=key)
        ax.set_xticks(range(len(rows)), [r['byte_bin'] for r in rows], rotation=45)
        ax.set(title=dataset, ylabel='GB/s', xlabel='Actual remote bytes (MiB = 1,048,576 bytes)')
        ax.legend(fontsize=7)
        ax.grid(alpha=.25)
    save(fig, cross / 'bandwidth-by-byte-bin')
    fig, axes = plt.subplots(1, 3, figsize=(15, 4.5))
    for ax, metric in zip(axes, REQUEST_METRICS[:3]):
        metric_units = {r.get(metric + '_unit') for r in requests if finite(r.get(metric))}
        if len(metric_units) > 1:
            ax.text(.5, .5, 'Incompatible units; see request CSV', transform=ax.transAxes, ha='center')
            ax.set_title(metric)
            continue
        for backend in ('kvcr', 'mooncake'):
            rows = [r for r in requests if r['backend'] == backend]
            units = {r.get(metric + '_unit') for r in rows if finite(r.get(metric))}
            if len(units) > 1:
                continue  # Incompatible units are retained in CSV, never merged.
            cdf_plot(ax, metric, rows, f'{backend} ({next(iter(units), "unit unavailable")})')
        if ax.lines:
            ax.legend(fontsize=8)
    save(fig, output / 'e2e-latency-cdfs')


def table(rows, columns):
    def cell(value):
        return 'N/A' if value is None else f'{value:.6g}' if isinstance(value, float) else str(value).replace('|', '\\|')
    return ['| ' + ' | '.join(columns) + ' |', '|' + '|'.join('---' for _ in columns) + '|'] + [
        '| ' + ' | '.join(cell(row.get(key)) for key in columns) + ' |' for row in rows]


def analyze(root, output, plots=True):
    root, output = root.resolve(), output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    cross = output / 'cross-worker'
    cross.mkdir(exist_ok=True)
    used = {Path(__file__).resolve(), Path(__file__).resolve().parents[1] / 'mooncake/cache_telemetry.py'}
    backends = {b: backend_data(root, b, used) for b in ('kvcr', 'mooncake')}
    invalid_capture = any(d['capture_valid'] is False for d in backends.values())
    capture_notice = ('INVALID CAPTURE: diagnostic observations only; protocol validation failed. '
                      'The CLI exits nonzero. See per-file issues in summary.json.' if invalid_capture else
                      'Capture protocol checks passed for participating protocol streams; legacy/missing streams remain unverified.')
    samples = [r for data in backends.values() for r in data.pop('samples')]
    stages = [r for data in backends.values() for r in data.pop('stages')]
    failures = [r for data in backends.values() for r in data.pop('failures')]
    requests = [r for data in backends.values() for r in data.pop('requests')]
    for row in requests:
        row['time_per_output_token'] = request_tpot(row)
        row['time_per_output_token_unit'] = row.get('request_latency_unit')
    tpot = [{'backend': backend, 'unit': unit + '/token',
             **stats(r['time_per_output_token'] for r in requests
                     if r['backend'] == backend and r['time_per_output_token_unit'] == unit)}
            for backend in ('kvcr', 'mooncake')
            for unit in sorted({r['time_per_output_token_unit'] for r in requests
                                if r['backend'] == backend and finite(r['time_per_output_token'])})]
    write_csv(output / 'tpot-summary.csv', tpot)
    summary, cdfs, bins, bin_cdfs, exact = distributions(samples)
    byte_bins, byte_cdfs = byte_distributions(samples)
    write_csv(cross / 'samples.csv', samples, list(dict.fromkeys(k for r in samples for k in r)) or
              ['dataset', 'bytes', 'blocks_or_objects', 'seconds', 'GBps'])
    for name, rows in [('summary', summary), ('cdf', cdfs), ('size-bin-summary', bins),
                       ('size-bin-cdf', bin_cdfs), ('exact-size-summary', exact),
                       ('byte-bin-summary', byte_bins), ('byte-bin-cdf', byte_cdfs)]:
        write_csv(cross / (name + '.csv'), rows)
    write_csv(output / 'stage-samples.csv', stages)
    stage_groups = defaultdict(list)
    for row in stages:
        stage_groups[(row['backend'], row['origin'], row['stage'], row['status'])].append(row)
    stage_summary = [{'backend': key[0], 'origin': key[1], 'stage': key[2], 'status': key[3],
                      'observations': len(rows), 'calls': sum(r.get('calls') or 0 for r in rows),
                      'timed_observations': sum(finite(r['seconds']) for r in rows),
                      'sum_seconds': sum(r['seconds'] for r in rows if finite(r['seconds'])),
                      **stats(r['seconds'] for r in rows)} for key, rows in stage_groups.items()]
    write_csv(output / 'stage-summary.csv', stage_summary)
    write_csv(output / 'failures.csv', failures)
    write_csv(output / 'request-samples.csv', requests)
    write_csv(output / 'exists-key-rates.csv', [{'backend': b, **d['exists']} for b, d in backends.items()])
    e2e = []
    for backend, data in backends.items():
        for metric, value in (data['profile'] or {}).items():
            if isinstance(value, dict) and any(k in value for k in ('avg', 'p50', 'count')):
                e2e.append({'backend': backend, 'metric': metric, **value})
    write_csv(output / 'e2e-profile-metrics.csv', e2e)
    write_json(output / 'e2e-profiles.json', {b: d['profile'] for b, d in backends.items()})
    outcomes = [{'backend': b, **d['request_outcomes'],
                 'profile_request_count': (d['profile'] or {}).get('request_count', {}).get('avg')}
                for b, d in backends.items()]
    write_csv(output / 'request-outcomes.csv', outcomes)
    cache_rates = [r for data in backends.values() for r in data['server_metrics']['cache_rates']]
    histogram_means = [r for data in backends.values() for r in data['server_metrics']['request_histograms']]
    for filename, rows in [
        ('server-cache-hit-rates', cache_rates), ('server-request-histogram-means', histogram_means),
        ('server-selected-series', [r for d in backends.values() for r in d['server_metrics']['series']]),
        ('server-queue-gauges', [r for d in backends.values() for r in d['server_metrics']['queue_gauges']]),
        ('hca-intervals', [r for d in backends.values() for r in d['hca']['intervals']]),
        ('hca-summary', [r for d in backends.values() for r in d['hca']['summary']])]:
        write_csv(output / (filename + '.csv'), rows)
    write_json(cross / 'summary.json', {'datasets': summary, 'backends': backends,
               'invalid_capture': invalid_capture,
               'native_mooncake_transfer_durations_available': False})
    evidence = []
    for path in sorted(used):
        evidence.append({'path': str(path), 'size_bytes': path.stat().st_size, 'sha256': digest(path)})
    write_json(output / 'provenance.json', {'pair_root': str(root), 'inputs': evidence})
    limits = [
        'Only measured phase result directories are read. Telemetry is selected by completion wall time '
        'inside each AIPerf profiling window, inclusive. Missing windows exclude telemetry; missing values stay N/A.',
        'All eight workers and the guards/Store for each backend share one pod and one node. '
        'This is cross-worker RDMA over the local HCA, not an inter-node wire-throughput benchmark.',
        'KVCR samples are successful cross-agent source_nixl_terminal events, including successful partial '
        'reuse. Partial means a subset of requested keys was served, not transfer failure. Sizes/counts use '
        'actual transferred bytes/source blocks, never requested blocks. Zero-byte lookups are not data transfers; target receipts '
        'are excluded. Native duration is xferDuration. Whole source-operation and post durations remain separate CSV fields.',
        'Mooncake pure and mixed populations are disjoint. Only successful, completely attributed GETs enter '
        'these populations. Both byte and object numerators contain remote data only; self bytes/objects are excluded.',
        'Mooncake duration covers the whole GET API, including metadata/completion and self work for mixed batches. '
        'Replica observation overhead is outside that timer. Native Mooncake transfer-only durations are unavailable. '
        'These scopes cannot establish a native transport speedup or physical NIC utilization.',
        'Mooncake object counts are actual attributed Store objects, not inferred KVCR blocks or distinct hash counts. '
        'No common object byte size or token coverage is assumed. Endpoint ownership uses exact measured-run TE endpoint/PID evidence; conflicts remain unknown.',
        'CDFs and nearest-rank quantiles use exact observations. Mean bandwidth is the arithmetic mean of per-operation rates. '
        'Zero/missing durations have undefined bandwidth and are omitted from bandwidth statistics, with sizes retained.',
        'Stage events, duration events, terminal stage summaries and release summaries overlap and are grouped separately by origin. '
        'Terminal-summary timings are accumulated active time per operation, not individual invocation latency; release may occur after terminal.',
        'Failure events, failed Store objects and AIPerf request errors have different denominators. Missing capture '
        'footers, sequence gaps and other coverage problems are listed below; sampled absence is not proof of zero failures.',
        'Store exists key hit rate is positive_keys / valid_checked_keys, excluding error and unknown keys. '
        'It is distinct from GET served-byte shares and scheduler external-prefix token reuse. No observed exists calls means unavailable rate.',
        'Request latency CDFs cover completed usable request exports only. Aggregate AIPerf profiles, throughput, '
        'error summaries are preserved verbatim in e2e-profiles.json; phase credit/cancellation counters come from '
        'replay.log and are retained in request-outcomes.csv. Units are retained, never inferred.',
        'Byte bins use actual remote payload sizes, lower-inclusive and upper-exclusive, with exact byte boundaries '
        'in the CSV. MiB means 1,048,576 bytes. Block/object bins remain separate; no size is inferred from object counts.',
        'Server cache rates use exported counter totals with identical hit/query label coverage, separately per endpoint. '
        'Exporter reset correction is not assumed. GPU/external token rates and host/guard chunk rates have different denominators. '
        'Guard hits include remote-fetchable hints. Missing counters and zero denominators have unavailable rates.',
        'Server request queue/prefill means use summed histogram sums divided by summed counts, never means of rank means. '
        'Queue-depth gauges retain per-series exported statistics; queue depth is not queue latency. '
        'Histograms and AIPerf end-to-end latency can describe different populations and must not be subtracted to infer unmeasured stages.',
        'Optional HCA input is measured-phase **/hca-port-usage/prometheus-raw.json. Whole-node ports include background traffic. '
        'Only fully in-window observed counter intervals are used; resets are excluded and partial intervals are not extrapolated. '
        'Link utilization requires a positive constant measured node_infiniband_rate_bytes_per_second series. TX/RX are separate.',
    ]
    cross_md = ['# Paired measured cross-worker transfers', '', capture_notice, ''] + ['- ' + line for line in limits[:6]]
    cross_md += ['', *table(summary, ['dataset', 'metric', 'count', 'mean', 'p50', 'p90', 'p99']), '',
                 '[Exact samples](samples.csv) · [CDF points](cdf.csv) · [Size-bin statistics](size-bin-summary.csv) · '
                 '[Conditional CDF points](size-bin-cdf.csv) · [Exact-size statistics](exact-size-summary.csv)', '']
    cross_md += ['Byte bins use actual remote bytes, independent of block/object count. Boundaries are lower-inclusive '
                 'and upper-exclusive; MiB = 1,048,576 bytes.', '',
                 '[Byte-bin bandwidth statistics](byte-bin-summary.csv) · [Byte-bin conditional CDF points](byte-bin-cdf.csv)', '']
    if plots:
        plots_for(samples, requests, bins, byte_bins, output)
        cross_md += [f'![{metric} CDF]({metric}-cdf.png)\n' for metric in METRICS]
        cross_md += ['![Conditional bandwidth CDFs](bandwidth-cdfs-by-size-bin.png)', '',
                     '![Bandwidth mean and quantiles by size](bandwidth-by-size-bin.png)', '',
                     '![Conditional bandwidth CDFs by actual bytes](bandwidth-cdfs-by-byte-bin.png)', '',
                     '![Bandwidth mean and quantiles by actual bytes](bandwidth-by-byte-bin.png)', '',
                     'Every PNG also has a PDF with the same basename.', '']
    (cross / 'report.md').write_text('\n'.join(cross_md))
    md = ['# Paired measured KVCR / Mooncake analysis', '', f'Pair root: `{root}`.', '', capture_notice, '',
          '[Standalone cross-worker comparison](cross-worker/report.md)', '',
          '## End-to-end AIPerf profiles', '',
          '[Full profiles and errors](e2e-profiles.json) · [Profile statistics](e2e-profile-metrics.csv) · '
          '[Exact request samples and units](request-samples.csv)', '']
    metric_rows = [r for r in e2e if r['metric'] in REQUEST_METRICS[:3] +
                   ('request_count', 'request_throughput', 'input_token_throughput', 'output_token_throughput')]
    md += table(metric_rows, ['backend', 'metric', 'unit', 'count', 'avg', 'p50', 'p90', 'p99'])
    md += ['', '### Time per output token', '',
           'TPOT is derived per usable completed response as (request latency − TTFT) / '
           '(generated tokens − 1), retaining matching time units. Single-token responses '
           'and incompatible units are excluded. ITL above remains a separate exported metric.', '',
           '[TPOT statistics](tpot-summary.csv)', '',
           *table(tpot, ['backend', 'unit', 'count', 'mean', 'p50', 'p90', 'p99'])]
    md += ['', '### Request completion and grace cancellations', '',
           'These are literal AIPerf phase credit counters. Cancelled credits are shown separately '
           'from reported errors; an empty error summary does not imply every sent request completed. '
           'Latency distributions describe completed usable responses.', '',
           '[Exact phase outcomes](request-outcomes.csv)', '']
    md += table(outcomes, ['backend', 'sent_credits', 'completed_credits', 'cancelled_credits',
                          'reported_errors', 'profile_request_count', 'grace_period_timeout'])
    if plots:
        md += ['', '![E2E latency CDFs](e2e-latency-cdfs.png)', '']
    # Inline the standalone section, rewriting only its relative artifact links.
    embedded = '\n'.join(cross_md[1:])
    embedded = re.sub(r'\]\(([^)]+)\)', lambda m: '](cross-worker/' + m[1] + ')', embedded)
    md += ['', '## Cross-worker transfer distributions', '', embedded, '',
           '## Server cache rates and request stage means', '',
           '[Selected exported series](server-selected-series.csv) · [Cache rates](server-cache-hit-rates.csv) · '
           '[Request histogram means](server-request-histogram-means.csv) · [Queue depth gauges](server-queue-gauges.csv)', '']
    md += table(cache_rates, ['backend', 'endpoint', 'cache', 'hits', 'queries', 'unit', 'hit_rate'])
    md += ['', *table(histogram_means, ['backend', 'endpoint', 'metric', 'count', 'sum_seconds', 'mean_seconds']), '',
           '## Hardware HCA observations', '', '[Observed intervals](hca-intervals.csv) · [Hardware summary](hca-summary.csv)', '']
    for backend, data in backends.items():
        md += [f'{backend}: ' + ('Observed in-window hardware intervals are available.' if data['hca']['available'] else
                                'Hardware usage unavailable: no usable raw in-window port counters.'), '']
    md += table([r for d in backends.values() for r in d['hca']['summary']],
                ['backend', 'labels', 'direction', 'observed_bytes', 'observed_seconds',
                 'mean_bytes_per_second', 'mean_link_usage_percent', 'counter_resets_excluded'])
    md += ['']
    md += ['Whole-node ports include background traffic; link utilization is unavailable without measured link capacity. '
           'Hardware counters are not attributed to the benchmark or substituted for application transfer bandwidth.', '']
    md += ['', '## Failures, stages and exists keys', '',
           '[Failure observations](failures.csv) · [Exact stage samples](stage-samples.csv) · '
           '[Stage mean and quantiles](stage-summary.csv) · [Exists key rates](exists-key-rates.csv)', '']
    for backend, data in backends.items():
        md += [f'### {backend}', '', f'Capture protocol validity: `{data["capture_valid"]}` (null means unverified).', '',
               f'Profiling bounds (epoch ns): `{data["phase_bounds_ns"]}`.', '',
               f'Observed operation classifications: `{json.dumps(data["operation_counts"], sort_keys=True)}`.', '',
               f'Exists key accounting: `{json.dumps(data["exists"], sort_keys=True)}`.', '']
        md += ['- ' + issue for issue in data['issues']] or ['No input/capture issues detected; all-rank coverage still requires the runner proofs.']
        md += ['']
    md += ['## Interpretation', ''] + ['- ' + line for line in limits] + ['', '[Input hashes](provenance.json)', '']
    (output / 'report.md').write_text('\n'.join(md))
    return {'backends': backends, 'samples': samples, 'invalid_capture': invalid_capture}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    result = analyze(args.root, args.output or args.root / 'analysis')
    print((args.output or args.root / 'analysis') / 'report.md')
    if result['invalid_capture']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
