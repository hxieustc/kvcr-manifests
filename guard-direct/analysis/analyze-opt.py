#!/usr/bin/env python3
"""Offline compact-capture supplement; never writes raw inputs or the base report."""
import argparse
from collections import Counter, defaultdict
import csv
import importlib.util
import json
import os
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import observations as obs
import reference_agentx as ref
from opt_lineage import correlate
from capture_scope import capture_inventory, metadata_parents, measured_rank_paths, separate_canary_evidence

legacy = ref.legacy()
CLOCKS = ('KVCR native', 'KVCR complete source', 'KVCR core source_write', 'KVCR receive-to-cleanup')
STAGES = (
    ('receive_to_claim', 'source_receive_ns', 'claim_begin_ns'),
    ('claim', 'claim_begin_ns', 'claim_end_ns'),
    ('claim_to_submit', 'claim_end_ns', 'submit_begin_ns'),
    ('submit', 'submit_begin_ns', 'submit_end_ns'),
    ('submit_begin_to_done', 'submit_begin_ns', 'first_observed_done_ns'),
    ('done_to_native_release', 'first_observed_done_ns', 'native_handle_released_ns'),
    ('native_release_to_cleanup', 'native_handle_released_ns', 'cleanup_done_ns'),
    ('receive_to_cleanup', 'source_receive_ns', 'cleanup_done_ns'),
)

REPORT_SCOPE_WARNINGS = (
    'Comparison clocks are non-equivalent: Mooncake whole Store GET API includes metadata and self work in mixed batches; KVCR native/source-operation clocks and target GPU-job clocks have different boundaries. Direct after-first-API-return-to-cleanup starts after the first public API returns. These rates do not establish an equivalent-clock speedup. TTFT is the primary user-visible comparison; retain its observed profiling cohort.',
    'Client throughput uses the retained profiling-through-drain denominator, not just the configured send window. Preserve observed phase seconds, SDK aggregates and completed/cancelled cohorts separately; summed overlapping call durations are not benchmark wall time.',
    'Cross-agent means another process in this same-node HCA topology, not off-node traffic. Whole-port HCA counters include background and local loopback traffic; they do not attribute per-operation wire bytes or establish inter-node bandwidth.',
)


def compact_format():
    path = HERE / 'compact_format.py'
    if not path.exists():  # Local development; sealed bundles supply the exact copied format.
        path = HERE.parent / 'software/kvcr/src/kvcr/compact_telemetry.py'
    spec = importlib.util.spec_from_file_location('opt_compact_format', path)
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    return module, path


def read_csv(path):
    if not path.exists(): return []
    with path.open() as stream:
        rows = list(csv.DictReader(stream))
    for row in rows:
        for key, value in row.items():
            try: row[key] = float(value) if value else None
            except (ValueError, TypeError): pass
    return rows


def serial(value):
    if isinstance(value, bytes): return value.hex()
    if isinstance(value, dict): return {k: serial(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)): return [serial(v) for v in value]
    return value


def duration(stamps, start, end):
    a, b = stamps.get(start), stamps.get(end)
    return (b - a) / 1e9 if legacy.finite(a) and legacy.finite(b) and b >= a else None


def outcome_group(row):
    """Classify observed source terminals; unknown failures stay unclassified."""
    reasons = {r for r in row.get('reasons', ()) if isinstance(r, str)}
    if isinstance(row.get('reason'), str): reasons.add(row['reason'])
    native = row.get('native') or {}
    success = row.get('result') == 'success'
    # Positive recovered transfers remain successful; raw reasons still survive.
    if success and legacy.finite(row.get('blocks')) and row['blocks'] > 0:
        if row.get('status') == 'partial': return 'success/partial'
        if row.get('status') == 'success': return 'success/full'
    native_failure = (native.get('xfer_state') == 'ERR'
        or any(r == 'native_submission_failed' or r.startswith(('native_state:', 'native_poll_exception:', 'submit_exception:')) for r in reasons))
    if ('source_progress_gap' in reasons and not success and not native_failure
            and native.get('submitted') is not True and native.get('xfer_state') is None
            and (row.get('bytes') is None or row.get('bytes') == 0)):
        return 'pre-submit/source-epoch-refusal'
    if native_failure or native.get('submitted') is False:
        return 'nativeERR/ambiguous'
    if any(r in ('shutdown', 'stopping') or 'deadline_expired' in r or 'cancelled' in r for r in reasons):
        return 'timeout/cancel'
    if reasons & {'malformed_op_handle', 'malformed_start_write', 'malformed_cardinality',
                  'peer_metadata_validation_failed', 'selected_descriptor_validation_failed',
                  'duplicate_write_request', 'source_incarnation_changed', 'source_stalled',
                  'route_generation_changed', 'control_enqueue_failed'}:
        return 'setup/refusal'
    if reasons & {'guard_ready_miss', 'no_ready', 'all_missing'}:
        return 'no_READY(cachemiss)'
    return 'unclassified'


def histogram_tables(aggregates):
    counts = {}; edges = [0, 10000, 100000, 1000000, 10000000, 100000000, 1000000000, None]
    for row in aggregates:
        if row['event'] != 'progress_schedule': continue
        for name, values in row.items():
            if not (name.startswith(('active_', 'idle_')) and name.endswith('_histogram')): continue
            if not (isinstance(values, list) and len(values) == 7 and all(type(v) is int and v >= 0 for v in values)):
                raise ValueError('Invalid progress histogram: ' + name)
            counts[name] = [a + b for a, b in zip(counts.get(name, [0] * 7), values)]
    buckets = []; summaries = []
    for name, values in sorted(counts.items()):
        total = sum(values); cumulative = 0; summary = dict(histogram=name, count=total)
        for i, count in enumerate(values):
            cumulative += count
            buckets.append(dict(histogram=name, lower_ns=edges[i], lower_exclusive=i > 0, upper_inclusive_ns=edges[i+1], count=count,
                                cumulative_count=cumulative, cdf=cumulative / total if total else None))
            for q in (50, 90, 99):
                if total and cumulative >= total * q / 100 and f'p{q}_lower_ns' not in summary:
                    summary.update({f'p{q}_lower_ns': edges[i], f'p{q}_upper_ns': edges[i+1]})
        summaries.append(summary)
    return buckets, summaries


def plot_bins(output, tables):
    import matplotlib.pyplot as plt
    for kind, field, filename in (('object', 'blocks_or_objects_bin', 'size'), ('byte', 'byte_bin', 'byte')):
        rows = [r for r in tables[f'transfer-{kind}-bins'] if r['metric'] == 'GBps']
        labels = list(dict.fromkeys(r[field] for r in rows))
        fig, axes = plt.subplots(1, len(CLOCKS), figsize=(15, 4), layout='constrained')
        for ax, clock in zip(axes, CLOCKS):
            selected = [r for r in rows if r['dataset'] == clock]
            for metric in ('mean', 'p50', 'p99'): ax.plot(range(len(selected)), [r[metric] for r in selected], 'o-', label=metric)
            ax.set(title=clock, ylabel='GB/s (decimal)', xlabel='Actual subset ' + kind + ' bin')
            ax.set_xticks(range(len(labels)), labels, rotation=60); ax.legend(fontsize=7)
        directory = output / 'cross-worker'; directory.mkdir(exist_ok=True)
        fig.savefig(directory / f'bandwidth-by-{filename}-bin.png', dpi=140); plt.close(fig)
        fig, axes = plt.subplots(2, 4, figsize=(16, 8), layout='constrained')
        cdfs = tables[f'transfer-{kind}-bin-cdf']
        for ax, label in zip(axes.flat, labels):
            for clock in CLOCKS:
                selected = [r for r in cdfs if r[field] == label and r['dataset'] == clock and r.get('metric', 'GBps') == 'GBps']
                if selected: ax.step([r.get('GBps', r.get('value')) for r in selected], [r['cdf'] for r in selected], where='post', label=clock)
            ax.set(title=label, xlabel='GB/s', ylabel='CDF', ylim=(0, 1))
            if ax.lines: ax.legend(fontsize=6)
        fig.savefig(directory / f'bandwidth-cdfs-by-{filename}-bin.png', dpi=140); plt.close(fig)


def analyze(root, base, output, plots=True):
    root, base, output = (Path(p).resolve() for p in (root, base, output))
    if any(output == p or p in output.parents or output in p.parents for p in (root, base)):
        raise ValueError('Output must be outside raw and base input trees')
    issues = []; used = {base / 'report.json'}
    baseline = json.loads((base / 'report.json').read_text())
    bounds = baseline.get('phase_bounds_ns')
    if not (isinstance(bounds, list) and len(bounds) == 2 and all(legacy.finite(v) for v in bounds) and bounds[0] < bounds[1]):
        bounds = None; issues.append('Missing/invalid base profile bounds; no compact profile population selected')
    def inside(stamp): return bounds is not None and legacy.finite(stamp) and bounds[0] <= stamp <= bounds[1]
    fmt, format_path = compact_format(); used.add(format_path)
    scope = capture_inventory(root)
    directories = [Path(p) for p in scope['selected_directories']]
    used.add(HERE/'capture_scope.py')
    validations = []; records = []; events = []
    for directory in directories:
        expected = set(); metadata_valid = True
        expected_pids = set()
        for parent in metadata_parents(directory):
            for filename in ('capture-validity.json', 'event-monitor.json'):
                path = parent / filename
                if not path.exists(): continue
                used.add(path); evidence = json.loads(path.read_text())
                if filename == 'event-monitor.json':
                    expected.update(evidence.get('agent_identities', {}))
                    metadata_valid &= not evidence.get('fatal', False)
                else:
                    expected.update(evidence.get('expected_identities', evidence.get('identities', ())))
                    metadata_valid &= evidence.get('valid') is True
                    expected_pids.update(evidence.get('expected_pids', ()))
            if parent.name == 'measured': break
        try:
            validation = fmt.validate_capture(directory, sorted(expected))
            captured_pids={int(r['pid']) for r in validation.get('files',[])}
            if expected_pids-captured_pids:raise ValueError('Missing expected measured PIDs: '+str(sorted(expected_pids-captured_pids)))
            validation.update(expected_identities=sorted(expected), expected_pids=sorted(expected_pids), metadata_valid=metadata_valid)
            validation['valid'] &= bool(expected) and metadata_valid
            validations.append(validation)
            if not expected: issues.append('Missing expected measured identities for ' + str(directory))
            if not metadata_valid: issues.append('Measured capture-validity/event-monitor evidence reports failure')
        except (fmt.CaptureInvalid, ValueError) as error:
            validations.append(dict(valid=False, path=str(directory), error=str(error)))
            issues.append('Invalid compact capture: ' + str(error))
        for path in sorted(directory.glob('source-*.jsonl')):
            used.add(path)
            try:
                with path.open() as stream:
                    events.extend(dict(json.loads(line), source_file=str(path)) for line in stream)
            except (ValueError, OSError) as error: issues.append('Unreadable compact JSON: ' + str(error))
        for path in sorted(directory.glob('keys-*.bin')):
            used.add(path)
            try: records.extend(fmt.iter_key_journal(path))
            except fmt.CaptureInvalid as error: issues.append('Unreadable key journal: ' + str(error))
    valid = bool(validations) and all(v['valid'] for v in validations)
    if not directories: issues.append('No measured benchmark captures; prepare/smoke/canaries remain separate evidence')
    output.mkdir(parents=True, exist_ok=True)
    queries = Counter(); progress = Counter(); intervals = Counter(); aggregates = []
    for row in events:
        if row.get('event') not in ('query_outcomes', 'progress_schedule'): continue
        a, b = row.get('interval_start_ns'), row.get('interval_end_ns')
        if not (inside(a) and inside(b) and a <= b):
            intervals['excluded'] += 1
            if not (legacy.finite(a) and legacy.finite(b)): intervals['missing_boundaries'] += 1
            continue
        intervals['included'] += 1
        aggregates.append(row)
        if row['event'] == 'query_outcomes': queries.update(row.get('query_status_tier_counts', {}))
        else:
            progress.update({k: v for k, v in row.items() if k.endswith(('_turns', '_wall_ns', '_cpu_ns')) and legacy.finite(v)})
    if intervals['missing_boundaries']: issues.append('Aggregate intervals missing explicit boundaries; excluded')
    try: histograms, histogram_summary = histogram_tables(aggregates)
    except ValueError as error:
        histograms, histogram_summary = [], []; issues.append(str(error))
    expected_histograms = {kind + '_' + field + '_histogram' for kind in ('active', 'idle') for field in ('wall', 'cpu', 'start_gap', 'end_to_start_gap')}
    missing_histograms = sorted(expected_histograms - {r['histogram'] for r in histogram_summary})
    if missing_histograms: issues.append('Missing measured progress histograms: ' + ', '.join(missing_histograms))
    lineage = []; withdrawals = []; ready_times = {}; gets = {}
    for row in records:
        identity = (row.get('agent'), row.get('incarnation'), row.get('key'), row.get('generation'))
        if row.get('event') == 'ready': ready_times[identity] = row.get('transition_ns')
        if row.get('event') == 'source_get': gets[(row.get('agent'), row.get('incarnation'), row.get('operation_key'), row.get('wall_ns'))] = row
    for row in records:
        if row.get('event') != 'withdraw': continue
        identity = (row.get('agent'), row.get('incarnation'), row.get('key'), row.get('generation'))
        age = duration(dict(start=ready_times.get(identity), end=row.get('transition_ns')), 'start', 'end')
        withdrawals.append(dict(serial(row), ready_to_withdraw_seconds=age))
    if bounds:
        try: lineage = correlate(records, bounds)
        except ValueError as error: valid = False; issues.append('Invalid lineage: ' + str(error))
    for row in lineage:
        get = gets.get((row['agent'], row['incarnation'], row['operation_key'], row['wall_ns']), {})
        row['withdrawal_age_seconds'] = duration(dict(start=row['prior_transition_ns'], end=get.get('monotonic_ns')), 'start', 'end') if row['classification'].startswith('withdrawn:') else None
    selected = [r for r in events if r.get('event') == 'source_nixl_terminal' and inside(r.get('end_wall_ns', r.get('wall_ns')))]
    selected = [dict(r, outcome_group=outcome_group(r)) for r in selected]
    outcome_counts = Counter(r['outcome_group'] for r in selected)
    outcome_summary = [dict(outcome_group=group, count=count) for group, count in sorted(outcome_counts.items())]
    samples = []; stages = []; missing = Counter()
    for row in selected:
        stamps = row.get('timing_ns', {})
        for name, start, end in STAGES:
            seconds = duration(stamps, start, end)
            if seconds is None: missing[name] += 1
            done, submit_end = stamps.get('first_observed_done_ns'), stamps.get('submit_end_ns')
            inline = done <= submit_end if legacy.finite(done) and legacy.finite(submit_end) else None
            stages.append(dict(operation_key=row.get('operation_key'), stage=name, wall_seconds=seconds, cpu_seconds=None, inline_done=inline))
        stages.append(dict(operation_key=row.get('operation_key'), stage='handling_cpu', wall_seconds=None,
                           cpu_seconds=row['source_handling_cpu_ns'] / 1e9 if legacy.finite(row.get('source_handling_cpu_ns')) else None))
        if row.get('result') != 'success' or not legacy.finite(row.get('bytes')) or row['bytes'] <= 0: continue
        if not row.get('source_agent') or not row.get('target_agent') or row['source_agent'] == row['target_agent']:
            issues.append('Transfer lacks distinct source/target identities; bandwidth sample excluded'); continue
        start = row.get('start_monotonic_seconds')
        core = duration(dict(start=round(start * 1e9) if legacy.finite(start) else None,
                             end=stamps.get('core_source_write_ns')), 'start', 'end')
        for clock, seconds in zip(CLOCKS, (row.get('native_seconds'), row.get('source_operation_seconds'), core, duration(stamps, 'source_receive_ns', 'cleanup_done_ns'))):
            if legacy.finite(seconds) and seconds > 0:
                samples.append(dict(legacy.sample(row, clock, row['bytes'], row.get('blocks'), seconds, clock), bytes_source=row.get('bytes_source')))
            else: missing[clock] += 1
    if missing: issues.append('Missing stage/clock evidence: ' + json.dumps(dict(missing), sort_keys=True))
    requested = sum(len(r['keys']) for r in records if r.get('event') == 'source_get' and inside(r.get('wall_ns')))
    ready = sum(len(r.get('ready_indices', ())) for r in records if r.get('event') == 'source_get' and inside(r.get('wall_ns')))
    has_gets = any(r.get('event') == 'source_get' and inside(r.get('wall_ns')) for r in records)
    proof_rows = selected
    pins = [r for r in events if inside(r.get('end_wall_ns', r.get('wall_ns'))) and (r.get('has_framework_pins') or r.get('event') in ('forbidden_source_pin', 'capture_error'))]
    proof = dict(verified=valid and bool(proof_rows) and not pins and all(r.get('guard_only_sources') is True and r.get('has_framework_pins') is False and r.get('initial_caller_handoff') == 'absent_by_guard_only_progress_path' for r in proof_rows), source_operations=len(proof_rows), violations=len(pins), phase_bounds_ns=bounds, scope='Measured-profile source terminals and violations only; full-file protocol validity required')
    ranks = []
    for path in measured_rank_paths(root):
        used.add(path); ranks.append(json.loads(path.read_text()))
    strict = dict(verified={r.get('rank') for r in ranks} == set(range(8)) and len(ranks) == 8 and all(r.get('guard_only_sources') is True and r.get('guard_only_source_proof') is True and r.get('opportunistic_query') is False for r in ranks), observed=ranks)
    if not proof['verified']: issues.append('Guard-only/no-framework-pins proof incomplete or violated')
    if not strict['verified']: issues.append('Strict runtime config proof for exactly eight measured ranks unavailable or invalid')
    requests = read_csv(base / 'requests.csv')
    stage_summary = [dict(stage=name, metric=metric, **legacy.stats(r[metric] for r in stages if r['stage'] == name))
                     for name in dict.fromkeys(r['stage'] for r in stages) for metric in ('wall_seconds', 'cpu_seconds')]
    canaries=separate_canary_evidence(root,used)
    tables = {'separate-canary-evidence':canaries,'requests': requests, 'transfer-samples': samples, 'operation-stages': stages, 'stage-summary': stage_summary,
              'source-operation-outcomes': selected, 'query-outcomes': [r for r in aggregates if r['event'] == 'query_outcomes'],
              'source-outcome-summary': outcome_summary,
              'progress-histograms': histograms, 'progress-histogram-summary': histogram_summary,
              'progress-scheduling': [r for r in aggregates if r['event'] == 'progress_schedule'],
              'key-journal': [serial(r) for r in records], 'key-lineage': lineage, 'withdrawals': withdrawals,
              'base-transfer-samples': read_csv(base / 'transfer-samples.csv'), 'base-transfer-cdf': read_csv(base / 'transfer-cdf.csv')}
    old = legacy.DATASETS
    try:
        legacy.DATASETS = CLOCKS
        for name, rows in zip(('transfer-summary', 'transfer-cdf', 'transfer-object-bins', 'transfer-object-bin-cdf', 'transfer-exact-bytes'), legacy.distributions(samples)): tables[name] = rows
        tables['transfer-byte-bins'], tables['transfer-byte-bin-cdf'] = legacy.byte_distributions(samples)
    finally: legacy.DATASETS = old
    tables['latency-cdf'] = [dict(metric=m, value=v, cdf=p) for m in ('ttft_seconds', 'tpot_seconds') for v, p in legacy.ecdf(r.get(m) for r in requests)]
    for name, rows in tables.items(): legacy.write_csv(output / (name + '.csv'), rows)
    links = {p.name: os.path.relpath(p, output) for p in sorted(base.iterdir()) if p.is_file()}
    used.update(p for p in base.iterdir() if p.is_file())
    report = dict(status='incomplete' if issues else 'evidence_available', phase_bounds_ns=bounds,
        capture_scope=scope, separate_canary_evidence=canaries, capture_valid=valid if directories else None, capture_validation=validations, lineage_valid=valid and bounds is not None,
        query_counts=dict(queries), progress_totals=dict(progress), aggregate_intervals=dict(intervals),
        progress_histogram_summary=histogram_summary, missing_progress_histograms=missing_histograms,
        ready_yield=dict(numerator=ready if has_gets else None, denominator=requested if has_gets else None,
                        rate=ready/requested if requested else None, scope='Profile source GET key claims; not transferred bytes or token cache hits'),
        lineage_counts=dict(Counter(r['classification'] for r in lineage)), journal_counts=dict(Counter(r['event'] for r in records)),
        failure_reasons=dict(Counter(r.get('reason', 'unavailable') for r in selected if r.get('result') != 'success')),
        outcome_counts=dict(outcome_counts),
        withdrawal_age_summary=legacy.stats(r.get('withdrawal_age_seconds') for r in lineage),
        missing_stages=dict(missing), guard_only_proof=proof, strict_rank_proof=strict,
        latency_summary={m: legacy.stats(r.get(m) for r in requests) for m in ('ttft_seconds', 'tpot_seconds')},
        base_outputs=links, base_status=baseline.get('status'), issues=issues,
        limitations=[*REPORT_SCOPE_WARNINGS,
            'Actual remote bytes are successful selected-subset bytes only; requested bytes and failed-transfer bytes are not inferred.',
            'Raw failure_reasons retain terminal labels, not RDMA error counts. no_READY(cachemiss) is expected cache unavailability; nativeERR/ambiguous includes submission/poll uncertainty, not necessarily a hardware fault. Unknown failures stay unclassified.',
            'Complete source uses source_operation_seconds (body start through source_end after cleanup); receive-to-cleanup and exact core source_write clocks are separate.',
            'submit_begin_to_done includes native submission; inline DONE can precede submit_end. inline_done is explicit, with no negative post-submit wait inferred.',
            'Active/idle histogram quantiles are bounded buckets (lower exclusive except zero; upper inclusive; null upper means unbounded), not exact time estimates.',
            'Per-stage CPU is unavailable without explicit stage CPU instrumentation; handling CPU is accumulated active work, not CPU across waits.',
            'Whole-port traffic includes background and same-node HCA traffic; it is not proof of off-node bandwidth.',
            'Aggregate intervals crossing profile boundaries are excluded, never prorated; missing denominators are not reconstructed.',
            'Base tiered-cache, worker, occupancy and throughput outputs retain their original scopes; advisory hints do not prove READY residency.',
            'Loss or invalid capture invalidates exact lineage; diagnostic CSV rows remain available but are not complete-history proof.',
            'Offline analysis retains full journals and lineage in memory; memory grows with journal size and can reach GB for a full run.'])
    legacy.write_json(output / 'report.json', report)
    text = '# New Guard optimization analysis\n\nStatus: ' + report['status'] + '\n\n'
    for title, rows, columns in [('Client latency', [dict(metric=m, **s) for m, s in report['latency_summary'].items()], ['metric', 'count', 'mean', 'p50', 'p90', 'p99']),
        ('Measured source outcomes', outcome_summary, ['outcome_group', 'count']),
        ('Transfer clocks and bandwidth', tables['transfer-summary'], ['dataset', 'metric', 'count', 'mean', 'p50', 'p90', 'p99']),
        ('Operation stages (overlapping clocks)', stage_summary, ['stage', 'metric', 'count', 'mean', 'p50', 'p99']),
        ('Progress histogram quantile bounds', histogram_summary, ['histogram', 'count', 'p50_lower_ns', 'p50_upper_ns', 'p90_lower_ns', 'p90_upper_ns', 'p99_lower_ns', 'p99_upper_ns']),
        ('READY yield', [report['ready_yield']], ['numerator', 'denominator', 'rate', 'scope'])]:
        text += '\n## ' + title + '\n\n' + '\n'.join(legacy.table(rows, columns)) + '\n'
    text += f"\nGuard-only capture proof: {proof['verified']}; strict eight-rank runtime proof: {strict['verified']}; exact lineage valid: {report['lineage_valid']}.\n"
    text += '\n## Artifacts\n\n' + '\n'.join(f'- [{name}.csv]({name}.csv)' for name in tables)
    if plots:
        text += '\n\n' + '\n\n'.join(f'![{name}]({name}.png)' for name in ('latency-cdf', 'transfer-cdf', 'cross-worker/bandwidth-by-size-bin', 'cross-worker/bandwidth-by-byte-bin'))
    text += '\n\n## Base evidence\n\n' + '\n'.join(f'- [{name}]({link})' for name, link in links.items())
    text += '\n\n## Limits\n\n' + '\n'.join('- ' + s for s in report['limitations'])
    text += '\n\n## Evidence gaps\n\n' + ('\n'.join('- ' + s for s in issues) or 'None observed.') + '\n'
    (output / 'report.md').write_text(text)
    used.update((Path(__file__).resolve(), HERE / 'opt_lineage.py', HERE / 'observations.py', HERE / 'reference_agentx.py', Path(legacy.__file__)))
    legacy.write_json(output / 'input-hashes.json', [dict(path=str(p), sha256=legacy.digest(p)) for p in sorted(used)])
    if plots:
        obs.plots(output, requests, samples, [], [], [], [])
        plot_bins(output, tables)
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('root', 'base', 'output'): parser.add_argument('--' + name, required=True, type=Path)
    parser.add_argument('--no-plots', action='store_true'); args = parser.parse_args()
    report = analyze(args.root, args.base, args.output, not args.no_plots)
    print(json.dumps(dict(status=report['status'], issues=report['issues'])))
