"""Verify per-series phase scope and reconcile repeated warmup jumps.

Already scoped SDK series remain unchanged; raw exports are never edited.
This validates an offset, not counter continuity: unobservable resets cannot be
reconstructed from exported deltas. A failed reconciliation makes stats null.
"""
from copy import deepcopy
import json
import math


def identity(series):
    return (series.get('endpoint_url', series.get('endpoint')),
            json.dumps(series.get('labels', {}), sort_keys=True))


def close(a, b):
    return (isinstance(a, (int, float)) and isinstance(b, (int, float))
            and math.isfinite(a) and math.isfinite(b)
            and math.isclose(a, b, rel_tol=1e-10, abs_tol=1e-9))


def reconcile(path, original, bounds, used, issues, independent_totals=None):
    import ijson
    result = deepcopy(original)
    selected = {r['metric'] for r in result['series'] if r['type'] in ('counter', 'histogram')}
    if not path.exists() or not selected:
        result['phase_reconciliation'] = {'verified': False, 'verified_series': 0, 'series': []}
        return result
    used.add(path)
    compact = {}; warmup = {}
    for prefix, target in [('metrics', compact), ('warmup_metrics', warmup)]:
        with path.open('rb') as stream:
            for name, metric in ijson.kvitems(stream, prefix, use_float=True):
                if name not in selected:
                    continue
                fields = ('total',) if metric['type'] == 'counter' else ('count', 'sum')
                for s in metric.get('series', []):
                    key = (name, identity(s))
                    if key in target:
                        target[key] = None
                        continue
                    stats = s.get('stats', {})
                    values = {k: stats.get(k, 0 if stats.get('count') == 0 else None) for k in fields}
                    slices=s.get('timeslices', [])
                    active = [t for t in slices if any(t.get(k, 0) != 0 for k in fields)]
                    target[key] = dict(values=values, active=active, fields=fields,
                                       first_slice_start_ns=slices[0].get('start_ns') if slices else None,
                                       slices_contiguous=bool(slices) and all(t['start_ns']<t['end_ns'] for t in slices)
                                           and all(a['end_ns']==b['start_ns'] for a,b in zip(slices,slices[1:])),
                                       timeslices_present='timeslices' in s)
    # Independently instrumented admissions can disambiguate a first slice
    # containing both the repeated warmup value and new profiling traffic.
    independent = set()
    for name, expected in (independent_totals or {}).items():
        pairs = [(p, warmup.get(key)) for key, p in compact.items() if key[0] == name]
        endpoints = {key[1][0] for key in compact if key[0] == name}
        if pairs and len(endpoints) == 1 and all(p and w and 'total' in p['values']
                and close(p['values']['total'], p['values']['total'])
                and close(w['values'].get('total'), w['values'].get('total')) for p, w in pairs):
            if close(sum(p['values']['total']-w['values']['total'] for p, w in pairs), expected):
                independent.add(name)
    evidence = []
    for row in result['series']:
        if row['type'] not in ('counter', 'histogram'):
            continue
        key = (row['metric'], identity(row))
        profile, warm = compact.get(key), warmup.get(key)
        good = bool(bounds and profile and profile['timeslices_present'])
        already_scoped = bool(good and profile['first_slice_start_ns'] is not None
                              and profile['first_slice_start_ns']>=bounds[0])
        method='already_phase_scoped' if already_scoped else 'warmup_offset_subtracted'
        corrected={};initial_reference_delta=None
        reason = 'matched endpoint/labels, totals and first warmup jump'
        if good:
            fields = profile['fields']; active = profile['active']
            sliced={k:sum(t.get(k,0) for t in active) for k in fields}
            exact_total=all(close(sliced[k],profile['values'][k]) for k in fields)
            # Pinned SDK aggregate counters include the pre-window reference
            # delta; right-searchsorted timeslices start at the first in-window
            # value instead. Preserve that nonnegative initial contribution.
            if already_scoped and row['type']=='counter':
                delta=profile['values']['total']-sliced['total'] if close(profile['values']['total'],profile['values']['total']) else None
                if delta is not None and (delta>=0 or close(delta,0)):
                    initial_reference_delta=max(0,delta)
            good = profile['slices_contiguous'] and (exact_total or initial_reference_delta is not None)
            good = good and all(t['start_ns'] >= bounds[0] and t['end_ns'] <= bounds[1] for t in active)
            if already_scoped:
                corrected=dict(profile['values'])
                reason='SDK series window starts inside profiling; no warmup offset subtracted'
                if initial_reference_delta:
                    reason+='; aggregate initial reference contribution retained separately'
            elif warm is not None:
                has_warmup = any(warm['values'][k] != 0 for k in fields)
                if has_warmup:
                    good = good and bool(active) and 0 <= active[0]['start_ns'] - bounds[0] <= 2_000_000_000
                    exact = bool(active) and all(close(active[0].get(k, 0), warm['values'][k]) for k in fields)
                    admitted = (row['metric'] in independent and bool(active)
                                and active[0].get('total', -1) >= warm['values'].get('total', math.inf))
                    good = good and (exact or admitted)
                    if admitted and not exact:
                        reason = 'warmup offset plus new first-slice traffic; aggregate matches independent admissions'
                corrected = {k: profile['values'][k] - warm['values'][k]
                             if close(profile['values'][k], profile['values'][k])
                             and close(warm['values'][k], warm['values'][k]) else None for k in fields}
            else:
                good=False
            good = good and all(v is not None and v >= 0 for v in corrected.values())
        if not good:
            reason = 'unverified warmup offset, identity, boundary, or slice/total consistency'
            corrected = {}
            issues.append(f"Server metric phase reconciliation unavailable: {key}: {reason}")
        row['exported_stats'] = row['stats']
        row['stats'] = corrected
        evidence.append(dict(metric=row['metric'], endpoint=row['endpoint'], labels=row['labels'],
            verified=good, reason=reason, method=method,
            initial_reference_counter_delta=initial_reference_delta,
            export_window_start_ns=profile['first_slice_start_ns'] if profile else None,
            warmup_offset=warm['values'] if warm and not already_scoped else None))

    definitions = {'GPU prefix': ('vllm:prefix_cache', None),
        'External prefix': ('vllm:external_prefix_cache', None),
        'Framework primary host': ('vllm:kv_offload_tiering_chunk', '0:primary'),
        'Guard/KVCR secondary tier': ('vllm:kv_offload_tiering_chunk', '1:kvcr')}
    scope = ('Profiling through drain; SDK series already scoped to profiling retained, '
             'otherwise only a verified repeated warmup offset subtracted per endpoint/labels; '
             'nonzero boundary slices rejected; unobservable counter resets are not corrected')
    for cache in result['cache_rates']:
        prefix, tier = definitions[cache['cache']]
        totals = []; coverage = []
        for suffix in ('_hits', '_queries'):
            rows = [r for r in result['series'] if r['endpoint'] == cache['endpoint']
                    and r['metric'] == prefix + suffix
                    and (tier is None or r['labels'].get('tier') == tier)]
            valid = [r for r in rows if close(r['stats'].get('total'), r['stats'].get('total'))]
            totals.append(sum(r['stats']['total'] for r in valid) if rows and len(valid) == len(rows) else None)
            coverage.append({json.dumps(r['labels'], sort_keys=True) for r in valid})
        hits, queries = totals
        paired = bool(coverage[0]) and coverage[0] == coverage[1]
        cache.update(hits=hits, queries=queries, matching_series_coverage=paired, scope=scope,
            hit_rate=hits/queries if paired and hits is not None and queries is not None and 0 <= hits <= queries and queries > 0 else None)
    for hist in result['request_histograms']:
        rows = [r for r in result['series'] if r['endpoint'] == hist['endpoint'] and r['metric'] == hist['metric']]
        good = bool(rows) and all('count' in r['stats'] and 'sum' in r['stats'] for r in rows)
        count = sum(r['stats']['count'] for r in rows) if good else None
        total = sum(r['stats']['sum'] for r in rows) if good else None
        hist.update(count=count, sum_seconds=total, mean_seconds=total/count if count else None, scope=scope)
    result['phase_reconciliation'] = dict(verified=bool(evidence) and all(r['verified'] for r in evidence),
        verified_series=sum(r['verified'] for r in evidence), series=evidence, scope=scope)
    return result
