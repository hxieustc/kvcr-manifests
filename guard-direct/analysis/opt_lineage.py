"""Correlate key histories by export-index linearization, not flusher order."""
from collections import defaultdict


def correlate(records, bounds):
    mutations = defaultdict(list)
    gets = defaultdict(list)
    for record in records:
        owner = (record.get('agent'), record.get('incarnation'))
        event = record.get('event')
        if event in ('ready', 'withdraw'):
            sequence = record.get('export_sequence')
            if sequence is None:
                raise ValueError('Index transition lacks linearization sequence')
            mutations[owner].append((sequence, event, record['key'], record.get('generation'),
                                      record.get('reason'), record.get('transition_ns')))
        elif event == 'source_get' and bounds[0] <= record.get('wall_ns', -1) <= bounds[1]:
            gets[owner].append(record)
    result = []
    for owner, requests in gets.items():
        events = sorted(mutations[owner])
        if len({r[0] for r in events}) != len(events):
            raise ValueError('Duplicate index mutation sequence')
        requests.sort(key=lambda r: (r.get('export_sequence') is None,
                                    r.get('export_sequence') or 0, r.get('wall_ns', 0)))
        history = {}
        position = 0
        for request in requests:
            snapshot = request.get('export_sequence')
            if snapshot is not None:
                while position < len(events) and events[position][0] <= snapshot:
                    entry = events[position]
                    history[entry[2]] = entry
                    position += 1
            indices = set(request.get('ready_indices', ()))
            keys = request['keys']
            reasons = request.get('miss_reasons', ['unknown'] * len(keys))
            generations = request.get('generations', [None] * len(keys))
            if len(reasons) != len(keys) or len(generations) != len(keys):
                raise ValueError('GET reason/generation cardinality mismatch')
            for index, key in enumerate(keys):
                previous = history.get(key) if snapshot is not None else None
                if index in indices:
                    classification = 'ready'
                elif reasons[index] != 'absent':
                    classification = reasons[index]
                elif previous is None:
                    classification = 'never_observed_ready_at_source'
                elif previous[1] == 'withdraw':
                    classification = 'withdrawn:' + str(previous[4])
                else:
                    classification = 'miss_with_ready_history_review_required'
                result.append(dict(agent=owner[0], incarnation=owner[1],
                    operation_key=request.get('operation_key'), request_id=request.get('request_id'),
                    request_correlation=request.get('request_correlation'), wall_ns=request.get('wall_ns'),
                    export_sequence=snapshot, key=key.hex(), key_index=index,
                    group_suffix=key[-4:].hex() if len(key) >= 4 else None,
                    ready=index in indices, claim_reason=reasons[index], classification=classification,
                    claim_generation=generations[index], prior_generation=previous[3] if previous else None,
                    prior_transition_ns=previous[5] if previous else None))
    return result
