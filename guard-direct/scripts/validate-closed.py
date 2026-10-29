#!/usr/bin/env python3
"""Validate every expected worker/Guard capture and write analyzer-compatible evidence."""
import argparse
import json
from pathlib import Path
from workflow import atomic, read


def validate(root, result, validator):
    root, result = Path(root), Path(result)
    proofs = [read(p) for p in result.glob('option-a-rank-*.json')]
    receipt = dict(valid=False, validator_version='direct-guard-reproduction-v1')
    target = result / 'capture-validity.json'
    try:
        if len(proofs) != 8 or {p['rank'] for p in proofs} != set(range(8)):
            raise ValueError('Missing eight unique serving-rank proofs')
        health = [read(p) for p in (root / 'raw/kvcr/phases/measured/telemetry-health').glob('kvcr-*.json')]
        if not health:
            raise ValueError('No capture health evidence')
        identities = set()
        pids = {int(p['pid']) for p in proofs}
        for item in health:
            if (not item.get('closed') or item.get('error') or item.get('dropped')
                    or item.get('unflushed') or item.get('instrumentation_errors')):
                raise ValueError('Capture health is not closed/loss-free: ' + str(item.get('pid')))
            identities.update(item.get('identities', [])); pids.add(int(item['pid']))
        if not {p['agent'] for p in proofs} <= identities:
            raise ValueError('A serving rank is absent from capture health identities')
        if len([name for name in identities if name.startswith('KVCR-Guard-')]) != 8:
            raise ValueError('Missing eight Guard capture identities')
        details = validator(result / 'nixl-transfers', sorted(identities))
        if not details.get('valid') or pids - {int(item['pid']) for item in details['files']}:
            raise ValueError('Invalid capture or missing expected PID')
        receipt.update(valid=True, expected_identities=sorted(identities), expected_pids=sorted(pids),
                       full_validation=details)
    except Exception as error:
        receipt['reason'] = f'{type(error).__name__}: {error}'
    atomic(target, receipt); atomic(root / 'receipts/closed-capture.json', receipt)
    return receipt


if __name__ == '__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,required=True);parser.add_argument('--result',type=Path,required=True)
    args=parser.parse_args()
    from kvcr.compact_telemetry import validate_capture
    receipt=validate(args.root,args.result,validate_capture)
    print(json.dumps(receipt))
    if not receipt['valid']:raise SystemExit(1)
