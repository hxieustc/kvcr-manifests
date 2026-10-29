"""Explicit benchmark capture population; never fall back to prepare/smoke.

Selection is by measured result ownership, not by event timestamps or a directory
whose name merely contains 'measured'. Whole selected files keep prior lineage.
"""
from pathlib import Path
import hashlib
import json

_PACKAGED = {'analysis-tools', 'bundle', 'software', 'verification', 'validation', 'provenance', 'tests'}


def _phase_pairs(path):
    parts = Path(path).parts
    return [parts[i+1] for i, part in enumerate(parts[:-1]) if part == 'phases']


def _canonical_run(path):
    return (path.name == 'guard' and path.parent.name == 'results'
            and path.parent.parent.name == 'measured'
            and path.parent.parent.parent.name == 'phases'
            and all(phase == 'measured' for phase in _phase_pairs(path)))


def measured_runs(root):
    root = Path(root).resolve()
    candidates = list(root.glob('**/results/guard'))
    candidates.extend(p for p in (root, *root.parents) if _canonical_run(p))
    # Preserve the historical explicit results-root API. A marker under an
    # explicitly nonbenchmark phase cannot turn that phase into profiling.
    explicit = root.parent if root.name == 'nixl-transfers' else root
    if (explicit/'replay-measured').is_dir() and not any(p != 'measured' for p in _phase_pairs(explicit)):
        candidates.append(explicit)
    selected = set()
    for path in candidates:
        path = path.resolve()
        if not path.is_dir() or any(p != 'measured' for p in _phase_pairs(path)): continue
        prefix = path.relative_to(root).parts if root in path.parents else ()
        if any(p in _PACKAGED for p in prefix): continue
        if _canonical_run(path) or path == explicit and (path/'replay-measured').is_dir():
            selected.add(path)
    return sorted(selected)


def capture_inventory(root):
    root = Path(root).resolve()
    runs = measured_runs(root)
    selected = {run/'nixl-transfers' for run in runs if (run/'nixl-transfers').is_dir()}
    observed = {p.parent for p in root.rglob('source-*.jsonl')}
    return dict(
        version='measured-only-v1',
        selected_runs=[str(p) for p in runs],
        selected_directories=[str(p) for p in sorted(selected)],
        excluded_directories=[dict(path=str(p), phases=_phase_pairs(p),
            reason='Not a selected measured result capture; retained as separate evidence') for p in sorted(observed-selected)],
        benchmark_population='observed' if runs else 'not_observed',
        protocol_scope='Whole selected measured capture files, including pre-profile records',
        metric_scope='Measured timestamp bounds; prior READY/withdraw history is retained for lineage',
        nonbenchmark_scope='Prepare, smoke, canaries and packaged fixtures do not affect benchmark capture validity',
    )


def metadata_parents(directory):
    """Walk to the owning measured phase, even for a scoped --root result path."""
    directory = Path(directory).resolve()
    run = directory.parent
    stop = run.parent.parent if _canonical_run(run) else run
    for parent in (directory, *directory.parents):
        yield parent
        if parent == stop: break


def measured_rank_paths(root):
    return sorted({p for run in measured_runs(root) for p in run.glob('option-a-rank-*.json')})


def separate_canary_evidence(root, used):
    """Index original preparation receipts; do not apply benchmark expectations."""
    names=('direct-gpu-rdma.json','direct-adapter-smoke.json','guard-ready-validation.json',
           'guard-ready-rdma.json','rdma-verification.json')
    paths={p for name in names for p in Path(root).glob('**/phases/prepare/logs/'+name)}
    rows=[]
    for path in sorted(paths):
        used.add(path)
        raw=path.read_bytes()
        try:
            proof=json.loads(raw)
            if not isinstance(proof,dict):raise ValueError('Canary receipt must be a JSON object')
            validation=proof.get('capture_validation')
            validation=validation if isinstance(validation,dict) else {}
            rows.append(dict(path=str(path),sha256=hashlib.sha256(raw).hexdigest(),
                original_verified=proof.get('verified'),original_scope=proof.get('scope'),
                original_identities=validation.get('identities'),
                source_archive_sha256=proof.get('source_archive_sha256'),
                benchmark_population=False,
                scope='Original prepare verifier receipt; separate evidence, not revalidated with measured identities/PIDs'))
        except (ValueError,TypeError) as error:
            rows.append(dict(path=str(path),sha256=hashlib.sha256(raw).hexdigest(),
                             original_verified=None,benchmark_population=False,receipt_error=str(error)))
    return rows
