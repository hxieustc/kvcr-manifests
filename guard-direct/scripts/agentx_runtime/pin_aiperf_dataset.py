"""Pin the public HF loader's existing revision hook, preserving reconstruction."""
import argparse
import hashlib
import json
from pathlib import Path
import re

MARKER = '    tag: ClassVar[str] = "SemiAnalysisCCTracesWeka"\n'


def render(text, revision, source_sha256):
    if not re.fullmatch('[0-9a-f]{40}', revision):
        raise ValueError('Invalid immutable dataset revision')
    addition = f'    hf_revision = "{revision}"\n'
    original = text.replace(addition, '')
    if hashlib.sha256(original.encode()).hexdigest() != source_sha256 or original.count(MARKER) != 1:
        raise ValueError('Unrecognized pinned AIPerf loader source')
    return original.replace(MARKER, MARKER + addition)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--original', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    from aiperf.dataset.loader import semianalysis_cc_traces_weka
    config = json.loads(args.config.read_text())
    path = Path(semianalysis_cc_traces_weka.__file__)
    source_sha = hashlib.sha256(args.original.read_bytes()).hexdigest()
    updated = render(path.read_text(), config['dataset_revision'], source_sha)
    path.write_text(updated)
    result = dict(path=str(path), source_sha256=source_sha,
                  patched_sha256=hashlib.sha256(updated.encode()).hexdigest(),
                  revision=config['dataset_revision'], scope='Public loader revision only; reconstruction unchanged')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result))


if __name__ == '__main__':
    main()
