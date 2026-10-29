#!/usr/bin/env python3
"""Verify explicit compact capture and apply only reviewed startup budgets."""
import ast
import hashlib
import json
import os
from pathlib import Path
import sys


def digest(data):
    return hashlib.sha256(data).hexdigest()


def startup_overlay_source(source, filename):
    if filename not in ('guard.py', 'guard_protocol.py'):
        return source
    tree = ast.parse(source)
    owner, name = ('_Guard', '_prepare_core') if filename == 'guard.py' else ('KVCRClient', 'claim')
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == owner)
    fn = next(n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == name)
    if filename == 'guard.py':
        calls = [n for n in ast.walk(fn) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == 'KVCRConfig']
        if len(calls) != 1 or any(k.arg == 'startup_timeout_seconds' for k in calls[0].keywords):
            raise ValueError('Unexpected Guard startup seam')
        calls[0].keywords.append(ast.keyword(arg='startup_timeout_seconds', value=ast.Constant(900)))
    else:
        sites = [i for i, n in enumerate(fn.body) if isinstance(n, ast.Assign)
                 and ast.unparse(n) == 'connection = FramedConnection.connect(self._socket_path)']
        if len(sites) != 1:
            raise ValueError('Unexpected Guard claim seam')
        fn.body[sites[0] + 1:sites[0] + 1] = ast.parse('connection._connection.settimeout(1200.0)').body
    ast.fix_missing_locations(tree)
    result = ast.unparse(tree) + '\n'
    compile(result, filename, 'exec')
    return result


def package_hashes(package):
    return {str(path.relative_to(package)):digest(path.read_bytes()) for path in sorted(package.rglob('*.py'))}


def main():
    import kvcr
    bundle = Path(os.environ['BUNDLE_DIR'])
    package = Path(kvcr.__file__).parent
    contract = json.loads((bundle / 'config/source-contract.json').read_text())
    original = contract['package_sha256']
    if package_hashes(package) != original:raise ValueError('Installed recursive KVCR source contract differs')
    staged = {}
    for name, expected in original.items():
        data = (package / name).read_bytes()
        if digest(data) != expected:
            raise ValueError('Installed KVCR source drift: ' + name)
        if name in ('guard.py', 'guard_protocol.py'):
            staged[name] = startup_overlay_source(data.decode(), name).encode()
    helper = package / 'compact_telemetry.py'
    if helper.name not in original:
        raise ValueError('Compact capture absent from source contract')
    for name, data in staged.items():
        (package / name).write_bytes(data)
    proof = dict(source_archive_sha256=contract['source_archive_sha256'],
        custom_patch_sha256=contract['custom_patch_sha256'], source_kind='upstream_plus_custom_patch',
        source_sha256=original, patched_sha256={n: digest(v) for n, v in staged.items()},
        helper_file=helper.name, helper_sha256=digest(helper.read_bytes()),
        compact_capture_version=1, full_ast_observer_enabled=False,
        guard_only_sources_required=True,
        startup_overlays=dict(guard_core_startup_timeout_seconds=900,
                             claim_socket_timeout_seconds=1200, operation_timeouts_changed=False))
    (package / '_b200_telemetry_manifest.json').write_text(json.dumps(proof, indent=2) + '\n')
    Path(sys.argv[1]).write_text(json.dumps(proof, indent=2) + '\n')


if __name__ == '__main__':
    main()
