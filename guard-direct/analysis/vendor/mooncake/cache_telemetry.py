"""Mooncake telemetry integration, without native calls or cache policy changes.

Set RUN_DIR and put this directory on PYTHONPATH. B200_MOONCAKE_OBSERVE=1
activates Store instrumentation via sitecustomize; diagnostics additionally use
B200_MOONCAKE_DIAGNOSTICS=1. This standalone writer is the explicit fallback
until a shared telemetry writer exists. Events flush every second and at normal
exit. The queue is bounded to 16 MiB per stream. Any I/O/observation failure or
loss invalidates capture, even if a later retry succeeds. Per-PID/session health
is atomically published every second independently of the event sink, under
B200_MOONCAKE_TELEMETRY_HEALTH_DIR (default /tmp/mooncake-telemetry-health).
Sidecars report errors, pending events/bytes, last_flush_ns, max_lag_ns and lost.
Orderly shutdown appends capture_end footers via atexit and a per-emitting-PID
multiprocessing Finalize(exitpriority=10). Signal handlers remain unchanged;
SIGKILL never implies completion.
Call validate_capture(RUN_DIR, expected_pids, health_dir=...) after stop. It
requires healthy closed sidecars and complete event streams for every PID.

Parent scheduler integration: call record_external_prefix(request.request_id,
connector_prefix_cache_queries, connector_prefix_cache_hits,
preempted=request.num_preemptions > 0) at the existing admitted connector-prefix
stats.record boundary. Never call on pending async polls. Alternatively install
install_external_prefix_accounting on each *connector* stats instance, including
instances recreated after stats export. Do not install on GPU prefix stats.
prepare-runtime.sh installs the strict instrument-prefix-tokens.py source patch
for admitted request/prompt/GPU/external counters, gated by the same observation
environment. Do not additionally install the instance hook when using that patch.
Use request_context around known request operations; context does not cross
threads/processes automatically. No scheduler hook is installed by sitecustomize.

Host Store key rates and scheduler external-prefix token rates have different
denominators. GET shares describe successful served bytes, never cache hit rate.
The approved embedded configuration is 140 GiB DRAM Store + 35 GiB staging per
rank; staging is not a host primary lookup cache. This module changes no config.
"""
from collections import Counter, deque
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
import atexit
import json
import os
import sys
from pathlib import Path
import threading
import time
import uuid


_context = ContextVar('mooncake_request_context', default={})
_writers = {}
_writers_lock = threading.Lock()
_health = None
_finalizer = None
_health_lock = threading.Lock()
DEFAULT_HEALTH_DIR = '/tmp/mooncake-telemetry-health'
MAX_PENDING_BYTES = 16 * 1024**2


def _report(message):
    try:
        print(message, file=sys.stderr)
    except Exception:
        # Missing/failed sidecars and footers still fail the post-stop gate.
        pass


class CaptureHealth:
    """One local sidecar per process/session, independent of the event sink."""
    def __init__(self):
        self.pid = os.getpid()
        self.session = uuid.uuid4().hex
        self.run_dir = str(Path(os.environ['RUN_DIR']).resolve()) if os.environ.get('RUN_DIR') else None
        self.path = Path(os.environ.get('B200_MOONCAKE_TELEMETRY_HEALTH_DIR', DEFAULT_HEALTH_DIR)) / f'mooncake-{self.pid}-{self.session}.json'
        self.lock = threading.Lock()
        self.publish_lock = threading.Lock()
        self.close_lock = threading.Lock()
        self.writers = []
        self.errors = Counter()
        self.lost = 0
        self.error = None
        self.closing = self.closed = self.completed = False
        self.stop = threading.Event()
        self.thread = None
        self.publish()

    def start(self):
        with self.lock:
            if self.thread is None and not self.closing:
                self.thread = threading.Thread(target=self._run, daemon=True, name='mooncake-telemetry')
                self.thread.start()

    def record_error(self, stage, error, lost=0):
        with self.lock:
            self.errors[stage] += 1
            self.lost += lost
            self.error = f'{stage}: {type(error).__name__}: {error}'[:512]
        self.publish()

    def snapshot(self):
        with self.lock:
            writers = list(self.writers)
            result = {'schema_version': 1, 'pid': self.pid, 'session': self.session,
                      'run_dir': self.run_dir, 'wall_ns': time.time_ns(),
                      'valid': not self.errors and not self.lost,
                      'errors': sum(self.errors.values()), 'error_counts': dict(self.errors),
                      'error': self.error, 'lost': self.lost, 'dropped': self.lost,
                      'observation_errors': sum(n for key, n in self.errors.items() if key not in ('write', 'health', 'buffer_overflow')),
                      'closed': self.closed, 'completed': self.completed}
        streams = [writer.snapshot() for writer in writers]
        result.update(streams=streams, pending=sum(s['pending'] for s in streams),
                      pending_bytes=sum(s['pending_bytes'] for s in streams),
                      written=sum(s['written'] for s in streams),
                      write_errors=sum(s['write_errors'] for s in streams),
                      last_flush_ns=min((s['last_flush_ns'] for s in streams), default=0),
                      max_lag_ns=max((s['max_lag_ns'] for s in streams), default=0))
        result['unflushed'] = result['pending']
        result['max_lag_seconds'] = result['max_lag_ns'] / 1e9
        return result

    def publish(self):
        with self.publish_lock:
            try:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                temporary = self.path.with_suffix('.partial')
                temporary.write_text(json.dumps(self.snapshot()) + '\n')
                temporary.replace(self.path)
            except Exception as error:
                with self.lock:
                    self.errors['health'] += 1
                    self.error = f'health sidecar: {type(error).__name__}: {error}'[:512]
                _report(f'FATAL Mooncake telemetry health sidecar failed: {error}')

    def flush(self):
        with self.lock:
            writers = list(self.writers)
        success = all([writer.flush() for writer in writers])
        self.publish()
        return success

    def _run(self):
        while not self.stop.wait(1):
            try:
                self.flush()
            except Exception as error:
                self.record_error('flusher', error)

    def close(self):
        with self.close_lock:
            return self._close()

    def _close(self):
        with self.lock:
            already_closed = self.closed
            self.closing = True
        if already_closed:
            return self.snapshot()
        self.stop.set()
        if self.thread is not None:
            self.thread.join(timeout=2)
        if self.thread is not None and self.thread.is_alive():
            self.record_error('shutdown', RuntimeError('flusher did not stop within two seconds'))
        else:
            drained = self.flush()
            if drained:
                status = self.snapshot()
                footers_queued = []
                for writer in list(self.writers):
                    footers_queued.append(writer.emit({'event': 'capture_end', 'valid': status['valid'],
                                 'errors': status['errors'], 'lost': status['lost'],
                                 'events_before_footer': writer.snapshot()['written'],
                                 'pending': 0, 'pending_bytes': 0}, control=True))
                drained = self.flush() and all(footers_queued)
            with self.lock:
                self.completed = bool(self.writers) and drained
        with self.lock:
            self.closed = True
        self.publish()
        return self.snapshot()


class BufferedWriter:
    """Bounded event queue; filesystem I/O never holds the producer lock."""
    def __init__(self, path, start_thread=True, max_pending_bytes=MAX_PENDING_BYTES):
        self.path = Path(path)
        self.pid = os.getpid()
        self.health = capture_health(start_thread=False)
        self._pending = deque()
        self._pending_bytes = 0
        self._lock = threading.Lock()
        self._io_lock = threading.Lock()
        self.max_pending_bytes = max_pending_bytes
        self.sequence = self.written = self.write_errors = self.max_lag_ns = self.last_flush_ns = 0
        with self.health.lock:
            self.health.writers.append(self)
        self.emit({'event': 'capture_start', 'flush_interval_seconds': 1,
                   'max_pending_bytes': max_pending_bytes}, control=True)
        self.health.publish()
        if start_thread:
            self.health.start()

    @property
    def pending_bytes(self):
        with self._lock:
            return self._pending_bytes

    def snapshot(self):
        with self._lock:
            if self._pending:
                self.max_lag_ns = max(self.max_lag_ns, time.monotonic_ns() - self._pending[0][1])
            return {'capture_path': str(self.path.resolve()), 'pending': len(self._pending),
                    'pending_bytes': self._pending_bytes, 'written': self.written,
                    'write_errors': self.write_errors, 'last_flush_ns': self.last_flush_ns,
                    'max_lag_ns': self.max_lag_ns}

    def emit(self, row, control=False):
        try:
            with self._lock:
                payload = (json.dumps({**row, 'wall_ns': time.time_ns(), 'pid': self.pid,
                                      'session': self.health.session, 'sequence': self.sequence + 1},
                                     separators=(',', ':'), default=str) + '\n').encode()
                rejected = (not control and (self.health.closing or self._pending_bytes + len(payload) > self.max_pending_bytes))
                if not rejected:
                    self.sequence += 1
                    self._pending.append((payload, time.monotonic_ns()))
                    self._pending_bytes += len(payload)
            if rejected:
                self.health.record_error('buffer_overflow', RuntimeError('queue full or capture closing'), lost=1)
            return not rejected
        except Exception as error:
            self.health.record_error('serialization', error, lost=1)
            return False

    def flush(self):
        # Keep the exact unwritten suffix after a partial write. Never hold the
        # producer lock while opening/writing/closing a potentially stalled sink.
        with self._io_lock:
            fd = None
            try:
                with self._lock:
                    pending = bool(self._pending)
                if pending:
                    self.path.parent.mkdir(parents=True, exist_ok=True)
                    fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
                while pending:
                    with self._lock:
                        payload, queued_ns = self._pending[0]
                    count = os.write(fd, payload)
                    if count <= 0:
                        raise OSError('Short event write')
                    with self._lock:
                        self._pending_bytes -= count
                        self.max_lag_ns = max(self.max_lag_ns, time.monotonic_ns() - queued_ns)
                        if count == len(payload):
                            self._pending.popleft()
                            self.written += 1
                        else:
                            self._pending[0] = (payload[count:], queued_ns)
                        pending = bool(self._pending)
                if fd is not None:
                    os.close(fd)
                    fd = None
                with self._lock:
                    self.last_flush_ns = time.time_ns()
                return True
            except Exception as error:
                with self._lock:
                    self.write_errors += 1
                self.health.record_error('write', error)
                return False
            finally:
                if fd is not None:
                    try:
                        os.close(fd)
                    except OSError as error:
                        self.health.record_error('write', error)


def capture_health(start_thread=True):
    global _health, _finalizer
    with _health_lock:
        if _health is None:
            _health = CaptureHealth()
            # Normal multiprocessing children bypass atexit via os._exit.
            # Register after capture starts in this PID, never at parent import.
            from multiprocessing.util import Finalize
            _finalizer = Finalize(None, _shutdown, exitpriority=10)
        health = _health
    if start_thread:
        health.start()
    return health


def _after_fork():
    global _writers, _writers_lock, _health, _health_lock, _finalizer
    _writers = {}
    _writers_lock = threading.Lock()
    _health = None
    _finalizer = None
    _health_lock = threading.Lock()


if hasattr(os, 'register_at_fork'):
    os.register_at_fork(after_in_child=_after_fork)


def observation_error(error, stage='observation', lost=1):
    """Invalidate capture without replacing a native result or exception."""
    try:
        capture_health().record_error(stage, error, lost=lost)
    except Exception as health_error:
        _report(f'FATAL Mooncake telemetry observation/health failure: {type(health_error).__name__}')


def emit(row, diagnostics=False):
    try:
        pid = os.getpid()
        directory = Path(os.environ['RUN_DIR']) / ('diagnostics' if diagnostics else 'store-events')
        path = directory / (f'keys-{pid}.jsonl' if diagnostics else f'{pid}.jsonl')
        with _writers_lock:
            writer = _writers.get(path)
            if writer is None:
                writer = _writers[path] = BufferedWriter(path)
        writer.emit(row)
    except Exception as error:
        observation_error(error, stage='emit')


def flush():
    return _health.flush() if _health is not None else True


def finalize_capture():
    """Call in the emitting PID at orderly shutdown, never to bless a dead PID."""
    return _health.close() if _health is not None else {'valid': False, 'completed': False, 'closed': False}


def _shutdown():
    if _health is not None and not _health.closed:
        status = finalize_capture()
        if not status['completed']:
            _report(f'Mooncake telemetry exit flush failed: pid={os.getpid()} pending_bytes={_health.snapshot()["pending_bytes"]}')


atexit.register(_shutdown)


def safe_record(record, row):
    try:
        record(row)
    except Exception as error:
        observation_error(error, stage='record')


def validate_capture(run_dir, expected_pids, *, health_dir=None):
    """Post-stop gate: require health AND contiguous start/end records per PID.

    The parent must supply every expected emitting PID and collect sidecars from
    the same PID namespace. Missing health, footers or records fail closed even
    when the last reported pending count is zero. SIGKILL is never finalized here.
    """
    expected = {int(pid) for pid in expected_pids}
    root = str(Path(run_dir).resolve())
    directory = Path(health_dir or os.environ.get('B200_MOONCAKE_TELEMETRY_HEALTH_DIR', DEFAULT_HEALTH_DIR))
    reasons = []
    seen = set()
    sessions = []
    if not expected:
        reasons.append('no expected PIDs supplied')
    for path in sorted(directory.glob('mooncake-*.json')):
        try:
            if int(path.name.split('-')[1]) not in expected:
                continue
            status = json.loads(path.read_text())
            if status.get('run_dir') != root:
                continue
            pid = status['pid']
            if pid not in expected:
                raise ValueError('sidecar PID mismatch')
            seen.add(pid)
            sessions.append(status['session'])
            if (status.get('schema_version') != 1 or not status.get('valid') or not status.get('closed')
                    or not status.get('completed') or status.get('errors') != 0 or status.get('lost') != 0
                    or status.get('pending') != 0 or status.get('pending_bytes') != 0
                    or status.get('write_errors') != 0 or status.get('observation_errors') != 0
                    or not status.get('last_flush_ns')):
                raise ValueError('unhealthy or incomplete capture sidecar')
            streams = status.get('streams', [])
            if not streams:
                raise ValueError('missing capture streams')
            written = 0
            for stream in streams:
                count = starts = ends = 0
                first = footer = None
                # A full run can contain millions of events: stream validation
                # must not materialize the capture in the controller's memory.
                with Path(stream['capture_path']).open('rb') as capture:
                    for line in capture:
                        if not line.endswith(b'\n'):
                            raise ValueError('truncated capture stream')
                        row = json.loads(line)
                        if row.get('pid') != pid or row.get('session') != status['session']:
                            continue
                        count += 1
                        if row.get('sequence') != count:
                            raise ValueError('noncontiguous event sequence')
                        starts += row.get('event') == 'capture_start'
                        ends += row.get('event') == 'capture_end'
                        if first is None:
                            first = row
                        footer = row
                if (count < 2 or starts != 1 or ends != 1 or first.get('event') != 'capture_start'
                        or footer.get('event') != 'capture_end'):
                    raise ValueError('missing or duplicate footer/start')
                if (not footer.get('valid') or footer.get('errors') != 0 or footer.get('lost') != 0
                        or footer.get('events_before_footer') != count - 1
                        or stream.get('written') != count or not stream.get('last_flush_ns')
                        or stream.get('pending') != 0 or stream.get('pending_bytes') != 0):
                    raise ValueError('invalid footer or written-count mismatch')
                written += count
            if status.get('written') != written:
                raise ValueError('sidecar total written-count mismatch')
        except Exception as error:
            reasons.append(f'{path.name}: {type(error).__name__}: {error}')
    reasons.extend(f'missing health for PID {pid}' for pid in sorted(expected - seen))
    return {'valid': not reasons, 'reasons': reasons, 'expected_pids': sorted(expected), 'sessions': sessions}


def context_fields():
    return dict(_context.get())


@contextmanager
def request_context(**fields):
    token = _context.set({**_context.get(), **fields})
    try:
        yield
    finally:
        _context.reset(token)


def key_query_stats(keys, results):
    values = [] if results is None else results[:len(keys)]
    positive = sum(value > 0 for value in values)
    misses = sum(value == 0 for value in values)
    errors = Counter(str(value) for value in values if value < 0)
    valid = positive + misses
    return {'queried_keys': len(keys), 'positive_keys': positive,
            'valid_checked_keys': valid, 'miss_keys': misses,
            'error_keys': sum(errors.values()), 'unknown_keys': len(keys) - len(values),
            'error_return_codes': dict(errors),
            'host_store_key_hit_rate': positive / valid if valid else None}


def record_external_prefix(request_id, queries, matches, *, preempted=False, record=emit,
                           admitted=False, prompt_tokens=None, request_tokens=None,
                           gpu_cached_tokens=None, external_cached_tokens=None):
    """Record one resolved admission, or an explicitly unresolved observation.

    Queries/matches are scheduler token counts after local-prefix reconciliation,
    not raw Store keys. Repeated admitted/preempted lookups follow vLLM accounting.
    """
    try:
        resolved = queries is not None and matches is not None
        row = {**context_fields(), 'event': 'external_prefix_accounting',
               'resolved': resolved, 'preempted': preempted,
               'external_prefix_token_queries': queries if resolved else 0,
               'external_prefix_token_matches': matches if resolved else 0}
        if admitted and resolved:
            row.update(admitted_requests=1, prompt_tokens=prompt_tokens, request_tokens=request_tokens,
                       gpu_cached_tokens=gpu_cached_tokens, external_cached_tokens=external_cached_tokens)
        if request_id is not None:
            row['request_id'] = request_id
        safe_record(record, row)
    except Exception as error:
        observation_error(error, stage='prefix_accounting')


def install_external_prefix_accounting(stats, record=emit):
    """Wrap this connector-only accounting instance, preserving the native call.

    Must be reapplied when vLLM replaces connector_prefix_cache_stats on export.
    A parent may instead call record_external_prefix directly at admission.
    """
    native = stats.record
    if getattr(native, '_mooncake_telemetry', False):
        return

    @wraps(native)
    def observed(*args, **kwargs):
        result = native(*args, **kwargs)
        try:
            queries = kwargs.get('num_tokens', args[0] if args else None)
            matches = kwargs.get('num_hits', args[1] if len(args) > 1 else None)
            preempted = kwargs.get('preempted', args[2] if len(args) > 2 else False)
            record_external_prefix(None, queries, matches, preempted=preempted, record=record)
        except Exception as error:
            observation_error(error, stage='prefix_stats_hook')
        return result

    observed._mooncake_telemetry = True
    stats.record = observed


def summarize_cache(events):
    """Aggregate denominators, with preempted token counters split as in vLLM."""
    out = dict.fromkeys(['query_calls', 'queried_keys', 'positive_keys', 'valid_checked_keys',
                         'miss_keys', 'error_keys', 'unknown_keys', 'query_exception_calls', 'query_exception_keys',
                         'external_prefix_resolved_lookups', 'external_prefix_unresolved_lookups',
                         'external_prefix_token_queries', 'external_prefix_token_matches',
                         'external_prefix_preempted_lookups', 'external_prefix_preempted_token_queries',
                         'external_prefix_preempted_token_matches'], 0)
    admission_keys = ('admitted_requests', 'prompt_tokens', 'request_tokens',
                      'gpu_cached_tokens', 'external_cached_tokens')
    out.update(dict.fromkeys(admission_keys, 0))
    out.update(dict.fromkeys(('preempted_' + key for key in admission_keys), 0))
    errors = Counter()
    for row in events:
        if row.get('event') == 'store_exists':
            out['query_calls'] += 1
            if row.get('exception_type'):
                out['query_exception_calls'] += 1
                out['query_exception_keys'] += row.get('queried_keys', 0)
            for key in ('queried_keys', 'positive_keys', 'valid_checked_keys', 'miss_keys', 'error_keys', 'unknown_keys'):
                out[key] += row.get(key, 0)
            errors.update(row.get('error_return_codes', {}))
        elif row.get('event') == 'external_prefix_accounting':
            if row.get('resolved'):
                out['external_prefix_resolved_lookups'] += 1
                if row.get('admitted_requests'):
                    prefix = 'preempted_' if row.get('preempted') else ''
                    for key in admission_keys:
                        out[prefix + key] += row[key]
                if row.get('preempted'):
                    out['external_prefix_preempted_lookups'] += 1
                    out['external_prefix_preempted_token_queries'] += row['external_prefix_token_queries']
                    out['external_prefix_preempted_token_matches'] += row['external_prefix_token_matches']
                else:
                    for key in ('external_prefix_token_queries', 'external_prefix_token_matches'):
                        out[key] += row[key]
            else:
                out['external_prefix_unresolved_lookups'] += 1
    out['error_return_codes'] = dict(errors)
    out['host_store_key_hit_rate'] = (out['positive_keys'] / out['valid_checked_keys']
                                      if out['valid_checked_keys'] else None)
    out['external_prefix_token_hit_rate'] = (out['external_prefix_token_matches'] / out['external_prefix_token_queries']
                                            if out['external_prefix_token_queries'] else None)
    return out


def resolve_get(row, endpoint_pids):
    """Postprocess one GET using exact TE endpoint -> PID evidence, never IP.

    The map must come from the same host/PID namespace as the captured events.
    Missing object attribution in older records remains unknown. No remote
    duration or bandwidth is extrapolated from the whole batch API duration.
    """
    byte_counts = Counter(local=0, remote=0, unknown=row.get('unattributed_successful_bytes', 0))
    object_counts = Counter(local=0, remote=0, unknown=row.get('unattributed_successful_objects', 0))
    endpoints = row.get('endpoint_successful_bytes', {})
    objects = row.get('endpoint_successful_objects', {})
    for endpoint in endpoints.keys() | objects.keys():
        owner = endpoint_pids.get(endpoint)
        kind = 'unknown' if owner is None or row.get('pid') is None else ('local' if owner == row['pid'] else 'remote')
        byte_counts[kind] += endpoints.get(endpoint, 0)
        object_counts[kind] += objects.get(endpoint, 0)
    byte_counts['unknown'] += max(0, row.get('successful_bytes', 0) - sum(byte_counts.values()))
    known_objects = sum(object_counts.values())
    expected_objects = row.get('objects', 0) - row.get('failed_objects', 0)
    object_counts['unknown'] += max(0, expected_objects - known_objects)
    total = sum(byte_counts.values())
    if row.get('failed_objects', 0) or row.get('exception_type'):
        classification = 'failed'
    elif not total or byte_counts['unknown'] or object_counts['unknown']:
        classification = 'unknown'
    elif byte_counts['local'] and byte_counts['remote']:
        classification = 'mixed'
    elif byte_counts['remote']:
        classification = 'pure_remote'
    else:
        classification = 'local_only'
    result = {**row, 'get_class': classification, 'api_seconds': row.get('seconds')}
    for kind in ('local', 'remote', 'unknown'):
        result[f'{kind}_successful_bytes'] = byte_counts[kind]
        result[f'{kind}_successful_objects'] = object_counts[kind]
        result[f'{kind}_served_byte_share'] = byte_counts[kind] / total if total else None
    return result
