"""Shared workload contract; only the connector and RNIC profile vary by case."""
from pathlib import Path

CASES = ('mooncake', 'kvcr-single', 'kvcr-dual')


def validate_config(config):
    if config['dataset_entries'] != 393:
        raise ValueError('Measured AgentX must retain all 393 dataset entries')
    if config.get('agentic_cache_warmup_duration') is not None:
        raise ValueError('Duration warmup conflicts with deterministic per-lane warmup')
    if config['duration_seconds'] < 900:
        raise ValueError('AgentX scenario requires at least 900 measured seconds')
    if config.get('executed_cases') != ['kvcr-single']:
        raise ValueError('This guide executes only single-RNIC direct Guard')


def common_environment(config):
    validate_config(config)
    return {
        'AIPERF_HTTP_X_DYNAMO_SESSION_ID_FROM_CORRELATION_ID': '1',
        'AIPERF_HTTP_X_SESSION_ID_FROM_CORRELATION_ID': '1',
        'AIPERF_USE_DYNAMO_CONV_AWARE_ROUTING': '0',
        'AIPERF_DYNAMO_SESSION_TIMEOUT_SECONDS': str(config['session_timeout_seconds']),
        'AIPERF_SESSION_HEADER': 'X-Dynamo-Session-ID',
        'AIPERF_REQUIRED_SERVER_METRIC_PREFIX': 'vllm:',
        'AIPERF_RANDOM_SEED': str(config['random_seed']),
        'AIPERF_DATASET_WEKA_PARALLEL_WORKERS': str(config['weka_parallel_workers']),
        'DYNAMO_ROUTER_MODE': 'kv',
        'DYNAMO_ROUTER_SESSION_AFFINITY_TTL_SECONDS': str(config['router_session_affinity_ttl_seconds']),
        'MODEL_PATH': config['model_path'],
        'NIXL_UCX_SGL_ENABLE': '1',
        'KVCR_NVTX_LEVEL': 'off',
        'UCX_TLS': 'rc,cuda_copy',
        'UCX_PROTO_ENABLE': 'y',
        'KVCR_UCX_HOST_REGISTRATION': '',
        'PYTHONHASHSEED': str(config['random_seed']),
    }


def case_environment(config, case):
    if case not in CASES:
        raise ValueError(f'Unknown case: {case}')
    env = common_environment(config)
    devices = config['dual_rnics'] if case == 'kvcr-dual' else [config['single_rnic']]
    env.update(UCX_NET_DEVICES=','.join(devices), UCX_MAX_RMA_RAILS=str(len(devices)))
    return env


def replay_command(config, assets, output):
    validate_config(config)
    args = [str(Path(assets) / 'aiperf-env/bin/aiperf'), 'profile',
        '--scenario', config['scenario'], '--public-dataset', config['dataset_loader'],
        '--num-dataset-entries', str(config['dataset_entries']),
        '--url', 'http://127.0.0.1:8888', '--endpoint', '/v1/chat/completions',
        '--endpoint-type', 'chat', '--streaming', '--model', config['model'],
        '--tokenizer', config['tokenizer_repository'],
        '--tokenizer-revision', config['tokenizer_revision'], '--tokenizer-trust-remote-code',
        '--concurrency', str(config['concurrency']),
        '--benchmark-duration', str(config['duration_seconds']),
        '--benchmark-grace-period', str(config['grace_seconds']),
        '--random-seed', str(config['random_seed']), '--failed-request-threshold', '0.10',
        '--trajectory-start-min-ratio', str(config['trajectory_start_min_ratio']),
        '--trajectory-start-max-ratio', str(config['trajectory_start_max_ratio']),
        '--warmup-requests-per-lane', str(config['warmup_requests_per_lane']),
        '--warmup-grace-period', str(config['warmup_grace_seconds']),
        '--trace-idle-gap-cap-seconds', str(config['trace_idle_gap_cap_seconds']),
        '--system-idle-gap-cap-seconds', '10', '--cache-bust', 'first_turn_prefix',
        '--session-header', 'X-Dynamo-Session-ID', '--use-server-token-count',
        '--extra-inputs', 'ignore_eos:true', '--no-gpu-telemetry',
        '--slice-duration', '1.0', '--stats-interval', '30',
        '--server-metrics', 'http://127.0.0.1:9090/metrics',
        '--output-artifact-dir', str(output)]
    return args
