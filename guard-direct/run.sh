#!/usr/bin/env bash
set -euo pipefail
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
exec "${GUARD_DIRECT_PYTHON:-python3}" "$script_dir/scripts/workflow.py" "$@"
