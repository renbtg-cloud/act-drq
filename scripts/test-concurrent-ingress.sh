#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)";cd "$ROOT"
./scripts/build.sh
./scripts/test-adapter.sh
./scripts/test-http-facade.sh
./scripts/test-provider-transport.sh        # real socket transport
python3 -m unittest discover -s simulator/tests -v
if [[ "${JAMES_RUN_LIVE_TESTS:-0}" == 1 ]]; then
  export JAMES_TEST_CP="$(./scripts/print-classpath.sh)"
  python3 -u tests/run_end_to_end.py
  if [[ "${JAMES_RUN_LOAD_TEST:-0}" == 1 ]];then
    python3 -u tests/run_concurrent_ingress.py
  fi
fi
