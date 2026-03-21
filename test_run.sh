#!/bin/bash
echo "=== GakuToolkit TEST MODE ==="
echo $(date "+%Y-%m-%d %H:%M:%S")

# No git submodule update
# No rm of masterdb files
# No git commit/push

python3 test_run.py "$@"
EXIT_CODE=$?

echo "=== TEST MODE COMPLETE (exit code: $EXIT_CODE) ==="
exit $EXIT_CODE
