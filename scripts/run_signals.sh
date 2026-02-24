#!/usr/bin/env bash
set -e

for f in signal_research/signals/*.py; do
    echo "Running $f..."
    python "$f"
    echo ""
done

osascript -e 'display notification "All signals finished running." with title "run_signals.sh"'
