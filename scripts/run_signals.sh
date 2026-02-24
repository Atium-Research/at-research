#!/usr/bin/env bash
set -e

N=8
for f in signal_research/signals/*.py; do
    echo "Running $f..."
    python "$f" &
    # Wait if we've hit the batch limit
    if (( $(jobs -rp | wc -l) >= N )); then
        wait -n
    fi
done
wait

osascript -e 'display notification "All signals finished running." with title "run_signals.sh"'
