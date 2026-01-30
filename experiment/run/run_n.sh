#!/bin/bash

failures=("None")
protocols=("rcp" "raft" "fraft")

K=2

T_CONSENSUS=300
T_ELECTION_MIN=150
T_ELECTION_MAX=300
T_HEARTBEAT=50
BATCH_LOW=256
BATCH_HIGH=512
CLIENT=512

FAILURE_T=5
OSCILLATING_FAILURE_T=1
RUN_T=60

for protocol in "${protocols[@]}"; do
    echo "Running for protocol=$protocol. Waiting for 5 seconds in case of early termination is wanted..."
    sleep 5

    for failure in "${failures[@]}"; do
        if [[ "$failure" == "LOF" || "$failure" == "ROF" ]]; then
            yes "" | bash run.sh \
                --protocol $protocol \
                --K $K \
                --batch-low $BATCH_LOW \
                --batch-high $BATCH_HIGH \
                --client $CLIENT \
                --backoff-dec $BATCH_HIGH \
                --ct $T_CONSENSUS \
                --et-min $T_ELECTION_MIN \
                --et-max $T_ELECTION_MAX \
                --ht $T_HEARTBEAT \
                --time $RUN_T \
                --failure-time $OSCILLATING_FAILURE_T \
                --failure $failure

        else
            yes "" | bash run.sh \
                --protocol $protocol \
                --K $K \
                --batch-low $BATCH_LOW \
                --batch-high $BATCH_HIGH \
                --client $CLIENT \
                --backoff-dec $BATCH_HIGH \
                --ct $T_CONSENSUS \
                --et-min $T_ELECTION_MIN \
                --et-max $T_ELECTION_MAX \
                --ht $T_HEARTBEAT \
                --time $RUN_T \
                --failure-time $FAILURE_T \
                --failure $failure

        fi
    done
done
