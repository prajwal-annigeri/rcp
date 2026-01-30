#!/bin/bash

protocols=("rcp" "fraft")
ks=(1 2 3 4 5)
failures=("None" "RF" "LF")

T_CONSENSUS=300
T_ELECTION_MIN=150
T_ELECTION_MAX=300
T_HEARTBEAT=50
BATCH_SIZE=512
CLIENT=512

FAILURE_T=5
OSCILLATING_FAILURE_T=1
RUN_T=55

for protocol in "${protocols[@]}"; do
    for failure in "${failures[@]}"; do
        echo "Running for protocol=$protocol, failure=$failure. Waiting for 5 seconds in case of early termination is wanted..."
        sleep 5

        for k in "${ks[@]}"; do
            yes "" | bash run.sh \
                --protocol $protocol \
                --K $k \
                --batch-size $BATCH_SIZE \
                --client $CLIENT \
                --backoff-dec $BATCH_SIZE \
                --ct $T_CONSENSUS \
                --et-min $T_ELECTION_MIN \
                --et-max $T_ELECTION_MAX \
                --ht $T_HEARTBEAT \
                --time $RUN_T \
                --failure-time $FAILURE_T \
                --failure $failure
        done
    done
done
