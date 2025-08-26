#!/bin/bash

# failures=("None" "RF" "LF" "LOF" "ROF")
failures=("None" "LF")
protocols=("rcp" "raft" "fraft")

K=2

# For non-geodistributed
# T_CONSENSUS=300
# T_ELECTION_MIN=150
# T_ELECTION_MAX=300
# T_HEARTBEAT=50
# T_BATCH=4
# BATCH_LOW=256
# BATCH_HIGH=512
# CLIENT=512
# FAILURE_T=5
# OSCILLATING_FAILURE_T=1
# RUN_T=30

# For availability zone geodistribution
# T_CONSENSUS=600
# T_ELECTION_MIN=300
# T_ELECTION_MAX=600
# T_HEARTBEAT=100
# T_BATCH=8
# BATCH_LOW=256
# BATCH_HIGH=512
# CLIENT=512
# FAILURE_T=10
# OSCILLATING_FAILURE_T=2
# RUN_T=60

# For region geodistribution
T_CONSENSUS=2400
T_ELECTION_MIN=1200
T_ELECTION_MAX=2400
T_HEARTBEAT=300
T_BATCH=30
BATCH_LOW=256
BATCH_HIGH=512
CLIENT=512
FAILURE_T=40
OSCILLATING_FAILURE_T=8
RUN_T=240

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
                --bt $T_BATCH \
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
                --bt $T_BATCH \
                --ht $T_HEARTBEAT \
                --time $RUN_T \
                --failure-time $FAILURE_T \
                --failure $failure

        fi
    done
done
