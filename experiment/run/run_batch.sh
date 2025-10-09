#!/bin/bash

clients=(1024 512 256 128 64 32 16 8 4 2 1)

PROTOCOL="rcp"
K=2

RUN_T=30
FAILURE_T=5
FAILURE="None"

# For non-geodistributed
# T_CONSENSUS=300
# T_ELECTION_MIN=150
# T_ELECTION_MAX=300
# T_HEARTBEAT=50
# T_BATCH=4

# For availability zone geodistribution
# T_CONSENSUS=600
# T_ELECTION_MIN=300
# T_ELECTION_MAX=600
# T_HEARTBEAT=100
# T_BATCH=8

# For region geodistribution
T_CONSENSUS=2400
T_ELECTION_MIN=1200
T_ELECTION_MAX=2400
T_HEARTBEAT=300
T_BATCH=30

for client in "${clients[@]}"; do
    echo "Running for client = $client. Waiting for 5 seconds in case of early termination is wanted..."
    sleep 5

    # Strategy A
    batch_low=$(( client ))
    batch_high=$(( client ))

    yes "" | bash run.sh \
        --protocol $PROTOCOL \
        --K $K \
        --batch-low $batch_low \
        --batch-high $batch_high \
        --client $client \
        --backoff-dec $batch_high \
        --ct $T_CONSENSUS \
        --et-min $T_ELECTION_MIN \
        --et-max $T_ELECTION_MAX \
        --bt $T_BATCH \
        --ht $T_HEARTBEAT \
        --time $RUN_T \
        --failure-time $FAILURE_T \
        --failure $FAILURE

    # Strategy B
    batch_low=$(( client ))
    batch_high=$(( client * 2 ))

    yes "" | bash run.sh \
        --protocol $PROTOCOL \
        --K $K \
        --batch-low $batch_low \
        --batch-high $batch_high \
        --client $client \
        --backoff-dec $batch_high \
        --ct $T_CONSENSUS \
        --et-min $T_ELECTION_MIN \
        --et-max $T_ELECTION_MAX \
        --bt $T_BATCH \
        --ht $T_HEARTBEAT \
        --time $RUN_T \
        --failure-time $FAILURE_T \
        --failure $FAILURE

    # Skip strategy C and D if client is 1
    if [ "$client" -eq 1 ]; then
        continue
    fi

    # Strategy C
    batch_low=$(( client / 2 ))
    batch_high=$(( client ))

    yes "" | bash run.sh \
        --protocol $PROTOCOL \
        --K $K \
        --batch-low $batch_low \
        --batch-high $batch_high \
        --client $client \
        --backoff-dec $batch_high \
        --ct $T_CONSENSUS \
        --et-min $T_ELECTION_MIN \
        --et-max $T_ELECTION_MAX \
        --bt $T_BATCH \
        --ht $T_HEARTBEAT \
        --time $RUN_T \
        --failure-time $FAILURE_T \
        --failure $FAILURE

    # Strategy D
    batch_low=$(( client / 2 ))
    batch_high=$(( client * 2 ))

    yes "" | bash run.sh \
        --protocol $PROTOCOL \
        --K $K \
        --batch-low $batch_low \
        --batch-high $batch_high \
        --client $client \
        --backoff-dec $batch_high \
        --ct $T_CONSENSUS \
        --et-min $T_ELECTION_MIN \
        --et-max $T_ELECTION_MAX \
        --bt $T_BATCH \
        --ht $T_HEARTBEAT \
        --time $RUN_T \
        --failure-time $FAILURE_T \
        --failure $FAILURE
done
