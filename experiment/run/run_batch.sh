#!/bin/bash

pairs=(
  # "1,1,1"
  # "1,2,1"

  # "1,2,2"
  # "2,2,2"
  # "1,4,2"
  # "2,4,2"

  # "2,4,4"
  # "4,4,4"
  # "2,8,4"
  # "4,8,4"

  # "4,8,8"
  # "8,8,8"
  # "4,16,8"
  # "8,16,8"

  # "8,16,16"
  # "16,16,16"
  # "8,32,16"
  # "16,32,16"

  # "16,32,32"
  # "32,32,32"
  # "16,64,32"
  # "32,64,32"

  # "32,64,64"
  # "64,64,64"
  # "32,128,64"
  # "64,128,64"

  # "64,128,128"
  # "128,128,128"
  # "64,256,128"
  # "128,256,128"

  # "128,256,256"
  # "256,256,256"
  # "128,512,256"
  # "256,512,256"

  # "256,512,512"
  # "512,512,512"
  # "256,1024,512"
  # "512,1024,512"

  # "512,1024,1024"
  # "1024,1024,1024"
  # "512,2048,1024"
  # "1024,2048,1024"
)

batch_timeouts=(10)

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

# For availability zone geodistribution
# T_CONSENSUS=600
# T_ELECTION_MIN=300
# T_ELECTION_MAX=600
# T_HEARTBEAT=100

# For region geodistribution
T_CONSENSUS=2400
T_ELECTION_MIN=1200
T_ELECTION_MAX=2400
T_HEARTBEAT=300

for pair in "${pairs[@]}"; do
    IFS=',' read -r batch_low batch_high client <<< "$pair"
    
    echo "Running for batch size = $batch_low-$batch_high, client = $client. Waiting for 5 seconds in case of early termination is wanted..."
    sleep 5

    for batch_timeout in "${batch_timeouts[@]}"; do
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
            --bt $batch_timeout \
            --ht $T_HEARTBEAT \
            --time $RUN_T \
            --failure-time $FAILURE_T \
            --failure $FAILURE
    done
done
