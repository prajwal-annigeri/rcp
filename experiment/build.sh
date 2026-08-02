#!/bin/bash

USER="ec2-user"
GOFILE="main.go"
KEY="key"
APP_EXEC_NAME="app"
FAILURE_CLIENT_EXEC_NAME="failure-client"
RECONFIG_CLIENT_EXEC_NAME="reconfig-client"

IP=$1
CODE_ZIP_FILE_NAME=$2
CODE_FOLDER_NAME="${2%.*}"

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 <public_ip> <code_zip_file_name>"
  exit 1
fi

# Copy zip to EC2
echo "Copying zip $CODE_ZIP_FILE_NAME"
scp -i "$KEY" -o StrictHostKeyChecking=no "$CODE_ZIP_FILE_NAME" $USER@$IP:~

# SSH: install Go, unzip, build
echo "Building"
ssh -i "$KEY" -o StrictHostKeyChecking=no $USER@$IP << EOF
  if ! command -v go &> /dev/null; then
    echo "Go not found, installing..."
    if [ -f /etc/debian_version ]; then
      sudo apt update && sudo apt install -y golang unzip make
    else
      sudo yum install -y golang unzip make
    fi
  fi
  unzip -o ~/$CODE_ZIP_FILE_NAME
  cd ~/$CODE_FOLDER_NAME
  go build -o ~/$APP_EXEC_NAME $GOFILE
  cd failure_client
  go build -o ~/$FAILURE_CLIENT_EXEC_NAME
  cd ../reconfig_client
  go build -o ~/$RECONFIG_CLIENT_EXEC_NAME
  cd ../go-ycsb-rcp
  make
  mv bin/go-ycsb ~/
EOF

# Copy executable back
echo "Copying executable"
scp -i "$KEY" -o StrictHostKeyChecking=no $USER@$IP:~/$APP_EXEC_NAME .
scp -i "$KEY" -o StrictHostKeyChecking=no $USER@$IP:~/$FAILURE_CLIENT_EXEC_NAME .
scp -i "$KEY" -o StrictHostKeyChecking=no $USER@$IP:~/$RECONFIG_CLIENT_EXEC_NAME .
scp -i "$KEY" -o StrictHostKeyChecking=no $USER@$IP:~/go-ycsb .

echo "Done."
