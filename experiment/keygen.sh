#!/bin/bash

FILE_NAME="key"

ssh-keygen -t rsa -b 2048 -f $FILE_NAME
ssh-keygen -p -m PEM -f $FILE_NAME
