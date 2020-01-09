#!/bin/bash

source ~/.bash_profile

BASE_PATH=$(dirname "${BASH_SOURCE}")

# check args
if [ "$#" -eq 3 ]; then
  query=$1
  emails=$2
  rule_name=$3
  
else
  echo "Invalid number or arguments ($#) $*" 1>&2
  exit 1
fi

# generate AWS get script and email
echo "##########################################" 1>&2
echo -n "Generating AWS get script: " 1>&2
date 1>&2
python $BASE_PATH/aws_get.py "$query" "$emails" "$rule_name" > aws_get.log 2>&1
STATUS=$?
echo -n "Finished with AWS get script: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to send AWS get script." 1>&2
  cat aws_get.log 1>&2
  exit $STATUS
fi
