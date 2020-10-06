#!/bin/bash

source ~/.bash_profile

BASE_PATH=$(dirname "${BASH_SOURCE}")
export PYTHONDONTWRITEBYTECODE=1

# check args
if [ "$#" -eq 3 ]; then
  query=$1
  emails=$2
  rule_name=$3
  
else
  echo "Invalid number or arguments ($#) $*" 1>&2
  exit 1
fi

# generate wget scripy and email
echo "##########################################" 1>&2
echo -n "Generating wget script: " 1>&2
date 1>&2
python $BASE_PATH/wget.py "$query" "$emails" "$rule_name" > wget.log 2>&1
STATUS=$?
echo -n "Finished with wget script: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to send wget script." 1>&2
  cat wget.log 1>&2
  exit $STATUS
fi
