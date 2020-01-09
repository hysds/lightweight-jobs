#!/bin/bash

source ~/.bash_profile

BASE_PATH=$(dirname "${BASH_SOURCE}")

# check args
if [ "$#" -eq 3 ]; then
  query=$1
  component=$2
  operation=$3
else
  echo "Invalid number or arguments ($#) $*" 1>&2
  exit 1
fi

# purge products
echo "##########################################" 1>&2
echo -n "Purge/Stop/Revoke products: " 1>&2
date 1>&2
python $BASE_PATH/purge.py "$query" "$component" "$operation" > purge.log 2>&1
STATUS=$?
echo -n "Finished purging/revoking: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to purge/revoke." 1>&2
  cat purge.log 1>&2
  echo "{}"
  exit $STATUS
fi

exit 0
