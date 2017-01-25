#!/bin/bash

source $HOME/verdi/bin/activate

BASE_PATH=$(dirname "${BASH_SOURCE}")

# check args
if [ "$#" -eq 2 ]; then
  query=$1
  component=$2
else
  echo "Invalid number or arguments ($#) $*" 1>&2
  exit 1
fi

# purge products
echo "##########################################" 1>&2
echo -n "Purging products: " 1>&2
date 1>&2
python $BASE_PATH/purge.py "$query" "$component" > purge.log 2>&1
STATUS=$?
echo -n "Finished purging products: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to purge products." 1>&2
  cat purge.log 1>&2
  echo "{}"
  exit $STATUS
fi

exit 0
