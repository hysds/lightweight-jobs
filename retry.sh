#!/bin/bash

source $HOME/verdi/bin/activate

BASE_PATH=$(dirname "${BASH_SOURCE}")

if [ "$#" -eq 1 ]; then
  types=$1
else
  echo "Invalid number or arguments ($#) $*" 1>&2
  exit 1
fi
  
# retry job
echo "##########################################" 1>&2
echo -n "Retrying job: " 1>&2
date 1>&2
python $BASE_PATH/retry.py "$types" > retry.log 2>&1
STATUS=$?

echo -n "Finished retrying job: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to retry job." 1>&2
  cat retry.log 1>&2
  echo "{}"
  exit $STATUS
fi

exit 0
