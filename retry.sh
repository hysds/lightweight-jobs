#!/bin/bash

source ~/.bash_profile

BASE_PATH=$(dirname "${BASH_SOURCE}")
export PYTHONDONTWRITEBYTECODE=1

# retry job
echo "##########################################" 1>&2
echo -n "Retrying job: " 1>&2
date 1>&2
python $BASE_PATH/retry.py > retry.log 2>&1
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
