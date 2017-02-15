#!/bin/bash

source $HOME/verdi/bin/activate

BASE_PATH=$(dirname "${BASH_SOURCE}")

# check args
if [ "$#" -eq 8 ]; then
  AOI_name=$1
  label=$2
  coordinates=$3
  priority=$4
  version=$5
  event_time=$6
  days_back=$7
  days_fwd=$8
else
  echo "Invalid number or arguments ($#) $*" 1>&2
  exit 1
fi

# create AOI action
echo "##########################################" 1>&2
echo -n "create AOI job: " 1>&2
date 1>&2
python $BASE_PATH/create_AOI/create_AOI.py "$AOI_name" "$label" "$coordinates" "$priority" "$version" "$event_time" "$days_back" "$days_fwd" > create-AOI.log 2>&1
STATUS=$?

echo -n "Finished creating AOI: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to create AOI." 1>&2
  cat create-AOI.log 1>&2
  echo "{}"
  exit $STATUS
fi

exit 0
