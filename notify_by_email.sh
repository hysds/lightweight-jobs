#!/bin/bash

source $HOME/verdi/bin/activate

BASE_PATH=$(dirname "${BASH_SOURCE}")

# check args
if [ "$#" -eq 4 ]; then
  id=$1
  url=$2
  emails=$3
  rule_name=$4
  component=$5
else
  echo "Invalid number or arguments ($#) $*" 1>&2
  exit 1
fi

# send email
echo "##########################################" 1>&2
echo -n "Sending email: " 1>&2
date 1>&2
python $BASE_PATH/notify_by_email.py "$id" "$url" "$emails" "$rule_name" "$component" > notify_by_email.log 2>&1
STATUS=$?
echo -n "Finished sending email: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to send email." 1>&2
  cat notify_by_email.log 1>&2
  echo "{}"
  exit $STATUS
fi

exit 0
