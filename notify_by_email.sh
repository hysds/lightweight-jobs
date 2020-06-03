#!/bin/bash

source ~/.bash_profile

BASE_PATH=$(dirname "${BASH_SOURCE}")

# send email
echo "##########################################" 1>&2
echo -n "Sending email: " 1>&2
date 1>&2
python $BASE_PATH/notify_by_email.py > notify_by_email.log 2>&1
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
