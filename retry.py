#!/usr/bin/env python
import sys
import json
import requests
import traceback
import backoff
from datetime import datetime
from celery import uuid
from hysds.celery import app
from hysds.orchestrator import run_job
from hysds.log_utils import log_job_status

from utils import revoke


JOBS_ES_URL = app.conf["JOBS_ES_URL"]
STATUS_ALIAS = app.conf['STATUS_ALIAS']


@backoff.on_exception(backoff.expo,
                      Exception,
                      max_tries=10,
                      max_value=64)
def query_ES(job_id):
    # get the ES_URL
    es_url = JOBS_ES_URL
    index = STATUS_ALIAS
    query_json = {
        "query": {"bool": {"must": [{"term": {"job.job_info.id": "job_id"}}]}}}
    query_json["query"]["bool"]["must"][0]["term"]["job.job_info.id"] = job_id
    r = requests.post('%s/%s/_search?' %
                      (es_url, index), json.dumps(query_json))
    return r


@backoff.on_exception(backoff.expo,
                      Exception,
                      max_tries=10,
                      max_value=64)
def delete_by_id(es_url, query_idx, job_id):
    r = requests.delete("%s/%s/job/_query?q=_id:%s" %
                        (es_url, query_idx, job_id))
    r.raise_for_status()
    print("deleted original job status: %s" % job_id)


def get_new_job_priority(old_priority, increment_by, new_priority):
    if increment_by is not None:
        priority = int(old_priority) + int(increment_by)
        if priority == 0 or priority == 9:
            print(("Not applying {} on previous priority of {} as it needs to be in range 0 to 8".format(increment_by,
                                                                                                         old_priority)))
            priority = int(old_priority)
    else:
        priority = int(new_priority)
    return priority


def resubmit_jobs():
    es_url = JOBS_ES_URL

    # iterate through job ids and query to get the job json
    with open('_context.json') as f:
        ctx = json.load(f)

    increment_by = None
    new_priority = None
    if "job_priority_increment" in ctx:
        increment_by = ctx["job_priority_increment"]
    else:
        new_priority = ctx["new_job_priority"]

    retry_count_max = ctx['retry_count_max']
    retry_job_ids = ctx['retry_job_id'] if isinstance(
        ctx['retry_job_id'], list) else [ctx['retry_job_id']]
    for job_id in retry_job_ids:
        print(("Retrying job: {}".format(job_id)))
        try:
            # get job json for ES
            response = query_ES(job_id)
            if response.status_code != 200:
                print(("Failed to query ES. Got status code %d:\n%s" % (response.status_code, json.dumps(response.json(),
                                                                                                         indent=2))))
            response.raise_for_status()
            doc = response.json()
            if doc['hits']['total']['value'] == 0:
                print('job id %s not found in Elasticsearch. Continuing.' % job_id)
                continue
            doc = doc["hits"]["hits"][0]
            job_json = doc["_source"]["job"]
            task_id = doc["_source"]["uuid"]
            query_idx = doc["_index"]

            # don't retry a retry
            if job_json['type'].startswith('job-lw-mozart-retry'):
                print("Cannot retry retry job %s. Skipping" % job_id)
                continue

            # check retry_remaining_count
            if 'retry_count' in job_json:
                if job_json['retry_count'] < retry_count_max:
                    job_json['retry_count'] = int(job_json['retry_count']) + 1
                else:
                    print("For job {}, retry_count now is {}, retry_count_max limit of {} reached. Cannot retry again."
                          .format(job_id, job_json['retry_count'], retry_count_max))
                    continue
            else:
                job_json['retry_count'] = 1
            job_json["job_info"]["dedup"] = False
            # clean up job execution info
            for i in ('duration', 'execute_node', 'facts', 'job_dir', 'job_url',
                      'metrics', 'pid', 'public_ip', 'status', 'stderr',
                      'stdout', 'time_end', 'time_queued', 'time_start'):
                if i in job_json.get('job_info', {}):
                    del job_json['job_info'][i]

            # set queue time
            job_json['job_info']['time_queued'] = datetime.utcnow(
            ).isoformat() + 'Z'

            # reset priority
            old_priority = job_json['priority']
            job_json['priority'] = get_new_job_priority(old_priority=old_priority, increment_by=increment_by,
                                                        new_priority=new_priority)

            # get state
            task = app.AsyncResult(task_id)
            state = task.state

            # revoke original job
            job_id = job_json['job_id']
            try:
                revoke(task_id, state)
                print("revoked original job: %s (%s)" % (job_id, task_id))
            except Exception:
                print("Got error issuing revoke on job %s (%s): %s" % (job_id, task_id, traceback.format_exc()))
                print("Continuing.")

            # generate celery task id
            new_task_id = uuid()
            job_json['task_id'] = new_task_id

            # delete old job status
            delete_by_id(es_url, query_idx, job_json['job_id'])

            # log queued status
            job_status_json = {'uuid': new_task_id,
                               'job_id': job_json['job_id'],
                               'payload_id': job_json['job_info']['job_payload']['payload_task_id'],
                               'status': 'job-queued',
                               'job': job_json}
            log_job_status(job_status_json)

            # submit job
            queue = job_json['job_info']['job_queue']
            res = run_job.apply_async((job_json,), queue=queue,
                                      time_limit=job_json['job_info']['time_limit'],
                                      soft_time_limit=job_json['job_info']['soft_time_limit'],
                                      priority=job_json['priority'],
                                      task_id=new_task_id)
        except Exception as ex:
            print("[ERROR] Exception occured {0}:{1} {2}".format(
                type(ex), ex, traceback.format_exc()), file=sys.stderr)


if __name__ == "__main__":
    input_type = sys.argv[1]
    if input_type != "worker":
        resubmit_jobs()
    else:
        print("Cannot retry a worker.")
