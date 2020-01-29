#!/usr/bin/env python
import sys
import json
import time
import traceback
from random import randint, uniform
from datetime import datetime
from celery import uuid
from hysds.celery import app
from hysds.orchestrator import run_job
from hysds.log_utils import log_job_status

from hysds_commons.elasticsearch_utils import ElasticsearchUtility

JOBS_ES_URL = app.conf["JOBS_ES_URL"]
JOB_INDEX = app.conf["JOB_INDEX"]
es = ElasticsearchUtility(JOBS_ES_URL)


def query_es(job_id):
    query_json = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"job.job_info.id": job_id}}
                ]
            }
        }
    }
    doc = es.search(JOB_INDEX, query_json)
    return doc


def rand_sleep(sleep_min=0.1, sleep_max=1): time.sleep(
    uniform(sleep_min, sleep_max))


def get_new_job_priority(old_priority, increment_by, new_priority):
    if increment_by is not None:
        priority = int(old_priority) + int(increment_by)
        if priority == 0 or priority == 9:
            print("Not applying {} on previous priority of {}")
            print("Priority must be between 0 and 8".format(increment_by, old_priority))
            priority = int(old_priority)
    else:
        priority = int(new_priority)
    return priority


def resubmit_jobs():
    # random sleep to prevent from getting ElasticSearch errors:
    # 429 Client Error: Too Many Requests
    time.sleep(randint(1, 5))
    # can call submit_job

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

    if isinstance(ctx['retry_job_id'], list):
        retry_job_ids = ctx['retry_job_id']
    else:
        retry_job_ids = [ctx['retry_job_id']]

    for job_id in retry_job_ids:
        print(("Retrying job: {}".format(job_id)))
        try:
            # get job json for ES
            rand_sleep()
            doc = query_es(job_id)
            job_json = doc["hits"]["hits"][0]["_source"]["job"]

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
            for i in ('duration', 'execute_node', 'facts', 'job_dir', 'job_url', 'metrics', 'pid', 'public_ip',
                      'status', 'stderr', 'stdout', 'time_end', 'time_queued', 'time_start'):
                if i in job_json.get('job_info', {}):
                    del job_json['job_info'][i]

            # set queue time
            job_json['job_info']['time_queued'] = datetime.utcnow().isoformat() + 'Z'

            # reset priority
            old_priority = job_json['priority']
            job_json['priority'] = get_new_job_priority(old_priority=old_priority, increment_by=increment_by,
                                                        new_priority=new_priority)

            # revoke original job
            rand_sleep()

            job_id = job_json['job_id']
            try:
                app.control.revoke(job_id, terminate=True)
                print("revoked original job: %s" % job_id)
            except:
                print("Got error issuing revoke on job %s: %s" % (job_id, traceback.format_exc()))
                print("Continuing.")

            # generate celery task id
            task_id = uuid()
            job_json['task_id'] = task_id

            # delete old job status
            rand_sleep()
            es.delete_by_id(JOB_INDEX, job_id)

            # log queued status
            rand_sleep()
            job_status_json = {'uuid': task_id,
                               'job_id': job_id,
                               'payload_id': job_json['job_info']['job_payload']['payload_task_id'],
                               'status': 'job-queued',
                               'job': job_json}
            log_job_status(job_status_json)

            # submit job
            queue = job_json['job_info']['job_queue']
            run_job.apply_async((job_json,), queue=queue,
                                time_limit=job_json['job_info']['time_limit'],
                                soft_time_limit=job_json['job_info']['soft_time_limit'],
                                priority=job_json['priority'],
                                task_id=task_id)
        except Exception as ex:
            print("[ERROR] Exception occurred {0}:{1} {2}".format(type(ex), ex, traceback.format_exc()),
                  file=sys.stderr)


if __name__ == "__main__":
    input_type = sys.argv[1]
    if input_type != "worker":
        resubmit_jobs()
    else:
        print("Cannot retry a worker.")
