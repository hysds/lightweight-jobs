#!/usr/bin/env python
import sys
import json
import traceback
import backoff
import logging

from datetime import datetime, UTC
from celery import uuid

from hysds.celery import app
from hysds.es_util import get_mozart_es
from hysds.orchestrator import run_job
from hysds.log_utils import log_job_status
from hysds.utils import datetime_iso_naive

from utils import revoke


STATUS_ALIAS = app.conf["STATUS_ALIAS"]
JOB_STATUS_CURRENT = "job_status-current"

LOG_FILE_NAME = 'retry.log'
log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, filename=LOG_FILE_NAME, filemode='a', level=logging.INFO)
logger = logging

mozart_es = get_mozart_es()


def read_context():
    with open('_context.json') as f:
        cxt = json.load(f)
        return cxt


@backoff.on_exception(backoff.expo, Exception, max_tries=10, max_value=64)
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
    return mozart_es.search(index=JOB_STATUS_CURRENT, body=query_json)


@backoff.on_exception(backoff.expo, Exception, max_tries=10, max_value=64)
def delete_by_id(index, _id):
    results = mozart_es.search_by_id(index=index, id=_id, return_all=True)
    for result in results:
        logger.info(f"Deleting job {result['_id']} in {result['_index']}")
        mozart_es.delete_by_id(index=result['_index'], id=result['_id'])


def get_new_job_priority(old_priority, increment_by, new_priority):
    if increment_by is not None:
        priority = int(old_priority) + int(increment_by)
        if priority == 0 or priority == 9:
            logger.info("Not applying {} on previous priority of {}")
            logger.info(f"Priority must be between 0 and 8")
            priority = int(old_priority)
    else:
        priority = int(new_priority)
    return priority


def resubmit_jobs(context):
    """
    logic to resubmit the job
    :param context: contents from _context.json
    """

    # iterate through job ids and query to get the job json
    increment_by = None
    new_priority = None
    if "job_priority_increment" in context:
        increment_by = context["job_priority_increment"]
    else:
        new_priority = context["new_job_priority"]

    retry_count_max = context['retry_count_max']

    if isinstance(context['retry_job_id'], list):
        retry_job_ids = context['retry_job_id']
    else:
        retry_job_ids = [context['retry_job_id']]

    for job_id in retry_job_ids:
        logger.info(f"Validating retry job: {job_id}")
        try:
            doc = query_es(job_id)
            if doc['hits']['total']['value'] == 0:
                logger.warning('job id %s not found in Elasticsearch. Continuing.' % job_id)
                continue
            doc = doc["hits"]["hits"][0]

            job_json = doc["_source"]["job"]
            task_id = doc["_source"]["uuid"]
            index = doc["_index"]
            _id = doc["_id"]

            if not index.startswith("job"):
                logger.error("Cannot retry a worker: %s" % _id)
                continue

            # don't retry a retry
            if job_json['type'].startswith('job-lw-mozart-retry'):
                logger.error("Cannot retry retry job %s. Skipping" % job_id)
                continue

            # check retry_remaining_count
            if 'retry_count' in job_json:
                if job_json['retry_count'] < retry_count_max:
                    job_json['retry_count'] = int(job_json['retry_count']) + 1
                else:
                    logger.error("For job {}, retry_count now is {}, retry_count_max limit of {} reached. Cannot retry again."
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
            job_json['job_info']['time_queued'] = datetime_iso_naive() + 'Z'

            # reset priority
            old_priority = job_json['priority']
            job_json['priority'] = get_new_job_priority(old_priority=old_priority, increment_by=increment_by,
                                                        new_priority=new_priority)

            # get state
            task = app.AsyncResult(task_id)
            state = task.state

            # revoke
            job_id = job_json['job_id']
            try:
                revoke(task_id, state)
                logger.info("revoked original job: {} ({}) state={}".format(job_id, task_id, state))
            except:
                logger.error("Got error issuing revoke on job {} ({}): {}".format(job_id, task_id, traceback.format_exc()))
                logger.error("Continuing.")

            # generate celery task id
            new_task_id = uuid()
            job_json['task_id'] = new_task_id

            # delete old job status; we should pass in the job_status-current alias
            # instead so that we make sure to properly handle the scenario where
            # figaro rules are in place to auto retry jobs that fail due to spot termination.
            # This may potentially cause duplicate records across the job_status
            # and job_failed indices
            delete_by_id(JOB_STATUS_CURRENT, _id)

            # check if new queues, soft time limit, and time limit values were set
            new_job_queue = context.get("job_queue", "")
            if new_job_queue:
                logger.info(f"new job queue specified. Sending retry job to {new_job_queue}")
                job_json['job_info']['job_queue'] = new_job_queue

            new_soft_time_limit = context.get("soft_time_limit", "")
            if new_soft_time_limit:
                logger.info(f"new soft time limit specified. Setting new soft time limit to {int(new_soft_time_limit)}")
                job_json['job_info']['soft_time_limit'] = int(new_soft_time_limit)

            new_time_limit = context.get("time_limit", "")
            if new_time_limit:
                logger.info(f"new time limit specified. Setting new time limit to {int(new_time_limit)}")
                job_json['job_info']['time_limit'] = int(new_time_limit)

            # Before re-queueing, check to see if the job was under the job_failed index. If so, need to
            # move it back to job_status
            if index.startswith("job_failed"):
                current_time = datetime.now(UTC)
                job_json['job_info']['index'] = f"job_status-{current_time.strftime('%Y.%m.%d')}"

            # log queued status
            job_status_json = {
                'uuid': new_task_id,
                'job_id': job_id,
                'payload_id': job_json['job_info']['job_payload']['payload_task_id'],
                'status': 'job-queued',
                'job': job_json
            }
            log_job_status(job_status_json)

            # submit job
            run_job.apply_async((job_json,), queue=job_json['job_info']['job_queue'],
                                time_limit=job_json['job_info']['time_limit'],
                                soft_time_limit=job_json['job_info']['soft_time_limit'],
                                priority=job_json['priority'],
                                task_id=new_task_id)
            logger.info(f"re-submitted job_id={job_id}, payload_id={job_status_json['payload_id']}, task_id={new_task_id}")
        except Exception as ex:
            logger.error(f"[ERROR] Exception occurred {type(ex)}:{ex} {traceback.format_exc()}")


if __name__ == "__main__":
    ctx = read_context()
    # input_type = ctx['resource']
    # if input_type == "job":
    resubmit_jobs(ctx)
    # else:
    #     logger.info("Cannot retry a task, worker or event.")
