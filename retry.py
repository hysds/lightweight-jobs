#!/usr/bin/env python
import sys
import json
import traceback
import backoff
import logging

from datetime import datetime, timezone
from celery import uuid

from hysds.celery import app
from hysds.es_util import get_mozart_es
from hysds.lock import JobLock
from hysds.orchestrator import run_job
from hysds.log_utils import log_job_status
from hysds.utils import datetime_iso_naive

from utils import revoke, create_info_message_files


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


class JobNotFoundError(Exception):
    """The retry target job id has no status doc in OpenSearch."""


# Two retry budgets: the outer decorator retries a not-found result briefly
# (HC-633: the faster user_rules trigger can fire before the just-written
# status doc is search-visible), while the inner decorator keeps the long
# transport retry so a transient OpenSearch outage surfaces as a retried
# query, not a skipped job. JobNotFoundError is raised as a distinct type so
# only the not-found signal is handled as "job not found" by the caller --
# a ValueError from unrelated code must not be misreported as a missing job.
@backoff.on_exception(backoff.expo, JobNotFoundError, max_tries=4, max_value=4)
@backoff.on_exception(backoff.expo, Exception, max_tries=10, max_value=64,
                      giveup=lambda e: isinstance(e, JobNotFoundError))
def query_es_required(job_id):
    query_json = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"job.job_info.id": job_id}}
                ]
            }
        }
    }
    result = mozart_es.search(index=JOB_STATUS_CURRENT, body=query_json)
    if result['hits']['total']['value'] == 0:
        raise JobNotFoundError(f"job id {job_id} not found in OpenSearch")
    return result


@backoff.on_exception(backoff.expo, Exception, max_tries=10, max_value=64)
def delete_by_id(index, _id):
    # without ignore=[404], search_by_id raises ValueError on 0 hits (e.g. a
    # concurrent retry already deleted the doc), which would churn through
    # this backoff for minutes before failing the retry; an already-absent
    # doc is a no-op here, so ask for the not-found sentinel and skip it
    results = mozart_es.search_by_id(index=index, id=_id, return_all=True, ignore=[404])
    for result in results:
        if result.get("found") is False:
            logger.info(f"No {_id} doc found in {index}; nothing to delete.")
            continue
        logger.info(f"Deleting job {result['_id']} in {result['_index']}")
        mozart_es.delete_by_id(index=result['_index'], id=result['_id'], ignore=[404])


def _wait_for_lock_release(payload_id, task_id, timeout, max_interval):
    """Poll the Redis job lock with exponential backoff until released or timeout is reached.

    Returns "released" if the lock was released while polling, "force_released"
    if the timeout hit with the revoked task confirmed stopped (its stale lock
    is cleared before resubmit), or "held" if the timeout hit while the task
    still appears to be running (the lock is left in place -- force-releasing
    a live task's lock would let the resubmitted job run concurrently with the
    original, the duplicate-execution race the lock exists to prevent; the
    resubmitted job may fail lock acquisition until the task actually stops).
    """

    temp_lock = JobLock(payload_id, task_id="revoke-wait", worker_hostname="mozart")
    outcome = {"result": "held"}

    def _on_giveup(details):
        # revoke() failures are logged-and-continued by the caller, so the
        # original task may still be alive and heartbeating its lock; only
        # clear the lock once celery confirms the task reached a terminal
        # state, otherwise leave it to the resubmitted job's own acquisition
        state = app.AsyncResult(task_id).state
        if state in ("SUCCESS", "FAILURE", "REVOKED"):
            logger.warning(
                f"Lock for payload {payload_id} not released within "
                f"{details['elapsed']:.1f}s but task {task_id} is {state}. "
                f"Force-releasing stale lock before resubmit."
            )
            temp_lock.force_release()
            outcome["result"] = "force_released"
        else:
            logger.error(
                f"Lock for payload {payload_id} not released within "
                f"{details['elapsed']:.1f}s and task {task_id} is still {state}. "
                f"Leaving lock in place to avoid duplicate execution."
            )

    @backoff.on_predicate(
        backoff.expo,
        max_time=timeout,
        max_value=max_interval,
        on_backoff=lambda details: logger.info(
            f"Lock for payload {payload_id} still held by {task_id}, retrying in {details['wait']:.1f}s "
            f"(elapsed: {details['elapsed']:.1f}s)"
        ),
        on_giveup=_on_giveup,
    )
    def _poll():
        try:
            metadata = temp_lock.get_lock_metadata()
            if not metadata or metadata.get("task_id") != task_id:
                return True
        except Exception as e:
            logger.warning(f"Error polling lock metadata for payload {payload_id}: {e}")
        return False

    if _poll():
        logger.info(f"Lock for payload {payload_id} released, proceeding with resubmit")
        return "released"
    return outcome["result"]


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

    not_found_job_ids = []
    errored_job_ids = []
    force_released_locks = []
    locks_still_held = []
    info_msgs = []
    info_msg_details = ""

    heartbeat_interval = app.conf.get("JOB_LOCK_HEARTBEAT_INTERVAL", 30)
    revoke_wait_timeout = heartbeat_interval * app.conf.get("JOB_LOCK_STALE_CHECK_RETRIES", 3) + app.conf.get("JOB_LOCK_REDELIVERY_BUFFER_TIME", 10)
    revoke_wait_max_interval = heartbeat_interval // 2

    for job_id in retry_job_ids:
        logger.info(f"Validating retry job: {job_id}")
        try:
            doc = query_es_required(job_id)
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

            # if the task was actively running, wait for confirmation it has stopped
            # before resubmitting to avoid the deduplication lock race condition
            if state == "STARTED":
                payload_id = job_json['job_info']['job_payload']['payload_task_id']
                lock_outcome = _wait_for_lock_release(payload_id, task_id, revoke_wait_timeout, revoke_wait_max_interval)
                if lock_outcome == "force_released":
                    force_released_locks.append(f"payload_id: {payload_id} (task_id: {task_id})")
                elif lock_outcome == "held":
                    locks_still_held.append(f"payload_id: {payload_id} (task_id: {task_id})")

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
                current_time = datetime.now(timezone.utc)
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
        except JobNotFoundError as ex:
            logger.warning(str(ex))
            not_found_job_ids.append(job_id)
        except Exception as ex:
            logger.error(f"[ERROR] Exception occurred {type(ex)}:{ex} {traceback.format_exc()}")
            errored_job_ids.append(job_id)

    if force_released_locks:
        info_msgs.append("Revoke wait timed out")
        info_msg_details += f"\n\nLock for these jobs did not release within {revoke_wait_timeout}s. The revoked task had stopped, so the stale lock was force-released before resubmission:\n"
        for detail in force_released_locks:
            info_msg_details += f"{detail}\n"

    if locks_still_held:
        info_msgs.append("Job lock still held at resubmit")
        info_msg_details += f"\n\nThe original task for these jobs was still running {revoke_wait_timeout}s after revoke. Their locks were left in place to avoid duplicate execution, so the resubmitted job may fail with 'already running' until the task stops:\n"
        for detail in locks_still_held:
            info_msg_details += f"{detail}\n"

    if not_found_job_ids and len(retry_job_ids) > 1:
        not_found_details = "Some jobs not found, so could not retry:\n"
        not_found_details += json.dumps(not_found_job_ids, indent=2)
        logger.warning(not_found_details)
        info_msgs.append("Some retry jobs not found")
        info_msg_details += f"\n\n{not_found_details}"

    if errored_job_ids and len(retry_job_ids) > 1:
        errored_details = "Some jobs hit errors during resubmission; see tracebacks in the log:\n"
        errored_details += json.dumps(errored_job_ids, indent=2)
        logger.warning(errored_details)
        info_msgs.append("Some retry jobs errored")
        info_msg_details += f"\n\n{errored_details}"

    if info_msgs:
        create_info_message_files(msg=info_msgs, msg_details=info_msg_details)

    if len(retry_job_ids) == 1:
        if not_found_job_ids:
            raise RuntimeError(f"job id {not_found_job_ids[0]} not found")
        if errored_job_ids:
            raise RuntimeError(f"failed to resubmit job id {errored_job_ids[0]}; see traceback in the log")


if __name__ == "__main__":
    ctx = read_context()
    # input_type = ctx['resource']
    # if input_type == "job":
    resubmit_jobs(ctx)
    # else:
    #     logger.info("Cannot retry a task, worker or event.")
