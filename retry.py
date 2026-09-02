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
    # job.job_info.id is stamped once and reused by every attempt in a
    # payload's lineage, so this can match more than one doc: an orphaned
    # job_failed doc from a dead attempt alongside the live attempt's dated
    # doc. Unsorted, hits[0] is shard order, and picking the orphan would
    # revoke the wrong task and recompute retry_count backwards -- breaking
    # the monotonicity the supersession guard and the orphan reaper rely on.
    # Sort by retry_count first (retry.py increments it on every resubmit),
    # then recency; unmapped_type keeps this working on a venue whose
    # template does not declare the field.
    query_json = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"job.job_info.id": job_id}}
                ]
            }
        },
        "sort": [
            {"job.retry_count": {"order": "desc", "missing": "_last",
                                 "unmapped_type": "long"}},
            {"@timestamp": {"order": "desc", "unmapped_type": "date"}},
        ],
        "size": 1,
    }
    result = mozart_es.search(index=JOB_STATUS_CURRENT, body=query_json)
    total = result['hits']['total']['value']
    if total == 0:
        raise JobNotFoundError(f"job id {job_id} not found in OpenSearch")
    if total > 1:
        # visible in retry.log, so the orphan rate is observable from the
        # retry side without running a separate audit
        logger.warning(
            f"job id {job_id} matched {total} status docs; retrying the "
            f"newest attempt. The others are leftovers from earlier attempts."
        )
    return result


def _resolve_index_members(index):
    """Return the concrete OPEN indices behind an alias, or [index] if not an alias.

    Cluster metadata, not a search: immune to the refresh_interval staleness
    that a _search-based location lookup hits when logstash moves a
    job-failed doc between the alias's member indices. expand_wildcards="open"
    keeps closed members out of the sweep (an ISM policy that closes
    job_status dailies before deleting them can leave half the alias closed)
    instead of sending them deletes that can only answer 400.

    No backoff of its own: the caller's decorator retries the whole sweep, and
    stacking a second 10-try backoff here turns one OpenSearch outage into
    ~100 attempts before the retry job gives up. A 403 here is NOT ignored on
    purpose: it means the retry job's OpenSearch user cannot read aliases, and
    that must fail the retry loudly rather than skip the delete.
    """
    resp = mozart_es.es.indices.get_alias(name=index, expand_wildcards="open", ignore=[404])
    if isinstance(resp, dict) and "error" not in resp and len(resp) > 0:
        # Ordering only -- every member is still visited, so this keeps no
        # assumption about where the doc lives. job_failed goes last because
        # it is the member a job-failed doc is being moved INTO: deleting it
        # first would leave the rest of the sweep as a window in which a late
        # in-flight write can re-create it.
        return sorted(resp.keys(), key=lambda m: (m == "job_failed", m))
    logger.info(f"{index} is not an alias; treating it as a concrete index")
    return [index]


@backoff.on_exception(backoff.expo, Exception, max_tries=10, max_value=64)
def _sweep_members(index, _id, deleted_from, failed):
    """Visit every member once, recording outcomes in the caller's lists.

    One member must not be able to abort the sweep. The delete is spread over
    every member of the alias, so aborting partway can leave the doc destroyed
    where it mattered and the job never resubmitted -- the caller treats a
    raised sweep as "do not resubmit". So a per-member failure is recorded and
    the sweep carries on. Reachable ones are not exotic: 403 from an ISM
    read-only block on an aged daily, 429 from a flood-stage watermark, 503
    from a shard moving during a rolling restart. None of them say anything
    about the member that actually holds the doc.

    Raises only when nothing was deleted anywhere AND at least one member
    failed, which is the safe case to retry: no doc has been destroyed yet, so
    the caller's backoff can replay the whole sweep. `deleted_from` and
    `failed` are owned by the caller precisely so they survive that replay --
    a member emptied on attempt 1 answers not_found on attempt 2 and would
    otherwise drop out of the result.
    """
    for member in _resolve_index_members(index):
        if member in deleted_from:
            continue                      # already emptied on an earlier pass
        try:
            res = mozart_es.delete_by_id(index=member, id=_id, ignore=[400, 404])
        except Exception as e:
            failed[member] = f"{type(e).__name__}: {e}"
            logger.warning(f"Delete failed on {member} for {_id}: {failed[member]}")
            continue
        failed.pop(member, None)
        if isinstance(res, dict) and res.get("result") == "deleted":
            logger.info(f"Deleted job status doc {_id} from {member}")
            deleted_from.append(member)
        elif isinstance(res, dict) and res.get("status") in (400, 403, 429):
            # closed, write-blocked or throttled: this member cannot be swept
            # now, but it is not evidence about any other member
            logger.warning(f"Skipped {member} for {_id}: {res.get('error')}")
        else:
            logger.info(f"No {_id} doc in {member}; nothing to delete.")
    if failed and not deleted_from:
        raise RuntimeError(
            f"no member of {index} could be swept for {_id}: {failed}"
        )


def delete_by_id(index, _id):
    """Delete the doc id at EVERY concrete index behind `index`.

    Doc-ID deletes are realtime (translog), so unlike the previous
    search-then-delete-at-the-found-address, this cannot act on a stale view
    of which member index currently holds the doc (the job-failed doc moves
    from job_status-<date> to job_failed while the retry is running).

    Returns the list of member indices a live doc was actually deleted from.
    Raises only if nothing could be deleted anywhere, so the caller resubmits
    whenever the doc was removed from at least one member and fails loudly
    only when the sweep achieved nothing.
    """
    deleted_from = []
    failed = {}
    _sweep_members(index, _id, deleted_from, failed)
    if failed:
        # something was deleted, so the resubmit goes ahead; say plainly what
        # was left behind, because those members may still hold a copy
        logger.error(
            f"Deleted {_id} from {deleted_from} but could not sweep {sorted(failed)}: "
            f"{failed}. A stale copy may remain there."
        )
    if not deleted_from:
        # Doc not indexed anywhere yet at delete time. assert_doc_settled
        # makes this unexpected for the retry flow; a late in-flight write
        # re-creating the doc afterward is a separate, write-side defect.
        logger.warning(f"{_id} not found in any index behind {index}; nothing deleted")
    return deleted_from


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

            # delete the old job status doc everywhere it lives: the sweep is
            # alias-wide and realtime, so a job-failed doc that logstash has
            # already moved out of the dated index is still deleted.
            # A duplicate surviving this is the late-write variant.
            deleted_from = delete_by_id(JOB_STATUS_CURRENT, _id)

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
            # move it back to job_status. Decide from the realtime delete sweep
            # as well, not only the search's (possibly stale) view of _index.
            if index.startswith("job_failed") or any(m.startswith("job_failed") for m in deleted_from):
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

    if not_found_job_ids and len(retry_job_ids) == 1:
        raise RuntimeError(f"job id {not_found_job_ids[0]} not found")

    # An errored job may have had its status doc deleted without a resubmit,
    # so it must not exit 0. Figaro submits retry_job_id as a LIST even for a
    # single job, and the old length gate let that case report job-completed
    # while the job had silently vanished.
    if errored_job_ids:
        raise RuntimeError(
            f"failed to resubmit {len(errored_job_ids)} of {len(retry_job_ids)} "
            f"job id(s): {errored_job_ids}; see tracebacks in the log"
        )


if __name__ == "__main__":
    ctx = read_context()
    # input_type = ctx['resource']
    # if input_type == "job":
    resubmit_jobs(ctx)
    # else:
    #     logger.info("Cannot retry a task, worker or event.")
