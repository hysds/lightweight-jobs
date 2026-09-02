#!/bin/env python
import logging

import backoff
from redis import BlockingConnectionPool, StrictRedis, RedisError

from hysds.celery import app


REVOKED_TASK_POOL = None
REVOKED_TASK_TMPL = "hysds-revoked-task-%s"


def set_redis_revoked_task_pool():
    """Set redis connection pool for worker status."""

    global REVOKED_TASK_POOL
    if REVOKED_TASK_POOL is None:
        REVOKED_TASK_POOL = BlockingConnectionPool.from_url(
            app.conf.REDIS_JOB_STATUS_URL)


@backoff.on_exception(backoff.expo,
                      RedisError,
                      max_tries=10,
                      max_value=64)
def revoke(task_id, state):
    """Revoke task."""

    # set redis pool
    set_redis_revoked_task_pool()
    global REVOKED_TASK_POOL

    # record revoked task
    r = StrictRedis(connection_pool=REVOKED_TASK_POOL)
    r.setex(REVOKED_TASK_TMPL % task_id,
            app.conf.HYSDS_JOB_STATUS_EXPIRES,
            state)

    # revoke task
    app.control.revoke(task_id, terminate=True)


def create_info_message_files(msg=None, msg_details=None):
    """
    Creates the _alt_msg.txt and _alt_msg_details.txt
    files for population into the job status json.

    :param msg: The short info message. Can be a list or a string.
     Should be shorter than 35 characters.
    :param msg_details: The message details.
    :return:
    """

    if msg:
        with open('_alt_msg.txt', 'w') as f:
            if isinstance(msg, list):
                for m in msg:
                    f.write("%s\n" % str(m))
            else:
                f.write("%s\n" % str(msg))

    if msg_details:
        with open('_alt_msg_details.txt', 'w') as f:
            f.write("%s\n" % msg_details)


def resolve_index_members(es, index):
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
    resp = es.es.indices.get_alias(name=index, expand_wildcards="open", ignore=[404])
    if isinstance(resp, dict) and "error" not in resp and len(resp) > 0:
        # Ordering only -- every member is still visited, so this keeps no
        # assumption about where the doc lives. job_failed goes last because
        # it is the member a job-failed doc is being moved INTO: deleting it
        # first would leave the rest of the sweep as a window in which a late
        # in-flight write can re-create it.
        return sorted(resp.keys(), key=lambda m: (m == "job_failed", m))
    logging.info(f"{index} is not an alias; treating it as a concrete index")
    return [index]


@backoff.on_exception(backoff.expo, Exception, max_tries=10, max_value=64)
def sweep_members(es, index, _id, deleted_from, failed):
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
    for member in resolve_index_members(es, index):
        if member in deleted_from:
            continue                      # already emptied on an earlier pass
        try:
            res = es.delete_by_id(index=member, id=_id, ignore=[400, 404])
        except Exception as e:
            failed[member] = f"{type(e).__name__}: {e}"
            logging.warning(f"Delete failed on {member} for {_id}: {failed[member]}")
            continue
        failed.pop(member, None)
        if isinstance(res, dict) and res.get("result") == "deleted":
            logging.info(f"Deleted job status doc {_id} from {member}")
            deleted_from.append(member)
        elif isinstance(res, dict) and res.get("status") in (400, 403, 429):
            # closed, write-blocked or throttled: this member cannot be swept
            # now, but it is not evidence about any other member
            logging.warning(f"Skipped {member} for {_id}: {res.get('error')}")
        else:
            logging.info(f"No {_id} doc in {member}; nothing to delete.")
    if failed and not deleted_from:
        raise RuntimeError(
            f"no member of {index} could be swept for {_id}: {failed}"
        )


def delete_by_id(es, index, _id):
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
    sweep_members(es, index, _id, deleted_from, failed)
    if failed:
        # something was deleted, so the resubmit goes ahead; say plainly what
        # was left behind, because those members may still hold a copy
        logging.error(
            f"Deleted {_id} from {deleted_from} but could not sweep {sorted(failed)}: "
            f"{failed}. A stale copy may remain there."
        )
    if not deleted_from:
        # Doc not indexed anywhere yet at delete time. assert_doc_settled
        # makes this unexpected for the retry flow; a late in-flight write
        # re-creating the doc afterward is a separate, write-side defect.
        logging.warning(f"{_id} not found in any index behind {index}; nothing deleted")
    return deleted_from
