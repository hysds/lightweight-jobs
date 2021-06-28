#!/bin/env python
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

    :param msg: The short info message.
    :param msg_details: The message details.
    :return:
    """

    if msg:
        with open('_alt_msg.txt', 'w') as f:
            f.write("%s\n" % str(msg))

    if msg_details:
        with open('_alt_msg_details.txt', 'w') as f:
            f.write("%s\n" % msg_details)
