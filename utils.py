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

    # revoke task
    app.control.revoke(task_id, terminate=True)

    # record revoke
    r = StrictRedis(connection_pool=REVOKED_TASK_POOL)
    r.setex(REVOKED_TASK_TMPL % task_id,
            app.conf.HYSDS_JOB_STATUS_EXPIRES,
            state)
