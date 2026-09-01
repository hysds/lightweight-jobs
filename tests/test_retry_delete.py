"""HC-640: retry.py must delete the old status doc wherever it actually lives.

The defect these tests lock down: retry.py located the doc with a
near-realtime `_search` and then deleted only at the `_index` that search
reported. When logstash moves a job-failed doc from `job_status-<date>` to
`job_failed` while the retry runs, the search can still return the pre-move
address (the shard copy it landed on has not refreshed), so the delete misses
and an orphaned `job_failed` doc is left beside the retried attempt.
"""
import logging
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

ALIAS = "job_status-current"
DAILY = "job_status-2026.08.28"
FAILED = "job_failed"
PAYLOAD_ID = "a40296c2-2921-4f07-8d03-ee0fc22be6a2"
OLD_TASK_ID = "ba0f5f0e-6c9e-4a0e-9d3c-1f0f1f6f7a11"

DELETED = {"_index": FAILED, "_id": PAYLOAD_ID, "result": "deleted"}
NOT_FOUND = {"_id": PAYLOAD_ID, "result": "not_found"}
CLOSED_400 = {"error": {"type": "index_closed_exception",
                        "reason": "closed"}, "status": 400}
ALIAS_404 = {"error": "alias_not_found_exception", "status": 404}


class AuthorizationException(Exception):
    """Stand-in for the client's 403 exception type."""


def _alias_map(*members):
    return {m: {"aliases": {ALIAS: {}}} for m in members}


def _seed_location(es, members, stale_hit=None):
    """Seed BOTH addressing paths for the same doc.

    `get_alias` is what the fix reads; `search_by_id` is what the pre-HC-640
    code read. Seeding the old path too means these tests fail against the
    old code on the discriminating assertion (it deleted only at the stale
    `_index`) rather than on a mock that was never configured.
    """
    es.es.indices.get_alias.return_value = _alias_map(*members)
    if stale_hit is None:
        stale_hit = {"_index": DAILY, "_id": PAYLOAD_ID, "found": True}
    es.search_by_id.return_value = [stale_hit]


def _delete_returns(mapping, default=NOT_FOUND):
    """side_effect for mozart_es.delete_by_id keyed by target index."""
    def _side_effect(index=None, id=None, **_kwargs):
        value = mapping.get(index, default)
        if isinstance(value, Exception):
            raise value
        return value
    return _side_effect


def _failed_doc(retry_count=None, stale_index=DAILY):
    """A job-failed status doc shaped like the real INT-FWD orphans.

    `stale_index` is what the search reports; the doc has in fact already been
    moved to `job_failed` by logstash (job_info.index == "job_failed").
    """
    job = {
        "job_id": "send_notify_msg-job_worker-large",
        "type": "job-send_notify_msg:6.0.5",
        "priority": 4,
        "job_info": {
            "id": "send_notify_msg-job_worker-large",
            "index": FAILED,
            "job_queue": "system-jobs-queue",
            "time_limit": 3600,
            "soft_time_limit": 3300,
            "duration": 12.3,
            "status": 255,
            "time_start": "2026-08-28T18:09:54.000Z",
            "time_end": "2026-08-28T18:10:06.000Z",
            "job_payload": {"payload_task_id": PAYLOAD_ID},
        },
    }
    if retry_count is not None:
        job["retry_count"] = retry_count
    return {
        "hits": {
            "total": {"value": 1},
            "hits": [{
                "_index": stale_index,
                "_id": PAYLOAD_ID,
                "_source": {"uuid": OLD_TASK_ID, "payload_id": PAYLOAD_ID, "job": job},
            }],
        }
    }


def _context(**overrides):
    ctx = {"retry_job_id": "send_notify_msg-job_worker-large",
           "retry_count_max": 3,
           "new_job_priority": 4}
    ctx.update(overrides)
    return ctx


# --------------------------------------------------------------------------
# delete_by_id / _resolve_index_members
# --------------------------------------------------------------------------

def test_alias_resolves_and_every_member_is_swept(retry_module):
    """1. All concrete members behind the alias get a delete."""
    es = retry_module.mozart_es
    _seed_location(es, (DAILY, FAILED))
    es.delete_by_id.side_effect = _delete_returns({DAILY: DELETED, FAILED: DELETED})

    deleted_from = retry_module.delete_by_id(ALIAS, PAYLOAD_ID)

    es.es.indices.get_alias.assert_called_once_with(
        name=ALIAS, expand_wildcards="open", ignore=[404])
    swept = [c.kwargs["index"] for c in es.delete_by_id.call_args_list]
    assert swept == sorted([DAILY, FAILED])
    for call in es.delete_by_id.call_args_list:
        assert call.kwargs["id"] == PAYLOAD_ID
        assert call.kwargs["ignore"] == [400, 404]
    assert deleted_from == sorted([DAILY, FAILED])


def test_hc640_doc_already_moved_to_job_failed_is_still_deleted(retry_module):
    """2. The HC-640 scenario: gone from the daily, present in job_failed."""
    es = retry_module.mozart_es
    _seed_location(es, (DAILY, FAILED))
    es.delete_by_id.side_effect = _delete_returns({DAILY: NOT_FOUND, FAILED: DELETED})

    deleted_from = retry_module.delete_by_id(ALIAS, PAYLOAD_ID)

    assert deleted_from == [FAILED]
    assert FAILED in [c.kwargs["index"] for c in es.delete_by_id.call_args_list]


def test_concrete_index_falls_back_to_itself(retry_module):
    """3. Not an alias: behave like the old single-index delete."""
    es = retry_module.mozart_es
    es.es.indices.get_alias.return_value = ALIAS_404
    es.search_by_id.return_value = [{"_index": DAILY, "_id": PAYLOAD_ID, "found": True}]
    es.delete_by_id.side_effect = _delete_returns({DAILY: DELETED})

    deleted_from = retry_module.delete_by_id(DAILY, PAYLOAD_ID)

    assert deleted_from == [DAILY]
    es.delete_by_id.assert_called_once_with(index=DAILY, id=PAYLOAD_ID, ignore=[400, 404])


def test_nothing_found_anywhere_warns_and_returns_empty(retry_module, caplog):
    """4. No doc in any member: warn (HC-648 territory), do not raise."""
    caplog.set_level(logging.INFO)
    es = retry_module.mozart_es
    _seed_location(es, (DAILY, FAILED), stale_hit={"found": False})
    es.delete_by_id.side_effect = _delete_returns({})

    deleted_from = retry_module.delete_by_id(ALIAS, PAYLOAD_ID)

    assert deleted_from == []
    assert any("nothing deleted" in r.message for r in caplog.records)


def test_closed_member_is_skipped_without_sinking_the_sweep(retry_module, caplog):
    """7. A member closed between get_alias and the delete answers 400."""
    caplog.set_level(logging.INFO)
    es = retry_module.mozart_es
    closed = "job_status-2026.07.01"
    _seed_location(es, (closed, DAILY, FAILED))
    es.delete_by_id.side_effect = _delete_returns(
        {closed: CLOSED_400, DAILY: NOT_FOUND, FAILED: DELETED})

    deleted_from = retry_module.delete_by_id(ALIAS, PAYLOAD_ID)

    assert deleted_from == [FAILED]
    assert len(es.delete_by_id.call_args_list) == 3
    assert any("Skipped" in r.message and closed in r.message for r in caplog.records)


def test_403_on_a_member_delete_propagates(retry_module):
    """8a. A permission problem must not be swallowed as "nothing to delete"."""
    es = retry_module.mozart_es
    _seed_location(es, (DAILY, FAILED))
    es.delete_by_id.side_effect = _delete_returns(
        {DAILY: NOT_FOUND, FAILED: AuthorizationException("403")})

    with pytest.raises(AuthorizationException):
        retry_module.delete_by_id(ALIAS, PAYLOAD_ID)


def test_403_on_get_alias_propagates(retry_module):
    """9a. Cannot read the alias -> cannot know the sweep is complete."""
    es = retry_module.mozart_es
    es.es.indices.get_alias.side_effect = AuthorizationException("403")

    with pytest.raises(AuthorizationException):
        retry_module.delete_by_id(ALIAS, PAYLOAD_ID)
    es.delete_by_id.assert_not_called()


# --------------------------------------------------------------------------
# resubmit_jobs
# --------------------------------------------------------------------------

def test_resubmit_deletes_at_job_failed_despite_stale_search_index(retry_module):
    """5. End-to-end: search reports the daily, the doc lives in job_failed."""
    es = retry_module.mozart_es
    es.search.return_value = _failed_doc()
    _seed_location(es, (DAILY, FAILED))
    es.delete_by_id.side_effect = _delete_returns({DAILY: NOT_FOUND, FAILED: DELETED})

    retry_module.resubmit_jobs(_context())

    # the discriminating assertion: unpatched retry.py only ever deleted at
    # the search's _index (the daily), never at job_failed
    swept = [c.kwargs["index"] for c in es.delete_by_id.call_args_list]
    assert FAILED in swept

    status_json = retry_module.log_job_status.call_args.args[0]
    job_json = status_json["job"]
    today = datetime.now(timezone.utc).strftime("%Y.%m.%d")
    assert job_json["job_info"]["index"] == f"job_status-{today}"
    assert status_json["status"] == "job-queued"
    assert status_json["payload_id"] == PAYLOAD_ID
    assert job_json["retry_count"] == 1
    assert job_json["job_info"]["dedup"] is False
    assert "time_start" not in job_json["job_info"]
    retry_module.run_job.apply_async.assert_called_once()


def test_retry_count_max_reached_deletes_nothing(retry_module):
    """6. A terminally failed job keeps its doc: no delete, no resubmit."""
    es = retry_module.mozart_es
    es.search.return_value = _failed_doc(retry_count=3)
    _seed_location(es, (DAILY, FAILED))

    retry_module.resubmit_jobs(_context(retry_count_max=3))

    es.delete_by_id.assert_not_called()
    retry_module.log_job_status.assert_not_called()
    retry_module.run_job.apply_async.assert_not_called()


def test_403_during_the_sweep_fails_the_retry_without_resubmitting(retry_module):
    """8b. A half-swept doc is never resubmitted; a single retry raises."""
    es = retry_module.mozart_es
    es.search.return_value = _failed_doc()
    _seed_location(es, (DAILY, FAILED))
    es.delete_by_id.side_effect = _delete_returns(
        {DAILY: NOT_FOUND, FAILED: AuthorizationException("403")})

    with pytest.raises(RuntimeError, match="failed to resubmit"):
        retry_module.resubmit_jobs(_context())

    retry_module.log_job_status.assert_not_called()
    retry_module.run_job.apply_async.assert_not_called()


def test_403_on_get_alias_fails_the_retry_without_resubmitting(retry_module):
    """9b. Same outcome when the alias read is what is forbidden."""
    es = retry_module.mozart_es
    es.search.return_value = _failed_doc()
    es.es.indices.get_alias.side_effect = AuthorizationException("403")

    with pytest.raises(RuntimeError, match="failed to resubmit"):
        retry_module.resubmit_jobs(_context())

    es.delete_by_id.assert_not_called()
    retry_module.log_job_status.assert_not_called()
    retry_module.run_job.apply_async.assert_not_called()
