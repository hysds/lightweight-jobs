"""retry.py must delete the old status doc wherever it actually lives.

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

DELETED = {"_index": FAILED, "_id": PAYLOAD_ID, "result": "deleted",
           "_seq_no": 42, "_primary_term": 7}
NOT_FOUND = {"_id": PAYLOAD_ID, "result": "not_found", "_seq_no": 41, "_primary_term": 7}
CLOSED_400 = {"error": {"type": "index_closed_exception",
                        "reason": "closed"}, "status": 400}
ALIAS_404 = {"error": "alias_not_found_exception", "status": 404}


class AuthorizationException(Exception):
    """Stand-in for the client's 403 exception type."""


def _alias_map(*members):
    return {m: {"aliases": {ALIAS: {}}} for m in members}


def _seed_location(es, members, stale_hit=None):
    """Seed BOTH addressing paths for the same doc.

    `get_alias` is what the fix reads; `search_by_id` is what the previous
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


def _failed_doc(retry_count=None, stale_index=DAILY, payload_id=PAYLOAD_ID,
                job_id="send_notify_msg-job_worker-large"):
    """A job-failed status doc shaped like the real INT-FWD orphans.

    `stale_index` is what the search reports; the doc has in fact already been
    moved to `job_failed` by logstash (job_info.index == "job_failed").
    """
    job = {
        "job_id": job_id,
        "type": "job-send_notify_msg:6.0.5",
        "priority": 4,
        "job_info": {
            "id": job_id,
            "index": FAILED,
            "job_queue": "system-jobs-queue",
            "time_limit": 3600,
            "soft_time_limit": 3300,
            "duration": 12.3,
            "status": 255,
            "time_start": "2026-08-28T18:09:54.000Z",
            "time_end": "2026-08-28T18:10:06.000Z",
            "job_payload": {"payload_task_id": payload_id},
        },
    }
    if retry_count is not None:
        job["retry_count"] = retry_count
    return {
        "hits": {
            "total": {"value": 1},
            "hits": [{
                "_index": stale_index,
                "_id": payload_id,
                "_source": {"uuid": OLD_TASK_ID, "payload_id": payload_id, "job": job},
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
    # job_failed is visited LAST: it is the member the doc is being moved
    # into, so deleting it first would leave the rest of the sweep as a
    # window for a late write to re-create it
    assert swept == [DAILY, FAILED]
    for call in es.delete_by_id.call_args_list:
        assert call.kwargs["id"] == PAYLOAD_ID
        assert call.kwargs["ignore"] == [404, 400, 403, 429]
    assert deleted_from == [DAILY, FAILED]


def test_hc640_doc_already_moved_to_job_failed_is_still_deleted(retry_module):
    """2. The stale-read scenario: gone from the daily, present in job_failed."""
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
    es.delete_by_id.assert_called_once_with(
        index=DAILY, id=PAYLOAD_ID, ignore=[404, 400, 403, 429]
    )


def test_nothing_found_anywhere_warns_and_returns_empty(retry_module, caplog):
    """4. No doc in any member: warn (write-side defect), do not raise."""
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


def test_nothing_deleted_and_a_member_failed_is_fatal(retry_module):
    """8a. If no member could be swept, fail loudly: nothing was destroyed.

    This is the safe case to raise on. The doc is still wherever it was, so
    the caller declines to resubmit and an operator can retry by hand.
    """
    es = retry_module.mozart_es
    _seed_location(es, (DAILY, FAILED))
    es.delete_by_id.side_effect = _delete_returns(
        {DAILY: NOT_FOUND, FAILED: AuthorizationException("403")})

    with pytest.raises(RuntimeError, match="could not be swept|could be swept"):
        retry_module.delete_by_id(ALIAS, PAYLOAD_ID)


def test_a_member_failure_after_a_delete_does_not_abort_the_sweep(retry_module):
    """The data-loss case: the doc IS deleted, so the resubmit must proceed.

    The delete is spread over every alias member. If one member's failure
    aborted the sweep, the caller would treat the whole retry as errored and
    never resubmit -- while the doc had already been removed from the member
    that held it. The job would vanish from Figaro with no way to recover its
    parameters.
    """
    es = retry_module.mozart_es
    _seed_location(es, (DAILY, FAILED))
    es.delete_by_id.side_effect = _delete_returns(
        {DAILY: AuthorizationException("403 read-only"), FAILED: DELETED})

    deleted_from = retry_module.delete_by_id(ALIAS, PAYLOAD_ID)

    assert deleted_from == [FAILED]
    assert [c.kwargs["index"] for c in es.delete_by_id.call_args_list] == [DAILY, FAILED]


def test_read_only_member_answering_403_is_skipped(retry_module, caplog):
    """An ISM read-only block on an aged daily says nothing about job_failed."""
    caplog.set_level(logging.INFO)
    es = retry_module.mozart_es
    blocked = "job_status-2026.01.01"
    _seed_location(es, (blocked, DAILY, FAILED))
    es.delete_by_id.side_effect = _delete_returns(
        {blocked: {"error": {"type": "cluster_block_exception"}, "status": 403},
         DAILY: NOT_FOUND, FAILED: DELETED})

    deleted_from = retry_module.delete_by_id(ALIAS, PAYLOAD_ID)

    assert deleted_from == [FAILED]
    assert len(es.delete_by_id.call_args_list) == 3


def test_deleted_from_survives_a_backoff_replay(retry_module):
    """The sweep is replayed by the real backoff decorator; the result must
    reflect the whole run, and a member emptied on an earlier pass must not be
    revisited."""
    es = retry_module.mozart_es
    es.es.indices.get_alias.side_effect = [
        ConnectionError("transport blip"),
        _alias_map(DAILY, FAILED),
    ]
    es.search_by_id.return_value = [{"_index": DAILY, "_id": PAYLOAD_ID, "found": True}]
    es.delete_by_id.side_effect = _delete_returns({DAILY: NOT_FOUND, FAILED: DELETED})

    deleted_from = retry_module.delete_by_id(ALIAS, PAYLOAD_ID)

    assert deleted_from == [FAILED]
    assert es.es.indices.get_alias.call_count == 2
    assert [c.kwargs["index"] for c in es.delete_by_id.call_args_list] == [DAILY, FAILED]


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


def test_a_bulk_retry_with_one_errored_job_still_fails_loudly(retry_module):
    """Figaro submits retry_job_id as a LIST, even for one job.

    The old length gate only re-raised for a scalar, so on the list branch an
    errored job wrote an info-message file and the retry job exited 0 --
    reporting job-completed in Figaro for a job whose status doc may already
    have been deleted with no resubmit.
    """
    es = retry_module.mozart_es
    good, bad = "payload-good", "payload-bad"
    es.search.side_effect = [_failed_doc(payload_id=bad), _failed_doc(payload_id=good)]
    _seed_location(es, (DAILY, FAILED))

    def _delete(index=None, id=None, **_kw):
        if id == bad:
            raise AuthorizationException("403 on every member")
        return DELETED if index == FAILED else NOT_FOUND

    es.delete_by_id.side_effect = _delete

    with pytest.raises(RuntimeError, match=r"failed to resubmit 1 of 2"):
        retry_module.resubmit_jobs(_context(retry_job_id=[bad, good]))

    # the healthy job in the same batch was still resubmitted
    retry_module.run_job.apply_async.assert_called_once()
    assert retry_module.log_job_status.call_args.args[0]["payload_id"] == good


def test_query_picks_the_newest_attempt_and_flags_duplicates(retry_module, caplog):
    """job.job_info.id is shared across a payload's attempts, so an orphan and
    the live attempt both match. Unsorted, hits[0] is shard order."""
    caplog.set_level(logging.INFO)
    es = retry_module.mozart_es
    doc = _failed_doc()
    doc["hits"]["total"]["value"] = 2
    es.search.return_value = doc

    retry_module.query_es_required("send_notify_msg-job_worker-large")

    body = es.search.call_args.kwargs["body"]
    assert body["size"] == 1
    assert body["sort"][0]["job.retry_count"]["order"] == "desc"
    assert body["sort"][0]["job.retry_count"]["unmapped_type"] == "long"
    assert body["sort"][1]["@timestamp"]["order"] == "desc"
    assert any("matched 2 status docs" in r.message for r in caplog.records)


def test_purge_uses_the_shared_sweep_not_a_search():
    """purge.py carried the identical defect: it chose its delete target from
    a near-realtime search's _index. Unlike the retry case nothing masked it,
    so the job could not be purged while success was logged."""
    import pathlib

    src = (pathlib.Path(__file__).resolve().parents[1] / "purge.py").read_text()
    assert "delete_by_id(es, es_index, payload_id)" in src, (
        "purge.py must sweep the ALIAS; handing the sweep result['_index'] "
        "resolves to that one concrete index and degenerates to the old delete"
    )
    assert "search_by_id" not in src


# --------------------------------------------------------------------------
# skippable statuses, delete marks, batch outcomes
# --------------------------------------------------------------------------

def test_403_and_429_are_ignored_at_the_client_not_caught(retry_module):
    """The hysds_commons connection wrapper backs off on bare Exception, so an
    un-ignored 403 costs ten HTTP attempts and ~34 s before surfacing, and it
    surfaces as a non-transport exception. Only `ignore` makes the skip branch
    reachable, and only then does a blocked member cost one round trip."""
    es = retry_module.mozart_es
    blocked, throttled = "job_status-2026.01.01", "job_status-2026.02.01"
    _seed_location(es, (blocked, throttled, DAILY, FAILED))
    es.delete_by_id.side_effect = _delete_returns({
        blocked: {"error": {"type": "cluster_block_exception"}, "status": 403},
        throttled: {"error": {"type": "es_rejected_execution_exception"}, "status": 429},
        DAILY: NOT_FOUND, FAILED: DELETED})

    deleted_from = retry_module.delete_by_id(ALIAS, PAYLOAD_ID)

    assert deleted_from == [FAILED]
    for call in es.delete_by_id.call_args_list:
        assert 403 in call.kwargs["ignore"] and 429 in call.kwargs["ignore"]


def test_skipped_members_are_not_failures(retry_module, caplog):
    """A skip must not populate `failed`, or every retry on an ISM read-only
    venue logs the 'a stale copy may remain' ERROR on success."""
    caplog.set_level(logging.INFO)
    es = retry_module.mozart_es
    blocked = "job_status-2026.01.01"
    _seed_location(es, (blocked, FAILED))
    es.delete_by_id.side_effect = _delete_returns(
        {blocked: {"error": "blocked", "status": 403}, FAILED: DELETED})

    retry_module.delete_by_id(ALIAS, PAYLOAD_ID)

    assert not any(r.levelno >= logging.ERROR for r in caplog.records)


def test_delete_marks_record_every_members_sequence_position(retry_module):
    """Even a not_found delete is sequenced, so every member gets a mark. The
    reaper compares a job_failed doc's own _seq_no against the job_failed mark
    to tell indexing order exactly, which timestamps cannot."""
    es = retry_module.mozart_es
    _seed_location(es, (DAILY, FAILED))
    es.delete_by_id.side_effect = _delete_returns({DAILY: NOT_FOUND, FAILED: DELETED})
    marks = []

    retry_module.delete_by_id(ALIAS, PAYLOAD_ID, marks=marks)

    assert marks == [
        {"index": DAILY, "seq_no": 41, "primary_term": 7},
        {"index": FAILED, "seq_no": 42, "primary_term": 7},
    ]


def test_resubmitted_job_carries_the_delete_marks(retry_module):
    es = retry_module.mozart_es
    es.search.return_value = _failed_doc()
    _seed_location(es, (DAILY, FAILED))
    es.delete_by_id.side_effect = _delete_returns({DAILY: NOT_FOUND, FAILED: DELETED})

    retry_module.resubmit_jobs(_context())

    job = retry_module.log_job_status.call_args.args[0]["job"]
    marks = job["job_info"]["retry_delete"]
    assert {"index": FAILED, "seq_no": 42, "primary_term": 7} in marks
    # A list of flat entries, so the mapping never grows with the alias.
    assert all(set(m) == {"index", "seq_no", "primary_term"} for m in marks)


def test_all_not_found_and_nothing_resubmitted_raises(retry_module):
    """Re-running a batch whose ids were swept and then errored: every id is
    now not-found, and it used to exit 0 having resubmitted nothing."""
    es = retry_module.mozart_es
    es.search.return_value = {"hits": {"total": {"value": 0}, "hits": []}}

    with pytest.raises(RuntimeError, match="not found and nothing was resubmitted"):
        retry_module.resubmit_jobs(_context(retry_job_id=["gone-1", "gone-2"]))

    retry_module.run_job.apply_async.assert_not_called()


def test_not_found_inside_a_batch_that_did_work_is_benign(retry_module):
    es = retry_module.mozart_es

    def _search(index=None, body=None, **_kw):
        # the not-found id is searched 4 times by the JobNotFoundError backoff
        job_id = body["query"]["bool"]["must"][0]["term"]["job.job_info.id"]
        if job_id == "gone":
            return {"hits": {"total": {"value": 0}, "hits": []}}
        return _failed_doc(payload_id="payload-good")

    es.search.side_effect = _search
    _seed_location(es, (DAILY, FAILED))
    es.delete_by_id.side_effect = _delete_returns({DAILY: NOT_FOUND, FAILED: DELETED})

    retry_module.resubmit_jobs(_context(retry_job_id=["gone", "good-job"]))   # no raise

    retry_module.run_job.apply_async.assert_called_once()


def test_batch_info_message_lists_the_outcome_sets(retry_module):
    """An operator acting on a partial batch needs the succeeded set, and a
    warning not to blindly re-run the same selection."""
    es = retry_module.mozart_es
    es.search.side_effect = [_failed_doc(retry_count=3, payload_id="spent", job_id="spent-job"),
                             _failed_doc(payload_id="good", job_id="good-job")]
    _seed_location(es, (DAILY, FAILED))
    es.delete_by_id.side_effect = _delete_returns({DAILY: NOT_FOUND, FAILED: DELETED})

    retry_module.resubmit_jobs(_context(retry_job_id=["spent-job", "good-job"], retry_count_max=3))

    details = retry_module.create_info_message_files.call_args.kwargs["msg_details"]
    assert "Resubmitted (1)" in details and '"good-job"' in details
    assert "Skipped before any delete (1)" in details and '"spent-job"' in details
