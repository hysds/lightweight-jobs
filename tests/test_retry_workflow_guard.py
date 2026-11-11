"""D5 guard (workflow contract 12.4) and workflow-job retry semantics of retry.py."""
import json

import pytest

from test_retry_delete import (DAILY, FAILED, PAYLOAD_ID, DELETED, NOT_FOUND, _context,
                               _delete_returns, _failed_doc, _seed_location)

TAG = {"run_id": "run-1", "step_id": "B", "spec_id": "s:1", "child_index": 2, "child_total": 5}


def _tagged_doc(where="job", retry_count=None):
    d = _failed_doc(retry_count=retry_count)
    src = d["hits"]["hits"][0]["_source"]
    if where == "job":
        src["job"]["workflow"] = dict(TAG)
    elif where == "top":
        src["workflow"] = dict(TAG)
    elif where == "job_info":
        src["job"]["job_info"]["workflow"] = dict(TAG)
    return d


def _arm(es, doc):
    es.search.return_value = doc
    _seed_location(es, (DAILY, FAILED))
    es.delete_by_id.side_effect = _delete_returns({DAILY: NOT_FOUND, FAILED: DELETED})


@pytest.mark.parametrize("where", ["job", "top", "job_info"])
def test_workflow_job_skipped_without_workflow_retry(retry_module, where, caplog):
    es = retry_module.mozart_es
    _arm(es, _tagged_doc(where))
    with caplog.at_level("ERROR"):
        retry_module.resubmit_jobs(_context())  # skipped is not an error: no raise
    es.delete_by_id.assert_not_called()          # nothing deleted
    retry_module.revoke.assert_not_called()      # nothing revoked
    retry_module.log_job_status.assert_not_called()
    retry_module.run_job.apply_async.assert_not_called()
    retry_module.create_info_message_files.assert_not_called()  # single job: no batch message


@pytest.mark.parametrize("value", [False, "false", "", None, 0, "no"])
def test_workflow_job_skipped_when_workflow_retry_falsy(retry_module, value):
    es = retry_module.mozart_es
    _arm(es, _tagged_doc())
    retry_module.resubmit_jobs(_context(workflow_retry=value))
    retry_module.run_job.apply_async.assert_not_called()
    es.delete_by_id.assert_not_called()


@pytest.mark.parametrize("value", [True, "true", "True", " TRUE "])
def test_workflow_job_retried_with_workflow_retry(retry_module, value):
    es = retry_module.mozart_es
    _arm(es, _tagged_doc())
    retry_module.resubmit_jobs(_context(workflow_retry=value))
    retry_module.run_job.apply_async.assert_called_once()


def test_non_workflow_job_retried_normally_without_flag(retry_module):
    es = retry_module.mozart_es
    _arm(es, _failed_doc())
    retry_module.resubmit_jobs(_context())
    retry_module.run_job.apply_async.assert_called_once()


def test_non_workflow_job_unaffected_by_flag(retry_module):
    es = retry_module.mozart_es
    _arm(es, _failed_doc())
    retry_module.resubmit_jobs(_context(workflow_retry=True))
    retry_module.run_job.apply_async.assert_called_once()


def test_resubmit_preserves_job_id_and_workflow_tag(retry_module):
    """Contract 12.3: the retried attempt keeps its job id and top-level workflow tag; the
    job-queued doc (which becomes the job_status doc) carries the tag via job['workflow']."""
    es = retry_module.mozart_es
    _arm(es, _tagged_doc(retry_count=1))
    retry_module.resubmit_jobs(_context(workflow_retry=True, retry_count_max=3))
    status_json = retry_module.log_job_status.call_args.args[0]
    job = status_json["job"]
    assert status_json["job_id"] == job["job_id"] == "send_notify_msg-job_worker-large"
    assert job["job_info"]["id"] == job["job_id"]
    assert job["workflow"] == TAG
    assert job["retry_count"] == 2
    args, kwargs = retry_module.run_job.apply_async.call_args
    sent = args[0][0]
    assert sent["workflow"] == TAG and sent["job_id"] == job["job_id"]
    assert job["job_info"]["dedup"] is False  # retry resubmits straight to run_job, no orchestrator dedup


def test_workflow_retry_exhausted_still_respects_retry_count_max(retry_module):
    es = retry_module.mozart_es
    _arm(es, _tagged_doc(retry_count=2))
    retry_module.resubmit_jobs(_context(workflow_retry=True, retry_count_max=2))
    retry_module.run_job.apply_async.assert_not_called()
    es.delete_by_id.assert_not_called()


def test_stale_error_class_cleared_on_resubmit(retry_module):
    es = retry_module.mozart_es
    d = _failed_doc()
    d["hits"]["hits"][0]["_source"]["job"]["job_info"]["error_class"] = "exit_code:3"
    _arm(es, d)
    retry_module.resubmit_jobs(_context())
    assert "error_class" not in retry_module.log_job_status.call_args.args[0]["job"]["job_info"]


def test_batch_skips_workflow_job_but_retries_others(retry_module):
    es = retry_module.mozart_es
    tagged, plain = _tagged_doc(), _failed_doc(job_id="other-job")
    es.search.side_effect = [tagged, plain]
    _seed_location(es, (DAILY, FAILED))
    es.delete_by_id.side_effect = _delete_returns({DAILY: NOT_FOUND, FAILED: DELETED})
    retry_module.resubmit_jobs(_context(retry_job_id=["send_notify_msg-job_worker-large", "other-job"]))
    assert retry_module.run_job.apply_async.call_count == 1
    msg = " ".join(map(str, retry_module.create_info_message_files.call_args.kwargs.values()))
    assert "1 skipped" in msg or "Skipped before any delete (1)" in msg


def test_hysds_io_and_job_spec_pass_workflow_retry_through():
    import os
    d = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "docker")
    io = json.load(open(os.path.join(d, "hysds-io.json.lw-mozart-retry")))
    spec = json.load(open(os.path.join(d, "job-spec.json.lw-mozart-retry")))
    wr = [p for p in io["params"] if p["name"] == "workflow_retry"]
    assert len(wr) == 1 and wr[0]["optional"] is True and wr[0]["type"] == "boolean"
    assert {"name": "workflow_retry", "destination": "context"} in spec["params"]
    # every names the consumer sends exists in both
    for name in ("retry_job_id", "retry_count_max", "job_priority_increment", "workflow_retry"):
        assert any(p["name"] == name for p in io["params"]) and any(p["name"] == name for p in spec["params"])
    assert spec["required_queues"] == ["system-jobs-queue"]
