"""Test harness for retry.py.

retry.py is a top-level script that, at import time, pulls in the hysds
runtime (`from hysds.celery import app` reads a celeryconfig that only exists
on a cluster), builds an OpenSearch client, and calls logging.basicConfig with
a file in the CWD. Everything it imports is therefore stubbed into sys.modules
BEFORE the import, and backoff is replaced with a passthrough so failure-path
tests do not sleep through a real exponential backoff.
"""
import os
import sys
import time
import types
from unittest.mock import MagicMock

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _stub(name, **attrs):
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod
    return mod


mozart_es_mock = MagicMock(name="mozart_es")

# The REAL backoff library is used, not a passthrough stub: retry.py's retry
# semantics are part of what these tests are checking, and a neutralised
# decorator asserts every failure path as if it happened on the first attempt.
# Only the waiting is removed, by stubbing time.sleep in the fixture below.
import backoff  # noqa: F401,E402

_stub("hysds")
_app = types.SimpleNamespace(
    conf={"STATUS_ALIAS": "job_status",
          "JOB_LOCK_HEARTBEAT_INTERVAL": 30,
          "JOB_LOCK_STALE_CHECK_RETRIES": 3,
          "JOB_LOCK_REDELIVERY_BUFFER_TIME": 10},
    AsyncResult=MagicMock(),
    control=MagicMock(),
)
# retry.py uses app.conf["STATUS_ALIAS"] and app.conf.get(...) - a dict serves both
_stub("hysds.celery", app=_app)
_stub("hysds.es_util", get_mozart_es=lambda: mozart_es_mock)
_stub("hysds.lock", JobLock=MagicMock())
_stub("hysds.orchestrator", run_job=MagicMock())
_stub("hysds.log_utils", log_job_status=MagicMock())
_stub("hysds.utils", datetime_iso_naive=lambda: "2026-01-01T00:00:00")
_stub("celery", uuid=lambda: "new-task-uuid")
# utils is NOT stubbed: retry.py's sweep lives there now, and stubbing the
# module would replace the code under test with a mock. Its own imports are
# satisfiable here (backoff, redis, and the stubbed hysds.celery), and the
# fixture rebinds the two side-effecting helpers per test.


@pytest.fixture()
def retry_module(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)          # retry.log lands here
    monkeypatch.setattr(time, "sleep", lambda *_a, **_k: None)  # real backoff, no waiting
    if REPO_ROOT not in sys.path:
        sys.path.insert(0, REPO_ROOT)
    sys.modules.pop("retry", None)       # fresh import per test
    import retry

    # rebind the names retry.py captured at import time to per-test mocks, so
    # call assertions never see another test's calls
    mozart_es_mock.reset_mock(return_value=True, side_effect=True)
    retry.mozart_es = mozart_es_mock
    retry.log_job_status = MagicMock(name="log_job_status")
    retry.run_job = MagicMock(name="run_job")
    retry.revoke = MagicMock(name="revoke")
    retry.create_info_message_files = MagicMock(name="create_info_message_files")
    retry.uuid = lambda: "new-task-uuid"
    yield retry
