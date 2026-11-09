"""purge.py must delete every dataset its query matches, or fail.

The defect these tests lock down: the purge query was paged without a sort,
so pages could overlap and skip documents. The duplicates came back from the
bulk delete as not_found, the skipped documents were never deleted, and the
job still exited 0. On one multi-page purge 3 to 10 percent of the matches
survived.
"""
import json
import os
from unittest.mock import MagicMock

import pytest

ALIAS = "grq"
MONTHLY = "grq_v1.0_l0a_l_rrst-2026.11"
MATCH_ALL = {"query": {"match_all": {}}}
START = "2026-11-09T12:00:00.000000Z"
BEFORE_START = "2026-11-09T11:00:00.000000Z"
AFTER_START = "2026-11-09T12:00:01.000000Z"


def _doc(_id, timestamp=BEFORE_START):
    source = {
        "dataset": "L0A_L_RRST",
        "urls": [f"https://example.com/{_id}", f"s3://bucket/products/{_id}"],
    }
    if timestamp is not None:
        source["@timestamp"] = timestamp
    return {"_index": MONTHLY, "_id": _id, "_source": source}


def _find_cutoff(node):
    """The `lte` bound of the @timestamp range in a query body, if any."""
    if isinstance(node, dict):
        bound = node.get("range", {}).get("@timestamp", {}).get("lte")
        if bound is not None:
            return bound
        node = list(node.values())
    if isinstance(node, list):
        for child in node:
            bound = _find_cutoff(child)
            if bound is not None:
                return bound
    return None


class FakeGrq:
    """A GRQ alias with a document store, a query and the bulk API.

    `query_plan` scripts what each query call gets wrong, in order: a dict with
    `skip` (ids left out, as a page gap would) and `repeat` (ids returned
    twice, as a page overlap would). `undeletable` ids answer every delete
    with an error, as a write-blocked index would. `on_bulk`, if set, runs
    before each bulk request is applied, to change the store underneath it.
    The query honors the purge's ingest cutoff the way OpenSearch would.
    """

    def __init__(self, docs, query_plan=(), undeletable=()):
        self.store = {}
        for doc in docs:
            self.add(doc)
        self.query_plan = list(query_plan)
        self.undeletable = set(undeletable)
        self.on_bulk = None
        self.queries = []
        self.es = MagicMock(name="client")
        self.es.bulk.side_effect = self._bulk

    def add(self, doc):
        if isinstance(doc, str):
            doc = _doc(doc)
        self.store[(doc["_index"], doc["_id"])] = doc

    def query(self, index, body, _source):
        assert index == ALIAS
        self.queries.append(body)
        cutoff = _find_cutoff(body.get("query"))
        plan = self.query_plan.pop(0) if self.query_plan else {}
        hits = []
        for (_index, _id), doc in sorted(self.store.items()):
            stamp = doc["_source"].get("@timestamp")
            if cutoff is not None and stamp is not None and stamp > cutoff:
                continue
            if _id in plan.get("skip", ()):
                continue
            hits.append(doc)
            if _id in plan.get("repeat", ()):
                hits.append(doc)
        return hits

    def _bulk(self, index, body, filter_path=None, refresh=None):
        if self.on_bulk:
            self.on_bulk(self)
        items = []
        for action in body:
            target = action["delete"]
            key = (target["_index"], target["_id"])
            if target["_id"] in self.undeletable:
                items.append({"delete": {**target, "error": {"reason": "blocked"}}})
            elif self.store.pop(key, None) is not None:
                items.append({"delete": {**target, "result": "deleted"}})
            else:
                items.append({"delete": {**target, "result": "not_found"}})
        return {"items": items}


@pytest.fixture()
def purge(purge_module, monkeypatch):
    monkeypatch.setattr(purge_module, "utc_now_iso", lambda: START)
    return purge_module


def _purge(purge, grq, delete_from_obj_store=True, query=MATCH_ALL):
    purge.get_grq_es = lambda: grq
    purge.purge_products(query, "tosca", "purge", delete_from_obj_store)


def _purged_files(purge):
    return sorted(c.args[0] for c in purge.osaka.main.rmall.call_args_list)


def _files(*ids):
    return [f"s3://bucket/products/{i}" for i in ids]


def _details():
    with open("_alt_msg_details.txt") as f:
        return f.read()


def _section(details, title):
    """The JSON list that follows a message-details section title."""
    after = details.split(title, 1)[1]
    return json.loads(after.split(":\n", 1)[1].split("\n", 1)[0])


def test_dataset_query_has_a_total_order_and_keeps_the_callers_query(purge):
    grq = FakeGrq(["a"])
    caller_query = {"query": {"term": {"dataset.keyword": "L0A_L_RRST"}},
                    "sort": [{"_score": "desc"}]}

    _purge(purge, grq, query=caller_query)

    sent = grq.queries[0]
    assert sent["sort"] == purge.DATASET_SORT
    assert [list(s)[0] for s in sent["sort"]] == ["@timestamp", "id.keyword"]
    assert caller_query["query"] in sent["query"]["bool"]["filter"]
    assert _find_cutoff(sent["query"]) == START
    # the caller's query is left as it was
    assert caller_query == {"query": {"term": {"dataset.keyword": "L0A_L_RRST"}},
                            "sort": [{"_score": "desc"}]}


def test_documents_a_page_skipped_are_deleted_on_the_next_pass(purge):
    grq = FakeGrq(["a", "b", "c", "d"], query_plan=[{"skip": {"c", "d"}}])

    _purge(purge, grq)

    assert grq.store == {}
    assert grq.es.bulk.call_count == 2
    # pass 1, pass 2, then the query that finds nothing left
    assert len(grq.queries) == 3
    assert _purged_files(purge) == _files("a", "b", "c", "d")
    details = _details()
    assert f"{MONTHLY} - 4" in details
    assert "failed to purge" not in details


def test_datasets_ingested_after_the_purge_started_are_kept(purge):
    grq = FakeGrq(["a", "b", "c"], query_plan=[{"skip": {"c"}}])
    # a new matching dataset lands while pass 1 is deleting
    grq.on_bulk = lambda g: g.add(_doc("new", timestamp=AFTER_START))

    _purge(purge, grq)

    assert list(grq.store) == [(MONTHLY, "new")]
    assert _purged_files(purge) == _files("a", "b", "c")


def test_datasets_without_a_timestamp_are_still_purged(purge):
    grq = FakeGrq([_doc("a"), _doc("legacy", timestamp=None)])

    _purge(purge, grq)

    assert grq.store == {}


def test_repeated_hits_are_deleted_once(purge):
    grq = FakeGrq(["a", "b"], query_plan=[{"repeat": {"a"}}])

    _purge(purge, grq)

    body = grq.es.bulk.call_args.kwargs["body"]
    assert [action["delete"]["_id"] for action in body] == ["a", "b"]
    assert _purged_files(purge) == _files("a", "b")
    assert "already deleted" not in _details()


def test_bulk_delete_waits_for_refresh(purge):
    grq = FakeGrq(["a"])

    _purge(purge, grq)

    assert grq.es.bulk.call_args.kwargs["refresh"] == "wait_for"


def test_not_found_is_treated_as_already_deleted(purge):
    grq = FakeGrq(["a", "b"])
    # something else deletes b between the query and the bulk delete
    grq.on_bulk = lambda g: g.store.pop((MONTHLY, "b"), None)

    _purge(purge, grq)

    details = _details()
    assert "failed to purge" not in details
    assert f"Datasets purged from ES:\n{MONTHLY} - 1" in details
    assert f"Datasets already deleted from ES by something else:\n{MONTHLY} - 1" in details
    # b's catalog entry is gone, so its files must go too
    assert _purged_files(purge) == _files("a", "b")


def test_undeletable_documents_fail_the_job_and_keep_their_files(purge):
    grq = FakeGrq(["a", "b"], undeletable={"b"})

    with pytest.raises(RuntimeError, match="1 dataset"):
        _purge(purge, grq)

    assert list(grq.store) == [(MONTHLY, "b")]
    assert grq.es.bulk.call_count == purge.MAX_PURGE_PASSES
    # b's document is still there, so its files must be too
    assert _purged_files(purge) == _files("a")
    details = _details()
    remaining = _section(details, "still match the query after 3 delete passes")
    assert remaining == [{"_index": MONTHLY, "_id": "b"}]
    # the failure is listed once, not once per pass
    assert details.count('"blocked"') == 1


def test_a_failure_cleared_by_a_later_pass_is_not_reported(purge):
    grq = FakeGrq(["a", "b"], undeletable={"b"})
    real_bulk = grq._bulk

    def unblock_after_first(*args, **kwargs):
        res = real_bulk(*args, **kwargs)
        grq.undeletable.clear()
        return res
    grq.es.bulk.side_effect = unblock_after_first

    _purge(purge, grq)

    assert grq.store == {}
    assert "failed to purge" not in _details()


def test_object_store_failures_are_listed_and_fail_the_job(purge):
    grq = FakeGrq(["a", "b", "c"])

    def rmall(url):
        if url.endswith("/b"):
            raise OSError("AccessDenied")
    purge.osaka.main.rmall.side_effect = rmall

    with pytest.raises(RuntimeError, match="1 deleted dataset.*left files"):
        _purge(purge, grq)

    # the failure did not stop the other datasets' files from being removed
    assert _purged_files(purge) == _files("a", "b", "c")
    assert grq.store == {}
    left = _section(_details(), "whose files could not be removed from the object store")
    assert left == [{"_index": MONTHLY, "_id": "b", "url": "s3://bucket/products/b",
                     "error": "OSError: AccessDenied"}]


def test_files_are_kept_when_object_store_purge_is_off(purge):
    grq = FakeGrq(["a", "b"])

    _purge(purge, grq, delete_from_obj_store=False)

    assert grq.store == {}
    purge.osaka.main.rmall.assert_not_called()


def test_no_matches_is_a_clean_no_op(purge):
    grq = FakeGrq([])

    _purge(purge, grq)

    grq.es.bulk.assert_not_called()
    assert not os.path.exists("_alt_msg_details.txt")


def test_job_purge_ends_its_sort_on_payload_id_and_visits_each_job_once(purge):
    mozart = MagicMock(name="mozart_es")
    job = {"_index": "job_status-2026.11.05", "_id": "job-1",
           "_source": {"uuid": "task-1", "payload_id": "job-1"}}
    mozart.query.return_value = [job, job]
    purge.get_mozart_es = lambda: mozart
    purge.app.AsyncResult = MagicMock(return_value=MagicMock(state="SUCCESS"))
    purge.bulk_delete_by_id = MagicMock(return_value=["job_status-2026.11.05"])

    purge.purge_products(MATCH_ALL, "mozart", "purge")

    sent = mozart.query.call_args.kwargs["body"]
    assert sent["sort"] == purge.JOB_SORT
    assert [list(s)[0] for s in sent["sort"]] == ["@timestamp", "payload_id"]
    # no ingest cutoff on the job path: it makes a single pass
    assert sent["query"] == MATCH_ALL["query"]
    purge.bulk_delete_by_id.assert_called_once_with(
        mozart, "job_status-current", "job-1")
