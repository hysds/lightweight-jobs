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


def _doc(_id, index=MONTHLY):
    return {
        "_index": index,
        "_id": _id,
        "_source": {
            "dataset": "L0A_L_RRST",
            "urls": [f"https://example.com/{_id}", f"s3://bucket/products/{_id}"],
        },
    }


class FakeGrq:
    """A GRQ alias with a document store, a query and the bulk API.

    `query_plan` scripts what each query call gets wrong, in order: a dict with
    `skip` (ids left out, as a page gap would) and `repeat` (ids returned
    twice, as a page overlap would). `undeletable` ids answer every delete
    with an error, as a write-blocked index would.
    """

    def __init__(self, ids, query_plan=(), undeletable=()):
        self.store = {(MONTHLY, i): _doc(i) for i in ids}
        self.query_plan = list(query_plan)
        self.undeletable = set(undeletable)
        self.queries = []
        self.es = MagicMock(name="client")
        self.es.bulk.side_effect = self._bulk

    def query(self, index, body, _source):
        assert index == ALIAS
        self.queries.append(body)
        plan = self.query_plan.pop(0) if self.query_plan else {}
        hits = []
        for (_index, _id), doc in sorted(self.store.items()):
            if _id in plan.get("skip", ()):
                continue
            hits.append(doc)
            if _id in plan.get("repeat", ()):
                hits.append(doc)
        return hits

    def _bulk(self, index, body, filter_path=None, refresh=None):
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


def _purge(purge, grq, delete_from_obj_store=True, query=MATCH_ALL):
    purge.get_grq_es = lambda: grq
    purge.purge_products(query, "tosca", "purge", delete_from_obj_store)


def _purged_files(purge):
    return sorted(c.args[0] for c in purge.osaka.main.rmall.call_args_list)


def _details():
    with open("_alt_msg_details.txt") as f:
        return f.read()


def test_query_has_a_total_order_with_a_unique_tiebreaker(purge_module):
    grq = FakeGrq(["a"])
    caller_query = {"query": {"term": {"dataset.keyword": "L0A_L_RRST"}},
                    "sort": [{"_score": "desc"}]}

    _purge(purge_module, grq, query=caller_query)

    sent = grq.queries[0]
    assert sent["sort"] == purge_module.QUERY_SORT
    assert [list(s)[0] for s in sent["sort"]] == ["@timestamp", "id.keyword"]
    assert sent["query"] == caller_query["query"]
    # the caller's query is left as it was
    assert caller_query["sort"] == [{"_score": "desc"}]


def test_documents_a_page_skipped_are_deleted_on_the_next_pass(purge_module):
    grq = FakeGrq(["a", "b", "c", "d"], query_plan=[{"skip": {"c", "d"}}])

    _purge(purge_module, grq)

    assert grq.store == {}
    assert grq.es.bulk.call_count == 2
    # pass 1, pass 2, then the query that finds nothing left
    assert len(grq.queries) == 3
    assert _purged_files(purge_module) == [f"s3://bucket/products/{i}" for i in "abcd"]
    details = _details()
    assert f"{MONTHLY} - 4" in details
    assert "failed to purge" not in details


def test_repeated_hits_are_deleted_once(purge_module):
    grq = FakeGrq(["a", "b"], query_plan=[{"repeat": {"a"}}])

    _purge(purge_module, grq)

    body = grq.es.bulk.call_args.kwargs["body"]
    assert [action["delete"]["_id"] for action in body] == ["a", "b"]
    assert _purged_files(purge_module) == ["s3://bucket/products/a",
                                           "s3://bucket/products/b"]
    assert "not_found" not in _details()


def test_bulk_delete_waits_for_refresh(purge_module):
    grq = FakeGrq(["a"])

    _purge(purge_module, grq)

    assert grq.es.bulk.call_args.kwargs["refresh"] == "wait_for"


def test_undeletable_documents_fail_the_job_and_keep_their_files(purge_module):
    grq = FakeGrq(["a", "b"], undeletable={"b"})

    with pytest.raises(RuntimeError, match="1 dataset"):
        _purge(purge_module, grq)

    assert list(grq.store) == [(MONTHLY, "b")]
    assert grq.es.bulk.call_count == purge_module.MAX_PURGE_PASSES
    # b's document is still there, so its files must be too
    assert _purged_files(purge_module) == ["s3://bucket/products/a"]
    details = _details()
    assert "still match the query after 3 delete passes" in details
    remaining = json.loads(details.rsplit(":\n", 1)[1])
    assert remaining == [{"_index": MONTHLY, "_id": "b"}]
    # the failure is listed once, not once per pass
    assert details.count('"blocked"') == 1


def test_a_failure_cleared_by_a_later_pass_is_not_reported(purge_module):
    grq = FakeGrq(["a", "b"], undeletable={"b"})
    real_bulk = grq._bulk

    def unblock_after_first(*args, **kwargs):
        res = real_bulk(*args, **kwargs)
        grq.undeletable.clear()
        return res
    grq.es.bulk.side_effect = unblock_after_first

    _purge(purge_module, grq)

    assert grq.store == {}
    assert "failed to purge" not in _details()


def test_files_are_kept_when_object_store_purge_is_off(purge_module):
    grq = FakeGrq(["a", "b"])

    _purge(purge_module, grq, delete_from_obj_store=False)

    assert grq.store == {}
    purge_module.osaka.main.rmall.assert_not_called()


def test_no_matches_is_a_clean_no_op(purge_module):
    grq = FakeGrq([])

    _purge(purge_module, grq)

    grq.es.bulk.assert_not_called()
    assert not os.path.exists("_alt_msg_details.txt")


def test_job_purge_sorts_and_visits_each_job_once(purge_module):
    mozart = MagicMock(name="mozart_es")
    job = {"_index": "job_status-2026.11.05", "_id": "job-1",
           "_source": {"uuid": "task-1", "payload_id": "job-1"}}
    mozart.query.return_value = [job, job]
    purge_module.get_mozart_es = lambda: mozart
    purge_module.app.AsyncResult = MagicMock(return_value=MagicMock(state="SUCCESS"))
    purge_module.bulk_delete_by_id = MagicMock(return_value=["job_status-2026.11.05"])

    purge_module.purge_products(MATCH_ALL, "mozart", "purge")

    assert mozart.query.call_args.kwargs["body"]["sort"] == purge_module.QUERY_SORT
    purge_module.bulk_delete_by_id.assert_called_once_with(
        mozart, "job_status-current", "job-1")
