#!/bin/env python
import json
import logging
from datetime import datetime, timezone

import psutil
from multiprocessing import Pool

import osaka.main
from hysds.celery import app
from hysds.es_util import get_mozart_es, get_grq_es
from utils import bulk_delete_by_id, create_info_message_files, revoke

LOG_FILE_NAME = 'purge.log'
log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, filename=LOG_FILE_NAME, filemode='a', level=logging.INFO)
logger = logging

tosca_es = get_grq_es()
mozart_es = get_mozart_es()

# Total orders for the purge queries. Below 10,000 hits SearchUtility.query()
# pages with from/size, and without a total order consecutive pages can
# overlap and skip documents: hits that tie on score are not guaranteed to
# come back in the same order on every page request (a different shard copy
# can answer each one). The overlap shows up as not_found on the duplicate
# deletes; the skipped documents are never deleted. Each order ends on a
# field that is unique per document: a dataset's id, and a job's payload_id
# (job_status docs have no top-level id; payload_id is also their _id, and it
# is mapped as keyword, not as text with a .keyword subfield). unmapped_type
# keeps an alias member that lacks a field from failing its shard, which
# would drop that member's hits from the result.
DATASET_SORT = [
    {"@timestamp": {"order": "desc", "unmapped_type": "date"}},
    {"id.keyword": {"order": "asc", "unmapped_type": "keyword"}},
]
JOB_SORT = [
    {"@timestamp": {"order": "desc", "unmapped_type": "date"}},
    {"payload_id": {"order": "asc", "unmapped_type": "keyword"}},
]

# Delete passes a dataset purge makes before it fails on documents that still
# match its query.
MAX_PURGE_PASSES = 3

# Most documents listed per section of the job's message details.
MAX_LISTED = 100

# Delete actions per bulk request. Keeps each request well under the request
# size limit (10 MB on smaller AWS OpenSearch instance types, roughly 60,000
# deletes) and the client timeout, and keeps each refresh=wait_for short.
BULK_BATCH_SIZE = 2000

# filter fields returned with the bulk API
BULK_FILTER_PATH = [
    "items.delete.error.reason",
    "items.delete._id",
    "items.delete._index",
    "items.delete.result"
]


def read_context():
    with open('_context.json') as f:
        cxt = json.load(f)
        return cxt


def object_store_url(es_result):
    """The dataset's object-store URL (its last non-http URL), or None."""
    best = None
    for url in es_result["_source"].get("urls", []):
        if not url.startswith("http"):
            best = url
    return best


def delete_from_object_store(es_result):
    """Remove a dataset's files from the object store.

    Returns None when the files were removed, or when the dataset lists no
    object-store URL, and a record of the failure otherwise. Errors are
    caught here, per dataset, because the dataset's document has already
    been deleted: an exception escaping Pool.map would abort the job before
    it reports which files it left behind with no catalog entry.
    """
    _id = es_result["_id"]
    best = object_store_url(es_result)
    if best is None:
        logger.warning("url not found for: %s" % _id)
        return None

    print('paramater being passed to osaka.main.rmall:', best)  # making osaka call to delete product
    try:
        osaka.main.rmall(best)
    except Exception as e:
        logger.error("Could not purge %s from object store at %s: %s", _id, best, e)
        return {"_index": es_result["_index"], "_id": _id, "url": best,
                "error": f"{type(e).__name__}: {e}"}
    logger.info('Purged from object store %s' % _id)
    return None


def utc_now_iso():
    """The current UTC time, in the ISO 8601 form @timestamp uses."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def sorted_query(query, sort):
    """Return a copy of the query that sorts on `sort`.

    A sort the caller supplied is replaced: a purge visits every match, so
    order matters only for paging, and a caller's sort may have no unique
    tiebreaker.
    """
    body = dict(query)
    body["sort"] = sort
    return body


def ingested_before(query, cutoff):
    """Return a copy of a dataset query limited to datasets ingested by `cutoff`.

    GRQ's ingest pipeline sets @timestamp to the ingest time, so this keeps a
    later delete pass from deleting datasets ingested after the purge
    started, which the user never selected. A document without @timestamp
    was indexed outside the pipeline and can't be dated, so it still matches.
    """
    body = dict(query)
    body["query"] = {"bool": {"filter": [
        query.get("query", {"match_all": {}}),
        {"bool": {"minimum_should_match": 1, "should": [
            {"range": {"@timestamp": {"lte": cutoff}}},
            {"bool": {"must_not": [{"exists": {"field": "@timestamp"}}]}},
        ]}},
    ]}}
    return body


def delete_batch(es, es_index, batch):
    """Bulk-delete a batch of hits and return each one's outcome.

    Returns {(_index, _id): item}. An item whose "result" is "deleted" or
    "not_found" means the document is gone; any other item carries an
    "error". If the bulk request itself fails (rejected as too large, or timed
    out while the server may still be deleting), each document is looked up
    by id: one the failed request did delete is still reported gone, so its
    files are removed, and one still present gets an error and is retried on
    the next pass. If the lookup fails too, the outcome is unknown and the
    item is marked "unknown".
    """
    body = [{"delete": {"_index": hit["_index"], "_id": hit["_id"]}} for hit in batch]
    try:
        # wait_for: the deletes must be visible to the next pass's query, or
        # it would find them again and delete them a second time
        bulk_res = es.es.bulk(index=es_index, body=body, filter_path=BULK_FILTER_PATH,
                              refresh="wait_for")
        logger.info(json.dumps(bulk_res, indent=2))
        return {(row["delete"].get("_index"), row["delete"].get("_id")): row["delete"]
                for row in bulk_res["items"]}
    except Exception as e:
        bulk_error = f"{type(e).__name__}: {e}"
        logger.error("bulk delete of %d document(s) failed: %s", len(batch), bulk_error)

    keys = [(hit["_index"], hit["_id"]) for hit in batch]
    try:
        found = es.es.mget(body={"docs": [{"_index": i, "_id": d} for i, d in keys]},
                           _source=False)
        outcomes = {}
        for doc in found["docs"]:
            key = (doc.get("_index"), doc.get("_id"))
            if doc.get("found") is False:
                outcomes[key] = {"_index": key[0], "_id": key[1], "result": "not_found"}
            else:
                outcomes[key] = {"_index": key[0], "_id": key[1],
                                 "error": {"reason": f"bulk request failed: {bulk_error}"}}
        return outcomes
    except Exception as e:
        lookup_error = f"{type(e).__name__}: {e}"
        logger.error("lookup after the failed bulk delete failed too: %s", lookup_error)
        return {key: {"_index": key[0], "_id": key[1], "unknown": True,
                      "error": {"reason": f"bulk request failed: {bulk_error}; "
                                          f"lookup failed: {lookup_error}"}}
                for key in keys}


def query_unique(es, index, query, _source):
    """Run the query and drop repeated hits (same _index and _id)."""
    seen = set()
    hits = []
    for hit in es.query(index=index, body=query, _source=_source):
        key = (hit["_index"], hit["_id"])
        if key not in seen:
            seen.add(key)
            hits.append(hit)
    return hits


def _listing(title, records):
    """A message-details section listing up to MAX_LISTED records."""
    listed = records[:MAX_LISTED]
    text = f"\n\n{title}"
    if len(records) > len(listed):
        text += f" (first {len(listed)} of {len(records)} listed)"
    return text + ":\n" + json.dumps(listed)


def _counts(title, counts):
    """A message-details section of per-index document counts."""
    return f"{title}:\n" + "".join(f"{k} - {v}\n" for k, v in counts.items())


def purge_datasets(es, es_index, query, _source, delete_from_obj_store=True):
    """Delete every dataset the query matches, from ES and the object store.

    Only datasets ingested before the purge started are deleted (see
    ingested_before). Each pass deletes in bulk requests of BULK_BATCH_SIZE,
    removing each batch's files right after its documents are gone. After
    each pass the query is run again, and whatever it still matches is
    deleted in another pass, up to MAX_PURGE_PASSES. The job fails, instead
    of exiting 0 with something left behind, when datasets still match after
    the last pass, when a deleted dataset's files could not be removed from
    the object store, or when a delete's outcome is unknown.

    A dataset's files are removed only once its document is gone, so a
    document that could not be deleted keeps its files, and the next pass, or
    a rerun, retries both. A delete that answers not_found means something
    else deleted the document after this pass's query; it is treated as
    deleted, and its files are removed too.
    """
    cutoff = utc_now_iso()
    logger.info("purging datasets ingested at or before %s", cutoff)
    query = ingested_before(query, cutoff)

    purged = {}            # _index -> documents this job deleted
    already_gone = {}      # _index -> documents found already deleted
    failed_deletions = {}  # (_index, _id) -> the latest failed delete item
    file_failures = []     # deleted datasets whose files could not be removed
    remaining = []

    pool = None
    if delete_from_obj_store is True:
        # Started before anything is deleted, so a host that can't start the
        # pool fails the job while every dataset still has its document.
        pool = Pool(processes=max(1, (psutil.cpu_count() or 1) - 2))
    else:
        logger.info("skip purging datasets from object store. delete_from_object_store=False")
    try:
        for pass_num in range(1, MAX_PURGE_PASSES + 2):
            hits = query_unique(es, es_index, query, _source)
            if not hits:
                break
            if pass_num > MAX_PURGE_PASSES:
                remaining = hits
                break
            if pass_num > 1:
                logger.warning("%d document(s) still match the query; delete pass %d of %d",
                               len(hits), pass_num, MAX_PURGE_PASSES)

            for start in range(0, len(hits), BULK_BATCH_SIZE):
                batch = hits[start:start + BULK_BATCH_SIZE]
                outcomes = delete_batch(es, es_index, batch)
                gone_hits = []
                for hit in batch:
                    key = (hit["_index"], hit["_id"])
                    item = outcomes.get(key) or {
                        "_index": key[0], "_id": key[1],
                        "error": {"reason": "no result for this document in the bulk response"}}
                    result = item.get("result")
                    if result in ("deleted", "not_found"):
                        counts = purged if result == "deleted" else already_gone
                        counts[key[0]] = counts.get(key[0], 0) + 1
                        logger.info("%s from ES: %s",
                                    "deleted" if result == "deleted" else "already deleted", key[1])
                        failed_deletions.pop(key, None)
                        gone_hits.append(hit)
                    else:
                        if item.get("unknown"):
                            item = {**item, "url": object_store_url(hit)}
                        failed_deletions[key] = item

                if pool is not None and gone_hits:
                    logger.info("purging datasets from object store: ")
                    results = pool.map(delete_from_object_store, gone_hits)  # deleting objects from storage (s3, etc.)
                    file_failures.extend(r for r in results if r)
    finally:
        if pool is not None:
            pool.close()
            pool.join()

    unknown = [item for item in failed_deletions.values() if item.get("unknown")]
    msg_details = ""
    if purged:
        msg_details += _counts("Datasets purged from ES", purged)
    if already_gone:
        msg_details += "\n" + _counts("Datasets found already deleted from ES", already_gone)
    if failed_deletions:
        failures = list(failed_deletions.values())
        msg_details += _listing("Datasets failed to purge from ES", failures)
        logger.warning("datasets failed to delete: ")
        logger.warning(json.dumps(failures))
    if remaining:
        msg_details += _listing(
            f"{len(remaining)} dataset(s) still match the query after {MAX_PURGE_PASSES} delete passes",
            [{"_index": hit["_index"], "_id": hit["_id"]} for hit in remaining])
    if file_failures:
        msg_details += _listing(
            f"{len(file_failures)} dataset(s) deleted from ES whose files could not be removed "
            "from the object store", file_failures)
        logger.warning("files left in the object store: ")
        logger.warning(json.dumps(file_failures))
    if msg_details:
        create_info_message_files(msg_details=msg_details.lstrip("\n"))
    else:
        logger.info("No datasets matched the query; nothing to purge.")

    problems = []
    if remaining:
        problems.append(f"{len(remaining)} dataset(s) still match the purge query after "
                        f"{MAX_PURGE_PASSES} delete passes")
    if file_failures:
        problems.append(f"{len(file_failures)} deleted dataset(s) left files in the object store")
    if unknown:
        problems.append(f"{len(unknown)} dataset(s) have an unknown delete outcome; check their "
                        "documents and files")
    if problems:
        raise RuntimeError("; ".join(problems))


def purge_products(query, component, operation, delete_from_obj_store=True):
    """
    Iterator used to iterate across a query result and submit jobs for every hit
    :param query: query to post to ElasticSearch and whose result will be iterated, JSON sting enc
    :param component: tosca || figaro
    :param operation: purge or something else
    :param delete_from_obj_store: Flag to indicate whether to purge the dataset from the object store
    """
    logger.debug("action: %s for %s", operation, component)
    logger.debug("query: %s" % json.dumps(query, indent=2))

    if component == "mozart" or component == "figaro":
        es = get_mozart_es()
        es_index = "job_status-current"
        _source = ["uuid", "payload_id"]
    else:  # "tosca"
        es = get_grq_es()
        es_index = app.conf["DATASET_ALIAS"]
        _source = ["dataset", "urls"]

    if component == 'tosca':
        purge_datasets(es, es_index, sorted_query(query, DATASET_SORT), _source,
                       delete_from_obj_store)
    else:
        # Querying for jobs
        results = query_unique(es, es_index, sorted_query(query, JOB_SORT), _source)
        purge = True if operation == 'purge' else False  # purge job from index

        for result in results:
            uuid = result["_source"]['uuid']
            payload_id = result["_source"]['payload_id']
            index = result["_index"]

            # Always grab latest state (not state from query result)
            task = app.AsyncResult(uuid)
            state = task.state  # Active states may only revoke
            logger.info("\nJob state: %s\n", state)

            if state in ["RETRY", "STARTED"] or (state == "PENDING" and not purge):
                if not purge:
                    logger.info('Revoking %s\n', uuid)
                    revoke(uuid, state)
                else:
                    logger.info('Cannot remove active job %s\n', uuid)
                continue
            elif not purge:
                logger.info('Cannot stop inactive job: %s\n', uuid)
                continue

            # Safety net to revoke job if in PENDING state
            if state == "PENDING":
                logger.info('Revoking %s\n', uuid)
                revoke(uuid, state)

            # Delete job(s) from ES. This used to pick the delete target from
            # a near-realtime search's _index, which is stale while logstash is
            # moving a job-failed doc between members of the alias: the delete
            # then went to the index the doc had already left, the 404 was
            # swallowed, and success was logged while the doc survived. Unlike
            # the retry case there is no resubmit to mask it -- the job simply
            # could not be purged. Sweep every member instead.
            # Sweep the ALIAS (es_index), not result["_index"]: that is the
            # concrete index off the search hit, and handing it to the sweep
            # resolves to itself and degenerates to the old single delete.
            # One bulk request per job rather than one delete per member:
            # purge walks its selection serially, so the per-member cost is
            # multiplied by the size of the purge.
            try:
                deleted_from = bulk_delete_by_id(es, es_index, payload_id)
            except Exception as e:
                # the sweep raises only when nothing could be deleted; one
                # un-purgeable job must not abort the rest of the batch
                logger.error('Could not purge %s from %s: %s', payload_id, es_index, e)
                continue
            if deleted_from:
                logger.info('Removed %s from: %s', payload_id, ', '.join(deleted_from))
            else:
                logger.warning('No %s doc in any member of %s; nothing purged',
                               payload_id, es_index)
        logger.info('Finished.')


if __name__ == "__main__":
    """Main program of purge_products"""
    context = read_context()  # reading the context file

    component_val = context['component']
    operation_val = context['operation']
    delete_from_object_store_val = context.get("delete_from_object_store", True)

    query_obj = context['query']
    try:
        query_obj = json.loads(query_obj)
    except TypeError as e:
        logger.warning(e)

    purge_products(query_obj, component_val, operation_val, delete_from_object_store_val)
