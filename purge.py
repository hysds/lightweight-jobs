#!/bin/env python
import json
import logging

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

# A total order for the purge query. Below 10,000 hits SearchUtility.query()
# pages with from/size, and without a total order consecutive pages can
# overlap and skip documents: hits that tie on score are not guaranteed to
# come back in the same order on every page request (a different shard copy
# can answer each one). The overlap shows up as not_found on the duplicate
# deletes; the skipped documents are never deleted. The job iterator sorts on
# the same two fields. unmapped_type keeps an alias member that lacks a field
# from failing its shard, which would drop that member's hits from the result.
QUERY_SORT = [
    {"@timestamp": {"order": "desc", "unmapped_type": "date"}},
    {"id.keyword": {"order": "asc", "unmapped_type": "keyword"}},
]

# Delete passes a dataset purge makes before it fails on documents that still
# match its query.
MAX_PURGE_PASSES = 3

# Most leftover documents listed in the job's message details.
MAX_LISTED_REMAINING = 100

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


def delete_from_object_store(es_result):
    _id = es_result["_id"]
    dataset = es_result["_source"]["dataset"]

    best = None  # find the Best URL first
    for url in es_result["_source"]["urls"]:
        if not url.startswith("http"):
            best = url

    if best is not None:
        print('paramater being passed to osaka.main.rmall:', best)  # making osaka call to delete product
        osaka.main.rmall(best)
        logger.info('Purged from object store %s' % _id)
    else:
        logger.warning("url not found for: %s" % _id)
    return dataset


def sorted_query(query):
    """Return a copy of the query that sorts on QUERY_SORT.

    A sort the caller supplied is replaced: a purge visits every match, so
    order matters only for paging, and a caller's sort may have no unique
    tiebreaker.
    """
    body = dict(query)
    body["sort"] = QUERY_SORT
    return body


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


def purge_datasets(es, es_index, query, _source, delete_from_obj_store=True):
    """Delete every dataset the query matches, from ES and the object store.

    After each delete pass the query is run again, and whatever it still
    matches is deleted in another pass, up to MAX_PURGE_PASSES. Documents
    that still match after the last pass fail the job instead of letting it
    exit 0 with datasets left behind.

    A dataset's files are removed from the object store only once its
    document has been deleted, so a document that could not be deleted keeps
    its files, and the next pass, or a rerun, retries both.
    """
    dataset_purge_stats = {}
    deleted_docs_count = 0
    failed_deletions = {}  # (_index, _id) -> the latest failed bulk item
    remaining = []

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

        body = [{
            "delete": {"_index": hit["_index"], "_id": hit["_id"]}
        } for hit in hits]
        # wait_for: the deletes must be visible to the next pass's query, or
        # it would find them again and delete them a second time
        bulk_res = es.es.bulk(index=es_index, body=body, filter_path=BULK_FILTER_PATH,
                              refresh="wait_for")
        logger.info(json.dumps(bulk_res, indent=2))

        hits_by_key = {(hit["_index"], hit["_id"]): hit for hit in hits}
        deleted_hits = []
        for row in bulk_res["items"]:
            item = row["delete"]
            key = (item.get("_index"), item.get("_id"))
            if item.get("result") == "deleted":
                deleted_docs_count += 1
                logger.info("deleted from ES: %s" % item["_id"])
                dataset_purge_stats[key[0]] = dataset_purge_stats.get(key[0], 0) + 1
                failed_deletions.pop(key, None)
                if key in hits_by_key:
                    deleted_hits.append(hits_by_key[key])
            else:
                failed_deletions[key] = item

        if delete_from_obj_store is not True:
            logger.info("skip purging datasets from object store. delete_from_object_store=False")
        elif deleted_hits:
            logger.info("purging datasets from object store: ")
            with Pool(processes=psutil.cpu_count() - 2) as p:
                p.map(delete_from_object_store, deleted_hits)  # deleting objects from storage (s3, etc.)

    msg_details = ""
    if deleted_docs_count > 0:
        msg_details += "Datasets purged from ES:\n"
        for k, v in dataset_purge_stats.items():
            msg_details += f"{k} - {v}\n"
    if failed_deletions:
        msg_details += "\n\n"
        msg_details += "Datasets failed to purge from ES:\n"
        msg_details += json.dumps(list(failed_deletions.values()))
        logger.warning("datasets failed to delete: ")
        logger.warning(json.dumps(list(failed_deletions.values())))
    if remaining:
        listed = [{"_index": hit["_index"], "_id": hit["_id"]}
                  for hit in remaining[:MAX_LISTED_REMAINING]]
        msg_details += "\n\n"
        msg_details += (f"{len(remaining)} dataset(s) still match the query after "
                        f"{MAX_PURGE_PASSES} delete passes")
        if len(remaining) > len(listed):
            msg_details += f" (first {len(listed)} listed)"
        msg_details += ":\n" + json.dumps(listed)
    if msg_details:
        create_info_message_files(msg_details=msg_details)
    else:
        logger.info("No datasets matched the query; nothing to purge.")

    if remaining:
        raise RuntimeError(f"{len(remaining)} dataset(s) still match the purge query after "
                           f"{MAX_PURGE_PASSES} delete passes")


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

    query = sorted_query(query)

    if component == 'tosca':
        purge_datasets(es, es_index, query, _source, delete_from_obj_store)
    else:
        results = query_unique(es, es_index, query, _source)  # Querying for jobs
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
