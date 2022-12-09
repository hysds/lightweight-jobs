#!/bin/env python
import json
import logging

import psutil
from multiprocessing import Pool

import osaka.main
from hysds.celery import app
from hysds.es_util import get_mozart_es, get_grq_es
from utils import revoke, create_info_message_files

LOG_FILE_NAME = 'purge.log'
logging.basicConfig(filename=LOG_FILE_NAME, filemode='a', level=logging.INFO)
logger = logging

tosca_es = get_grq_es()


def read_context():
    with open('_context.json', 'r') as f:
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
        es_index = app.conf["STATUS_ALIAS"]
        _source = ["uuid", "payload_id"]
    else:  # "tosca"
        es = get_grq_es()
        es_index = app.conf["DATASET_ALIAS"]
        _source = ["dataset", "urls"]

    results = es.query(index=es_index, body=query, _source=_source)  # Querying for products

    # filter fields returned with the bulk API
    filter_path = [
        "items.delete.error.reason",
        "items.delete._id",
        "items.delete._index",
        "items.delete.result"
    ]

    if component == 'tosca':
        num_processes = psutil.cpu_count() - 2
        p = Pool(processes=num_processes)

        body = [{
            "delete": {"_index": row["_index"], "_id": row["_id"]}
        } for row in results]
        bulk_res = es.es.bulk(index=es_index, body=body, filter_path=filter_path)
        logger.info(json.dumps(bulk_res, indent=2))

        if delete_from_obj_store is True:
            logger.info("purging datasets from object store: ")
            p.map(delete_from_object_store, results)  # deleting objects from storage (s3, etc.)
        else:
            logger.info("skip purging datasets from object store. delete_from_object_store=False")

        dataset_purge_stats = {}
        deleted_docs_count = 0
        failed_deletions = []
        for row in bulk_res["items"]:
            if row["delete"].get("result", None) == "deleted":
                deleted_docs_count += 1
                _index = row["delete"]["_index"]
                logger.info("deleted from ES: %s" % row["delete"]["_id"])
                if _index not in dataset_purge_stats:
                    dataset_purge_stats[_index] = 1
                else:
                    dataset_purge_stats[_index] += 1
            else:
                failed_deletions.append(row["delete"])

        if deleted_docs_count or failed_deletions:
            msg_details = ""
            if deleted_docs_count > 0:
                msg_details += "Datasets purged from ES:\n"
                for k, v in dataset_purge_stats.items():
                    msg_details += "{} - {}\n".format(k, v)
            if len(failed_deletions) > 0:
                msg_details += "\n\n"
                msg_details += "Datasets failed to purge from ES:\n"
                msg_details += json.dumps(failed_deletions)
                logger.warning("datasets failed to delete: ")
                logger.warning(json.dumps(failed_deletions))
            create_info_message_files(msg_details=msg_details)
    else:
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

            # Both associated task and job from ES
            logger.info('Removing document from index %s for %s', index, payload_id)
            es.delete_by_id(index=index, id=payload_id, ignore=404)
            logger.info('Removed %s from index: %s', payload_id, index)
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
