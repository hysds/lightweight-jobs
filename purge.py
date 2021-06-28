#!/bin/env python
import json
import logging

import osaka.main
from hysds.celery import app
from hysds.es_util import get_mozart_es, get_grq_es
from utils import revoke, create_info_message_files


LOG_FILE_NAME = 'purge.log'
logging.basicConfig(filename=LOG_FILE_NAME, filemode='a', level=logging.DEBUG)
logger = logging


def read_context():
    with open('_context.json', 'r') as f:
        cxt = json.load(f)
        return cxt


def purge_products(query, component, operation):
    """
    Iterator used to iterate across a query result and submit jobs for every hit
    :param query: query to post to ElasticSearch and whose result will be iterated, JSON sting enc
    :param component: tosca || figaro
    :param operation: purge or something else
    """
    logger.debug("action: %s for %s", operation, component)
    logger.debug("query: %s" % json.dumps(query, indent=2))

    if component == "mozart" or component == "figaro":
        es = get_mozart_es()
        es_index = app.conf["STATUS_ALIAS"]
    else:  # "tosca"
        es = get_grq_es()
        es_index = app.conf["DATASET_ALIAS"]

    results = es.query(index=es_index, body=query)  # Querying for products
    if component == 'tosca':
        deleted_datasets = dict()
        for result in results:
            ident = result["_id"]
            index = result["_index"]
            dataset = result["_source"]["dataset"]
            # find the Best URL first
            best = None
            for url in result["_source"]["urls"]:
                if not url.startswith("http"):
                    best = url

            print('paramater being passed to osaka.main.rmall: ', best)  # making osaka call to delete product
            if best is not None:
                osaka.main.rmall(best)

            es.delete_by_id(index=index, id=ident, ignore=404)  # removing the metadata
            logger.info('Purged %s' % ident)
            if dataset in deleted_datasets:
                count = deleted_datasets[dataset]
                deleted_datasets[dataset] = count + 1
            else:
                deleted_datasets[dataset] = 1
        msg_details = "Datasets purged by type:\n\n"
        for ds in deleted_datasets.keys():
            msg_details += "{} - {}\n".format(deleted_datasets[ds], ds)

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

    query_obj = context['query']
    try:
        query_obj = json.loads(query_obj)
    except TypeError as e:
        logger.warning(e)

    purge_products(query_obj, component_val, operation_val)
