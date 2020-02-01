#!/bin/env python
import json
import logging
import osaka.main
from hysds.celery import app
from hysds_commons.elasticsearch_utils import ElasticsearchUtility

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
    logger.debug("Doing %s for %s with query: %s", operation, component, query)

    if component == "mozart" or component == "figaro":
        es_url = app.conf["JOBS_ES_URL"]
        es_index = app.conf["STATUS_ALIAS"]
    else:  # "tosca"
        es_url = app.conf["GRQ_ES_URL"]
        es_index = app.conf["DATASET_ALIAS"]

    es = ElasticsearchUtility(es_url, logger=logger)

    results = es.query(es_index, query)  # Querying for products

    if component == 'tosca':
        for result in results:
            ident = result["_id"]
            index = result["_index"]

            # find the Best URL first
            best = None
            for url in result["_source"]["urls"]:
                if not url.startswith("http"):
                    best = url

            # making osaka call to delete product
            print('paramater being passed to osaka.main.rmall: ', best)
            if best is not None:
                osaka.main.rmall(best)

            # removing the metadata
            es.delete_by_id(index, ident)
            logger.info('Purged %s' % ident)

    else:
        purge = True if operation == 'purge' else False  # purge job from index
        logger.info("")

        for result in results:
            uuid = result["_source"]['uuid']
            payload_id = result["_source"]['payload_id']
            index = result["_index"]

            # Always grab latest state (not state from query result)
            task = app.AsyncResult(uuid)
            state = task.state  # Active states may only revoke
            logger.info("Job state: %s\n", state)

            if state in ["RETRY", "STARTED"] or (state == "PENDING" and not purge):
                if not purge:
                    logger.info('Revoking %s\n', uuid)
                    app.control.revoke(uuid, terminate=True)
                else:
                    logger.info('Cannot remove active job %s\n', uuid)
                continue
            elif not purge:
                logger.info('Cannot stop inactive job: %s\n', uuid)
                continue

            # Safety net to revoke job if in PENDING state
            if state == "PENDING":
                logger.info('Revoking %s\n', uuid)
                app.control.revoke(uuid, terminate=True)

            # Both associated task and job from ES
            logger.info('Removing document from index %s for %s', index, payload_id)
            es.delete_by_id(index, payload_id)
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
