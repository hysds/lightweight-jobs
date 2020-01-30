#!/bin/env python
import json
import logging
import sys
import osaka.main
from hysds.celery import app

from hysds_commons.elasticsearch_utils import ElasticsearchUtility

# TODO: Setup logger for this job here.  Should log to STDOUT or STDERR as this is a job
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("hysds")


def purge_products(query, component, operation):
    """
    Iterator used to iterate across a query result and submit jobs for every hit
    @param es_url - ElasticSearch URL to hit with query
    @param es_index - Index in ElasticSearch to hit with query (usually an alias)
    @param username - name of the user owning this submission
    @param query - query to post to ElasticSearch and whose result will be iterated, JSON sting enc
    @param kwargs - key-word args to match to HySDS IO
    """
    logger.debug("Doing %s for %s with query: %s", operation, component, query)

    if component == "mozart" or component == "figaro":
        es_url = app.conf["JOBS_ES_URL"]
        es_index = app.conf["STATUS_ALIAS"]
    elif component == "tosca":
        es_url = app.conf["GRQ_ES_URL"]
        es_index = app.conf["DATASET_ALIAS"]

    es = ElasticsearchUtility(es_url)

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

    else:
        # purge job from index
        purge = True if operation == 'purge' else False

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
        logger.info('Finished\n')


if __name__ == "__main__":
    """Main program of purge_products"""

    # encoding to a JSON object
    # decoded_string = sys.argv[1].decode('string_escape')
    # dec = decoded_string.replace('u""','"')
    # decoded_inp = dec.replace('""','"')
    decoded_inp = sys.argv[1]
    print(decoded_inp)

    if decoded_inp.startswith('{"query"') or decoded_inp.startswith("{u'query'") or decoded_inp.startswith("{'query'"):
        query_obj = json.loads(decoded_inp)
    else:
        # TODO: not sure what this means
        query_obj["query"] = json.loads(decoded_inp)

    component = sys.argv[2]
    operation = sys.argv[3]
    purge_products(query_obj, component, operation)
