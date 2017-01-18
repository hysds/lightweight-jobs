#!/bin/env python
import json
import logging
import sys
import hysds_commons.request_utils
import hysds_commons.metadata_rest_utils
import osaka.main
from hysds.celery import app

#TODO: Setup logger for this job here.  Should log to STDOUT or STDERR as this is a job
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("hysds")

def purge_products(query):
    '''
    Iterator used to iterate across a query result and submit jobs for every hit
    @param es_url - ElasticSearch URL to hit with query
    @param es_index - Index in ElasticSearch to hit with query (usually an alias)
    @param username - name of the user owning this submission
    @param query - query to post to ElasticSearch and whose result will be iterated, JSON sting enc
    @param kwargs - key-word args to match to HySDS IO
    '''
    
    es_url = app.conf["GRQ_ES_URL"]
    es_index = app.conf["DATASET_ALIAS"]

    #Querying for products
    start_url = "{0}/{1}/_search".format(es_url,es_index)
    scroll_url = "{0}/_search".format(es_url,es_index)
    
    results = hysds_commons.request_utils.post_scrolled_json_responses(start_url,scroll_url,data=json.dumps(query),logger=logger)
    print results
    
    for result in results:
    	es_type = result["_type"]
    	ident = result["_id"]
	index = result["_index"]
	#find the Best URL first
	best = None
	for url in result["_source"]["urls"]:
		if best is None or not url.startswith("http"):
			best = url
    
   	#making osaka call to delete product
	print 'paramater being passed to osaka.main.rmall: ',best
	osaka.main.rmall(best)
	#removing the metadata
	hysds_commons.metadata_rest_utils.remove_metadata(es_url,index,es_type,ident,logger)


if __name__ == "__main__":
    '''
    Main program of purge_products
    '''
    #encoding to a JSON object
    query_obj = {}
    query_obj['query'] = json.loads(sys.argv[1])
    purge_products(query_obj)

