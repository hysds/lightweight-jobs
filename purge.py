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

def purge_products(query,component,operation):
    '''
    Iterator used to iterate across a query result and submit jobs for every hit
    @param es_url - ElasticSearch URL to hit with query
    @param es_index - Index in ElasticSearch to hit with query (usually an alias)
    @param username - name of the user owning this submission
    @param query - query to post to ElasticSearch and whose result will be iterated, JSON sting enc
    @param kwargs - key-word args to match to HySDS IO
    '''
    logger.debug("Doing %s for %s with query: %s",operation,component,query)
    
    if component=="mozart" or component=="figaro":
        es_url = app.conf["JOBS_ES_URL"]
        es_index = app.conf["STATUS_ALIAS"]
        facetview_url = app.conf["MOZART_URL"]
    elif component=="tosca":
        es_url = app.conf["GRQ_ES_URL"]
        es_index = app.conf["DATASET_ALIAS"]
        facetview_url = app.conf["GRQ_URL"]

    #Querying for products
    start_url = "{0}/{1}/_search".format(es_url,es_index)
    scroll_url = "{0}/_search".format(es_url,es_index)
    
    results = hysds_commons.request_utils.post_scrolled_json_responses(start_url,scroll_url,data=json.dumps(query),logger=logger)
    print results
    
    if component=='tosca':
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

    else:
	if operation=='purge':
		purge = True
	else:
		purge = False
	# purge job from index
    
        for result in results:
            uuid = result["_source"]['uuid']
            payload_id = result["_source"]['payload_id']
	    index = result["_index"]
            es_type = result['_type']
            #Always grab latest state (not state from query result)
            task = app.AsyncResult(uuid)
            state = task.state
            #Active states may only revoke
            logger.info("Job state: %s\n",state)
            if state in ["RETRY","STARTED"] or (state == "PENDING" and not purge):
                if not purge:
                    logger.info('Revoking %s\n',uuid)
                    app.control.revoke(uuid,terminate=True)
                else:
                    logger.info( 'Cannot remove active job %s\n',uuid)
                continue
            elif not purge:
                logger.info( 'Cannot stop inactive job: %s\n',uuid)
                continue
            #Saftey net to revoke job if in PENDING state
            if state == "PENDING":
                logger.info( 'Revoking %s\n',uuid)
                app.control.revoke(uuid,terminate=True)
            
            # Both associated task and job from ES
            logger.info( 'Removing ES for %s:%s',es_type,payload_id)
	    r = hysds_commons.metadata_rest_utils.remove_metadata(es_url,index,es_type,payload_id,logger) 
            #r.raise_for_status() #not req
            #res = r.json() #not req
            logger.info('done.\n')
        logger.info('Finished\n')
    

if __name__ == "__main__":
    '''
    Main program of purge_products
    '''
    #encoding to a JSON object
    #decoded_string = sys.argv[1].decode('string_escape')
    #dec = decoded_string.replace('u""','"')
    #decoded_inp = dec.replace('""','"')
    decoded_inp = sys.argv[1]
    print decoded_inp
    query_obj = json.loads(decoded_inp)

    component = sys.argv[2]
    operation = sys.argv[3]
    purge_products(query_obj,component,operation)

