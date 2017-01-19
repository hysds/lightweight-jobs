#!/bin/env python
import copy, json, traceback
import logging

import hysds_commons.request_utils
import hysds_commons.hysds_io_utils
import hysds_commons.mozart_utils
import hysds_commons.job_rest_utils

import lib.get_component_configuration

from hysds.celery import app

#TODO: Setup logger for this job here.  Should log to STDOUT or STDERR as this is a job
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("job-iterator")


def job_iterator(component,rule):
    '''
    Iterator used to iterate across a query result and submit jobs for every hit
    @param component - "mozart" or "tosca" where this submission came from
    @param rule - rule containing information for running jobs
    '''
    #Accumulators variables
    ids = []
    error_count = 0
    errors = []
    
    #Read config from "origin"
    es_url, es_index, ignore1 = lib.get_component_configuration.get_component_config(component) 

    #Read in JSON formatted args and setup passthrough
    queryobj = {"query":rule["query"]}

    # Get wiriing
    hysdsio = hysds_commons.hysds_io_utils.get_hysds_io(es_url,rule["job_type"],logger=logger)
    
    #Is this a query_only submission, or per-dataset type
    query_only = False
    for param in hysdsio["params"]:
        if param["from"].startswith("dataset_json"):
            query_only = False
            break
        if param["name"] == "query" and param["from"] == "passthrough":
            query_only = True
    #Get results
    results = [{"_id":"Global-Query Only Submission"}]
    if not query_only:
        #Scroll product results
        start_url = "{0}/{1}/_search".format(es_url,es_index)
        scroll_url = "{0}/_search".format(es_url,es_index)
        results = hysds_commons.request_utils.post_scrolled_json_responses(start_url,scroll_url,data=json.dumps(queryobj),logger=logger)
    #Iterator loop
    for product in results:
        try:
            ids.append(hysds_commons.job_rest_utils.single_process_and_submission(app.conf["MOZART_URL"],product,rule,hysdsio))
        except Exception as e:
            error_count = error_count + 1
            if not str(e) in errors:
                errors.append(str(e))
            logger.warning("Failed to submit jobs: {0}:{1}".format(type(e),str(e)))
            logger.warning(traceback.format_exc())
    if error_count > 0:
        logger.error("Failed to submit: {0} of {1} jobs. {2}".format(error_count,len(results)," ".join(errors)))
        raise Exception("Job Submitter Job failed to submit all actions")

if __name__ == "__main__":
    '''
    Main program of job-iterator
    '''
    args = {}
    context = {}
    with open("_context.json","r") as f:
        context = json.load(f)
    job_iterator(context["submitter"],json.loads(context["submitter_rule"]))
