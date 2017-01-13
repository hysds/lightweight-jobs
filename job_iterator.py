#!/bin/env python
import copy, json, traceback
import logging

import hysds_commons.request_utils
import hysds_commons.hysds_io_utils
import hysds_commons.mozart_utils

#TODO: Setup logger for this job here.  Should log to STDOUT or STDERR as this is a job
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("hysds")

def job_iterator(mozart_url,es_url,es_index,es_wiring_url,wiring_name,username,query,queue,priority,job_type,tags,kwargs):
    '''
    Iterator used to iterate across a query result and submit jobs for every hit
    @param mozart_url - Mozart URL to hit with submission
    @param es_url - ElasticSearch URL to hit with query
    @param es_index - Index in ElasticSearch to hit with query (usually an alias)
    @param es_wiring_url - ElasticSearch to hit with query for wirings
    @param wiring_name - the name of the hysds-io wiring to look up
    @param username - name of the user owning this submission
    @param query - query to post to ElasticSearch and whose result will be iterated, JSON sting encoded
    @param queue - queue to submit to
    @param priority - priority of jobs submitted
    @param job_type - type of jobs to submit
    @param tags - tags to attach to submitted jobs. In JSON string encoding
    @param kwargs - key-word args to match to HySDS IO
    '''
    #Accumulators variables
    ids = []
    error_count = 0
    errors = []
    
    #Read in JSON formatted args and setup passthrough
    tags = json.loads(tags)
    queryobj = {"query":json.loads(query)}
    kwargs = json.loads(kwargs)
    passthrough = {"name":tags[0],"query":query,"username":username,"priority":priority,"type":job_type,"queue":queue}

    # Get wiriing
    wiring = hysds_commons.hysds_io_utils.get_hysds_io(es_wiring_url,wiring_name,logger=logger)
    
    #Is this a single submission, or per-dataset type
    single = False
    for param in wiring["params"]:
        if param["from"].startswith("dataset_json"):
            single = False
            break
        if param["name"] == "query" and param["from"] == "passthrough":
            single = True
    #Get results
    results = [{"_id":"Global"}]
    if not single:
        #Scroll product results
        start_url = "{0}/{1}/_search".format(es_url,es_index)
        scroll_url = "{0}/_search".format(es_url,es_index)
        results = hysds_commons.request_utils.post_scrolled_json_responses(start_url,scroll_url,data=json.dumps(queryobj),logger=logger)
    #This is the common data for all jobs, and will be copied for each individual submission
    base_data = {"queue":queue,"priority":priority,"type":job_type,"tags":json.dumps(tags)}

    #Iterator loop
    for product in results:
        try:
            logger.info("Running: '{0}' on '{1}' for '{2}'".format(job_type,product["_id"],tags[0]))
            data = copy.copy(base_data)
            params = get_params_for_submission(wiring,kwargs,passthrough,product,base_data) 
            data["params"] = json.dumps(params)
            ids.append(hysds_commons.mozart_utils.submit_job(mozart_url,data))
        except Exception as e:
            error_count = error_count + 1
            if not str(e) in errors:
                errors.append(str(e))
            logger.warning("Failed to submit jobs: {0}:{1}".format(type(e),str(e)))
            logger.warning(traceback.format_exc())
    if error_count > 0:
        logger.error("Failed to submit: {0} of {1} jobs. {2}".format(error_count,len(results)," ".join(errors)))
        raise Exception("Job Submitter Job failed to submit all actions")
def get_params_for_submission(wiring,kwargs,passthrough=None,product=None,params={}):
    '''
    Get params for submission for HySDS/Tosca style workflow
    @param wiring - wiring specification
    @param kwargs - arguments from user form
    @param passthrough - rule
    '''
    params = {}
    for wire in wiring["params"]:
        if not wire["name"] in params:
            val = get_inputs(wire,kwargs,passthrough,product)
            params[wire["name"]] = run_lambda(wire,val)
    return params
def run_lambda(wire, val):
    '''
    Runs the lambda key as a lambda function with 1 arg, the previous value
    @param wire - wiring spec to check for lambda
    @param val
    '''
    if "lambda" in wire:
        try:
            if not wire["lambda"].startswith("lambda:") and not wire["lambda"].startswith("lambda "):
                raise Exceion("[ERROR] Failed to run lambda function, must be lambda expression taking 0 or 1 inputs")
            import functools
            fn = eval(wire["lambda"],{"functools":functools},{})
            val = fn(val)
        except Exception as e:
            raise Exception("[ERROR] Failed to run lambda function to fill {0}. {1}:{2}".format(wire["name"],type(e),e))
    return val

def get_inputs(param,kwargs,rule=None,product=None):
    '''
    Update parameter to add in a value for the param
    @param param - parameter to update
    @param kwargs - inputs from user form
    @param rule - (optional) rule hit to use to fill pass throughs
    @param product - (optional) product hit for augmenting
    '''
    #Break out if value is known
    if "value" in param:
        ret = param["value"]
        return ret
    source = param.get("from","unknown")
    #Get a value
    ret = param.get("default_value",None)
    if source == "submitter":
        ret = kwargs.get(param.get("name","unknown"),None)
    elif source == "passthrough" and not rule is None:
        ret = rule.get(param["name"],None)
    elif source.startswith("dataset_jpath:") and not product is None:
        ret = process_xpath(source.split(":")[1],product)
    #Check value is found
    if ret is None and not product is None and not rule is None:
        raise Exception("Failed to find '{0}' input from '{1}'".format(param.get("name","unknown"),source))
    return ret

def process_xpath(xpath,trigger):
    '''
    Process the xpath to extract data from a trigger
    @param xpath - xpath location in trigger
    @param trigger - trigger metadata to extract XPath
    '''
    ret = trigger
    parts = xpath.split(".")
    for part in parts:
        if ret is None or part == "":
            return ret
        #Try to convert to integer, if possible, for list indicies
        try:
            part = int(part)
            if len(ret) <= part:
                ret = None
            else:
                ret = ret[part]
            continue
        except:
            pass
        ret = ret.get(part,None)
    return ret

if __name__ == "__main__":
    '''
    Main program of job-iterator
    '''
    args = {}
    context = {}
    with open("_context.json","r") as f:
        context = json.load(f)
    for param in context["job_specification"]["params"]:
        args[param["name"]] = param["value"]
    job_iterator(**args)
