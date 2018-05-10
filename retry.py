#!/usr/bin/env python
import os, sys, json, requests, time, traceback
from random import randint, uniform
from datetime import datetime
from celery import uuid
from hysds.celery import app
from hysds.orchestrator import run_job
from hysds.log_utils import log_job_status

def query_ES(job_id):
   #get the ES_URL
   es_url = app.conf["JOBS_ES_URL"]
   index = app.conf["STATUS_ALIAS"]
   query_json = {"query":{"bool": {"must": [{"term": {"job.job_info.id": "job_id"}}]}}}
   query_json["query"]["bool"]["must"][0]["term"]["job.job_info.id"] = job_id
   r = requests.post('%s/%s/_search?' % (es_url, index), json.dumps(query_json))
   return r


def rand_sleep(sleep_min=0.1, sleep_max=1): time.sleep(uniform(sleep_min, sleep_max))


def resubmit_jobs():
    # random sleep to prevent from getting ElasticSearch errors:
    # 429 Client Error: Too Many Requests
    time.sleep(randint(1,5))
    # can call submit_job

    #iterate through job ids and query to get the job json
    with open('_context.json') as f:
      ctx = json.load(f)

    retry_count_max = ctx['retry_count_max']
    retry_job_ids = ctx['retry_job_id'] if isinstance(ctx['retry_job_id'], list) else [ctx['retry_job_id']]
    for job_id in retry_job_ids:
        try:
            ## get job json for ES
            rand_sleep()
	    response = query_ES(job_id)
	    if response.status_code != 200:
		print("Failed to query ES. Got status code %d:\n%s" % (response.status_code, json.dumps)(response.json(), indent = 2))
	    response.raise_for_status()
            resp_json = response.json()
            job_json = resp_json["hits"]["hits"][0]["_source"]["job"]

            # don't retry a retry
            if job_json['type'].startswith('job-lw-mozart-retry'):
                print "Cannot retry retry job %s. Skipping" % job_id
                continue

            #check retry_remaining_count
            if 'retry_count' in job_json:
                if job_json['retry_count'] < retry_count_max:
    	            job_json['retry_count'] = int(job_json['retry_count']) + 1
                else:
                    print "Job reached retry_count_max limit. Cannot retry again."
                    continue
            else:
                job_json['retry_count'] = 1 
    	
	    job_json["job_info"]["dedup"] = False
            # clean up job execution info
            for i in ( 'duration', 'execute_node', 'facts', 'job_dir', 'job_url',
                       'metrics', 'pid', 'public_ip', 'status', 'stderr',
                       'stdout', 'time_end', 'time_queued', 'time_start' ):
                if i in job_json.get('job_info', {}): del job_json['job_info'][i]

            # set queue time
            job_json['job_info']['time_queued'] = datetime.utcnow().isoformat() + 'Z'

            # use priority from context
            priority = ctx['job_priority']

            # reset priority
            job_json['priority'] = priority

            # revoke original job
            rand_sleep()
            try:
                app.control.revoke(job_json['job_id'], terminate=True)
                print "revoked original job: %s" % job_json['job_id']
            except Exception, e:
                print "Got error issuing revoke on job %s: %s" % (job_json['job_id'], traceback.format_exc())
                print "Continuing."

            # generate celery task id
            job_json['task_id'] = uuid()

            # delete old job status
            rand_sleep()
            try:
                r = requests.delete("%s/%s/job/_query?q=_id:%s" % (es_url, query_idx, job_json['job_id']))
                r.raise_for_status()
                print "deleted original job status: %s" % job_json['job_id']
            except Exception, e:
                print "Got error deleting job status %s: %s" % (job_json['job_id'], traceback.format_exc())
                print "Continuing."

            # log queued status
            rand_sleep()
            job_status_json = { 'uuid': job_json['task_id'],
                                'job_id': job_json['job_id'],
                                'payload_id': job_json['job_info']['job_payload']['payload_task_id'],
                                'status': 'job-queued',
                                'job': job_json }
            log_job_status(job_status_json)

            # submit job
            queue = job_json['job_info']['job_queue']
            res = run_job.apply_async((job_json,), queue=queue,
                                      time_limit=job_json['job_info']['time_limit'],
                                      soft_time_limit=job_json['job_info']['soft_time_limit'],
                                      priority=priority,
                                      task_id=job_json['task_id'])
        except Exception as ex:
            print >> sys.stderr, "[ERROR] Exception occured {0}:{1} {2}".format(type(ex),ex,traceback.format_exc())

if __name__ == "__main__":
       
    es_url = app.conf['JOBS_ES_URL']
    query_idx = app.conf['STATUS_ALIAS']
    facetview_url = app.conf['MOZART_URL']
    
    input_type = sys.argv[1]

    #set dedup to false
    with open('_job.json') as f:
      job = json.load(f)

    job["job_info"]["dedup"] = False
    if input_type != "worker":  
        resubmit_jobs()
    else:
        print "Cannot retry a worker."

