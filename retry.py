#!/usr/bin/env python
import os, sys, json, requests, time, traceback
from random import randint
from datetime import datetime
from celery import uuid
from hysds.celery import app
from hysds.orchestrator import run_job
from hysds.log_utils import log_job_status


def resubmit_jobs():
    # random sleep to prevent from getting ElasticSearch errors:
    # 429 Client Error: Too Many Requests
    time.sleep(randint(1,5))
    # can call submit_job
    with open('_context.json') as f:
      ctx = json.load(f)
    for job_json in ctx['job']:
        try:
            ## get job json
            #job_json = ctx['job']
    
            #check retry_remaining_count
            retry_count_max = ctx['retry_count_max']

            if 'retry_count' in job_json:
        	if job_json['retry_count'] < retry_count_max :
		job_json['retry_count'] = int(job_json['retry_count']) + 1
        	elif job_json['retry_count'] == retry_count_max :
        		print "Job reached retry_count_max limit. Cannot retry again."
        		return
            else:
	        job_json['retry_count'] = 1 
    
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
            try:
                app.control.revoke(job_json['job_id'], terminate=True)
                print "revoked original job: %s" % job_json['job_id']
            except Exception, e:
                print "Got error issuing revoke on job %s: %s" % (job_json['job_id'], traceback.format_exc())
                print "Continuing."

            # generate celery task id
            job_json['task_id'] = uuid()

            # delete old job status
            try:
                r = requests.delete("%s/%s/job/_query?q=_id:%s" % (es_url, query_idx, job_json['job_id']))
                r.raise_for_status()
                print "deleted original job status: %s" % job_json['job_id']
            except Exception, e:
                print "Got error deleting job status %s: %s" % (ctx_json['job_id'], traceback.format_exc())
                print "Continuing."

            # log queued status
            job_status_json = { 'uuid': job_json['task_id'],
                                'job_id': job_json['job_id'],
                                'payload_id': job_json['job_info']['job_payload']['payload_task_id'],
                                'status': 'job-queued',
                                'job': job_json }
            log_job_status(job_status_json)

            # submit job
            queue = job_json['job_info']['job_queue']
            res = run_job.apply_async((job_json,), queue=queue,
                                      time_limit=None,
                                      soft_time_limit=None,
                                      priority=priority,
                                      task_id=job_json['task_id'])
        except Exception as ex:
            print >> sys.stderr, "[ERROR] Exception occured {0}:{1} {2}".format(type(ex),ex,traceback.format_exc())

if __name__ == "__main__":
       
    es_url = app.conf['JOBS_ES_URL']
    query_idx = app.conf['STATUS_ALIAS']
    facetview_url = app.conf['MOZART_URL']
    
    type = sys.argv[1]

    if type != "worker":  
    	resubmit_job()
    else:
	print "Cannot retry a worker."
