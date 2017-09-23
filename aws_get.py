import json, requests, types, re, getpass, sys, os
from pprint import pformat
import logging
import tarfile
import notify_by_email
from hysds.celery import app
import boto3
from urlparse import urlparse

#TODO: Setup logger for this job here.  Should log to STDOUT or STDERR as this is a job
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("aws_get")


def aws_get_script(dataset=None):
    """Return AWS get script."""

    # query
    es_url = app.conf["GRQ_ES_URL"]
    index = app.conf["DATASET_ALIAS"]
    #facetview_url = app.conf["GRQ_URL"]
    print('%s/%s/_search?search_type=scan&scroll=10m&size=100' % (es_url, index))
    logging.debug('%s/%s/_search?search_type=scan&scroll=10m&size=100' % (es_url, index))
    print json.dumps(dataset)
    logging.debug(json.dumps(dataset))

    r = requests.post('%s/%s/_search?search_type=scan&scroll=10m&size=100' % (es_url, index), json.dumps(dataset))
    if r.status_code != 200:
        print("Failed to query ES. Got status code %d:\n%s" %(r.status_code, json.dumps(r.json(), indent=2)))
	logger.debug("Failed to query ES. Got status code %d:\n%s" %
                         (r.status_code, json.dumps(r.json(), indent=2)))
    r.raise_for_status()
    logger.debug("result: %s" % pformat(r.json()))

    scan_result = r.json()
    count = scan_result['hits']['total']
    scroll_id = scan_result['_scroll_id']

    # stream output a page at a time for better performance and lower memory footprint
    def stream_aws_get(scroll_id):
        #formatted_source = format_source(source)
        yield '#!/bin/bash\n#\n' + \
              '# query:\n#\n' + \
              '%s#\n#\n#' % json.dumps(dataset) + \
              '# total datasets matched: %d\n\n' % count + \
              'echo ""\n'
        aws_get_cmd = 'aws s3 sync {} {}\n'

        while True:
            r = requests.post('%s/_search/scroll?scroll=10m' % es_url, data=scroll_id)
            res = r.json()
            logger.debug("res: %s" % pformat(res))
            scroll_id = res['_scroll_id']
            if len(res['hits']['hits']) == 0: break
            # Elastic Search seems like it's returning duplicate urls. Remove duplicates
            unique_urls=[]
            for hit in res['hits']['hits']: 
                [unique_urls.append(url) for url in hit['_source']['urls'] if url not in unique_urls and url.startswith("s3")]

            for url in unique_urls:
		logging.debug("urls in unique urls: %s",url)
                parsed_url = urlparse(url)
                yield 'echo "downloading  %s"\n' % os.path.basename(parsed_url.path)
                yield aws_get_cmd.format("{}://{}".format(parsed_url.scheme, parsed_url.path),
                                         os.path.basename(parsed_url.path))
    
    # malarout: interate over each line of stream_aws_get response, and write to a file which is later attached to the email.
    with open('aws_get_script.sh','w') as f:
        for i in stream_aws_get(scroll_id):
                f.write(i)

    # for gzip compressed use file extension .tar.gz and modifier "w:gz"
    os.rename('aws_get_script.sh','aws_get_script.bash')
    tar = tarfile.open("aws_get.tar.gz", "w:gz") 
    tar.add('aws_get_script.bash')
    tar.close()


if __name__ == "__main__":
    '''
    Main program of aws_get_script
    '''
    #encoding to a JSON object
    query = {} 
    query = json.loads(sys.argv[1]) 
    emails = sys.argv[2]
    rule_name = sys.argv[3]
  
    # getting the script
    aws_get_script(query)
    # now email the query
    attachments = None
    cc_recipients = [i.strip() for i in emails.split(',')]
    bcc_recipients = []
    subject = "[monitor] (aws_get_script:%s)" % (rule_name)
    body = "Product was ingested from query: %s" % query
    body += "\n\nYou can use this AWS get script attached to download products.\n"
    body += "Please rename aws_get_script.bash to aws_get_script.sh before running it."
    if os.path.isfile('aws_get.tar.gz'):
	aws_get_content = open('aws_get.tar.gz','r').read()
	attachments = { 'aws_get.tar.gz':aws_get_content} 
    notify_by_email.send_email(getpass.getuser(), cc_recipients, bcc_recipients, subject, body, attachments=attachments)
