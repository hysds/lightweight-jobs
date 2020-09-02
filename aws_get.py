import json
import getpass
import sys
import os
import logging
import tarfile
import notify_by_email
from hysds.celery import app
from urllib.parse import urlparse
from hysds.es_util import get_grq_es

# TODO: Setup logger for this job here.  Should log to STDOUT or STDERR as this is a job
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("aws_get")


def aws_get_script(dataset=None):
    """Return AWS get script."""
    grq_es = get_grq_es()
    index = app.conf["DATASET_ALIAS"]
    logger.debug("Dataset: {}".format(json.dumps(dataset, indent=2)))
    paged_result = grq_es.es.search(body=dataset, index=index, size=100, scroll="10m")
    logger.debug("Paged Result: {}".format(json.dumps(paged_result, indent=2)))

    count = len(paged_result["hits"]["hits"])
    scroll_id = paged_result["_scroll_id"]

    # stream output a page at a time for better performance and lower memory footprint
    def stream_aws_get(scroll_id, paged_result):
        yield '#!/bin/bash\n#\n' + \
              '# query:\n#\n' + \
              '#%s#\n#\n#' % json.dumps(dataset) + \
              '# total datasets matched: %d\n\n' % count + \
              'echo ""\n'
        aws_get_cmd = 'aws s3 sync {} {}\n'

        while True:
            if len(paged_result['hits']['hits']) == 0:
                break
            # Elastic Search seems like it's returning duplicate urls. Remove duplicates
            unique_urls = []
            for hit in paged_result['hits']['hits']:
                [unique_urls.append(url) for url in hit['_source']['urls']
                 if url not in unique_urls and url.startswith("s3")]

            for url in unique_urls:
                logging.debug("urls in unique urls: %s", url)
                parsed_url = urlparse(url)
                yield 'echo "downloading  %s"\n' % os.path.basename(parsed_url.path)
                yield aws_get_cmd.format("{}://{}".format(
                    parsed_url.scheme, parsed_url.path[1:] if parsed_url.path.startswith('/') else parsed_url.path),
                    os.path.basename(parsed_url.path))
            paged_result = grq_es.es.scroll(scroll_id=scroll_id, scroll="10m")
            scroll_id = paged_result['_scroll_id']

    # malarout: interate over each line of stream_aws_get response, and write to a file which is later attached to the
    # email.
    with open('aws_get_script.sh', 'w') as f:
        for i in stream_aws_get(scroll_id, paged_result):
            f.write(i)

    # for gzip compressed use file extension .tar.gz and modifier "w:gz"
    os.rename('aws_get_script.sh', 'aws_get_script.bash')
    tar = tarfile.open("aws_get.tar.gz", "w:gz")
    tar.add('aws_get_script.bash')
    tar.close()


if __name__ == "__main__":
    '''
    Main program of aws_get_script
    '''
    # encoding to a JSON object
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
        aws_get_content = open('aws_get.tar.gz', 'rb').read()
        attachments = {'aws_get.tar.gz': aws_get_content}
    notify_by_email.send_email(getpass.getuser(
    ), cc_recipients, bcc_recipients, subject, body, attachments=attachments)
