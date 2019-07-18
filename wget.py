import json, requests, types, re, getpass, sys, os
from pprint import pformat
import logging
import tarfile
import notify_by_email
from hysds.celery import app
import boto3
from urlparse import urlparse
import datetime


PRODUCT_TEMPLATE = "product_downloader-{0}-{1}-{2}"

#TODO: Setup logger for this job here.  Should log to STDOUT or STDERR as this is a job
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("hysds")


def wget_script(dataset=None, glob_list=None):
    """Return wget script."""

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
    #size = int(math.ceil(count/10.0))
    #print("SIZE : %d" %size)
    #scroll_id = scan_result['_scroll_id']
    logging.debug('%s/%s/_search?search_type=scan&scroll=10m&size=%s' % (es_url, index, count))
    r = requests.post('%s/%s/_search?search_type=scan&scroll=10m&size=%s' % (es_url, index, count), json.dumps(dataset))
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
    def stream_wget(scroll_id, glob_list=None):
        #formatted_source = format_source(source)
        yield '#!/bin/bash\n#\n' + \
              '# query:\n#\n' + \
              '%s#\n#\n#' % json.dumps(dataset) + \
              '# total datasets matched: %d\n\n' % count + \
              'read -s -p "JPL Username: " user\n' + \
              'echo ""\n' + \
              'read -s -p "JPL Password: " password\n' + \
              'echo ""\n'
        wget_cmd = 'wget --no-check-certificate --mirror -np -nH --reject "index.html*"'
        wget_cmd_password = wget_cmd + ' --user=$user --password=$password'

        while True:
            r = requests.post('%s/_search/scroll?scroll=10m' % es_url, data=scroll_id)
            res = r.json()
            logger.debug("res: %s" % pformat(res))
            scroll_id = res['_scroll_id']
            if len(res['hits']['hits']) == 0: break
            # Elastic Search seems like it's returning duplicate urls. Remove duplicates
            unique_urls=[]
            for hit in res['hits']['hits']: 
                [unique_urls.append(url) for url in hit['_source']['urls'] if url not in unique_urls and url.startswith("http")]

            for url in unique_urls:
		logging.debug("urls in unique urls: %s",url)
                if '.s3-website' in url or 'amazonaws.com' in url:
                        parsed_url = urlparse(url)
                        cut_dirs = len(parsed_url.path[1:].split('/')) - 1
                else:
                        if 's1a_ifg' in url:
                                cut_dirs = 3
                        else:
                                cut_dirs = 6
                if '.s3-website' in url or 'amazonaws.com' in url:
                        files = get_s3_files(url)
                        if glob_list:
                            files = glob_filter(files, glob_list)
                        for file in files:
                                yield 'echo "downloading  %s"\n' % file
                                if 's1a_ifg' in url:
                                        yield "%s --cut-dirs=%d %s\n" % (wget_cmd, cut_dirs, file)
                                else:
                                        yield "%s --cut-dirs=%d %s\n" % (wget_cmd, cut_dirs, file)
                if 'aria2-dav.jpl.nasa.gov' in url:
                        yield 'echo "downloading  %s"\n' % url
                        yield "%s --cut-dirs=%d %s/\n" % (wget_cmd_password, (cut_dirs+1), url)
                if 'aria-csk-dav.jpl.nasa.gov' in url:
                        yield 'echo "downloading  %s"\n' % url
                        yield "%s --cut-dirs=%d %s/\n" % (wget_cmd_password, (cut_dirs+1), url)
                if 'aria-dst-dav.jpl.nasa.gov' in url:
                        yield 'echo "downloading  %s"\n' % url
                        yield "%s --cut-dirs=%d %s/\n" % (wget_cmd, cut_dirs, url)
                        break
    
    # malarout: interate over each line of stream_wget response, and write to a file which is later attached to the email.
    with open('wget_script.sh','w') as f:
        for i in stream_wget(scroll_id):
                f.write(i)

    # for gzip compressed use file extension .tar.gz and modifier "w:gz"
    #os.rename('wget_script.sh','wget_script.bash')
    #tar = tarfile.open("wget.tar.gz", "w:gz") 
    #tar.add('wget_script.bash')
    #tar.close()


def get_s3_files(url):
        files = []
	print("Url in the get_s3_files function: %s",url)
        parsed_url = urlparse(url)
        bucket = parsed_url.hostname.split('.', 1)[0]
        client = boto3.client('s3')
        results = client.list_objects(Bucket=bucket, Delimiter='/', Prefix=parsed_url.path[1:] + '/')

        if results.get('Contents'):
                for result in results.get('Contents'):
                        files.append(parsed_url.scheme + "://" + parsed_url.hostname + '/' + result.get('Key'))

        if results.get('CommonPrefixes'):
                for result in results.get('CommonPrefixes'):
                        # Prefix values have a trailing '/'. Let's remove it to be consistent with our dir urls
                        folder = parsed_url.scheme + "://" + parsed_url.hostname + '/' + result.get('Prefix')[:-1]
                        files.extend(get_s3_files(folder))
        return files

def email(query, emails, rule_name):
    '''
    Sends out an email with the script attached
    '''
    # for gzip compressed use file extension .tar.gz and modifier "w:gz"
    os.rename('wget_script.sh', 'wget_script.bash')
    tar = tarfile.open("wget.tar.gz", "w:gz")
    tar.add('wget_script.bash')
    tar.close()
    attachments = None
    cc_recipients = [i.strip() for i in emails.split(',')]
    bcc_recipients = []
    subject = "[monitor] (wget_script:%s)" % (rule_name)
    body = "Product was ingested from query: %s" % query
    body += "\n\nYou can use this wget script attached to download products.\n"
    body += "Please rename wget_script.bash to wget_script.sh before running it."
    if os.path.isfile('wget.tar.gz'):
        wget_content = open('wget.tar.gz', 'r').read()
        attachments = { 'wget.tar.gz':wget_content}
    notify_by_email.send_email(getpass.getuser(), cc_recipients,
                               bcc_recipients, subject, body, attachments=attachments)

def make_product(rule_name, query):
    '''
    Make a product out of this WGET script
    '''
    with open("_context.json", "r") as fp:
        context = json.load(fp)
        name = PRODUCT_TEMPLATE.format(rule_name, context["username"],
                                       datetime.datetime.now().strftime("%Y%m%dT%H%M%S"))
    os.mkdir(name)
    os.rename("wget_script.sh", "{0}/wget_script.bash".format(name))
    with open("{0}/{0}.met.json".format(name), "w") as fp:
        json.dump({"source_query": json.dumps(query)}, fp)
    with open("{0}/{0}.dataset.json".format(name), "w") as fp:
        json.dump({"id": name, "version": "v0.1"}, fp)

def glob_filter(names,pattern):
    import fnmatch
    files = []
    for pat in pattern:
        matching = fnmatch.filter(names, pat)
        files.extend(matching)
    #unique list
    retfiles_set = set(files)
    return list(retfiles_set)

if __name__ == "__main__":
    '''
    Main program of wget_script
    '''
    #encoding to a JSON object
    query = {} 
    query = json.loads(sys.argv[1]) 
    emails = sys.argv[2]
    rule_name = sys.argv[3]

    # get the glob
    try:
        context_file = '_context.json'
        with open(context_file, 'r') as fin:
            context = json.load(fin)
    except:
        raise Exception('unable to parse _context.json from work directory')

    globs = None
    if "glob" in context:
        glob_strs = context["glob"]
        globs = [item.strip() for item in glob_strs.split(',')]
    # getting the script
    wget_script(query,globs)
    if emails=="unused":
	make_product(rule_name, query)
    else:
    	# now email the query
   	email(query, emails, rule_name)
