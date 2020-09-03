import json
import getpass
import sys
import os
import logging
import tarfile
import notify_by_email
from hysds.celery import app
import boto3
from urllib.parse import urlparse
import datetime

from hysds.es_util import get_grq_es


PRODUCT_TEMPLATE = "product_downloader-{0}-{1}-{2}"

# TODO: Setup logger for this job here.  Should log to STDOUT or STDERR as this is a job
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("hysds")


def wget_script(dataset=None, glob_dict=None):
    """Return wget script."""

    # query
    """Return AWS get script."""
    grq_es = get_grq_es()
    index = app.conf["DATASET_ALIAS"]
    logger.debug("Dataset: {}".format(json.dumps(dataset, indent=2)))
    paged_result = grq_es.es.search(body=dataset, index=index, size=100, scroll="10m")
    logger.debug("Paged Result: {}".format(json.dumps(paged_result, indent=2)))

    scroll_ids = set()
    count = paged_result["hits"]["total"]["value"]
    scroll_id = paged_result["_scroll_id"]
    scroll_ids.add(scroll_id)

    # stream output a page at a time for better performance and lower memory footprint
    def stream_wget(scroll_id, paged_result, glob_dict=None):
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
            if len(paged_result['hits']['hits']) == 0:
                break
            # Elastic Search seems like it's returning duplicate urls. Remove duplicates
            unique_urls = []
            for hit in paged_result['hits']['hits']:
                [unique_urls.append(url) for url in hit['_source']['urls']
                 if url not in unique_urls and url.startswith("http")]

            for url in unique_urls:
                logging.debug("urls in unique urls: %s", url)
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
                    if glob_dict:
                        files = glob_filter(files, glob_dict)
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

            paged_result = grq_es.es.scroll(scroll_id=scroll_id, scroll="10m")
            logger.debug("paged result: {}".format(json.dumps(paged_result, indent=2)))
            scroll_id = paged_result['_scroll_id']
            scroll_ids.add(scroll_id)

    # malarout: interate over each line of stream_wget response, and write to a file which is later attached to the
    # email.
    with open('wget_script.sh', 'w') as f:
        for i in stream_wget(scroll_id, paged_result, glob_dict):
            f.write(i)

    for sid in scroll_ids:
        grq_es.es.clear_scroll(scroll_id=sid)

    # for gzip compressed use file extension .tar.gz and modifier "w:gz"
    # os.rename('wget_script.sh','wget_script.bash')
    # tar = tarfile.open("wget.tar.gz", "w:gz")
    # tar.add('wget_script.bash')
    # tar.close()


def get_s3_files(url):
    files = []
    print(("Url in the get_s3_files function: %s", url))
    parsed_url = urlparse(url)
    bucket = parsed_url.hostname.split('.', 1)[0]
    client = boto3.client('s3')
    results = client.list_objects(
        Bucket=bucket, Delimiter='/', Prefix=parsed_url.path[1:] + '/')

    if results.get('Contents'):
        for result in results.get('Contents'):
            files.append(parsed_url.scheme + "://" +
                         parsed_url.hostname + '/' + result.get('Key'))

    if results.get('CommonPrefixes'):
        for result in results.get('CommonPrefixes'):
            # Prefix values have a trailing '/'. Let's remove it to be consistent with our dir urls
            folder = parsed_url.scheme + "://" + \
                parsed_url.hostname + '/' + result.get('Prefix')[:-1]
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
        wget_content = open('wget.tar.gz', 'rb').read()
        attachments = {'wget.tar.gz': wget_content}
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


def glob_filter(names, glob_dict):
    import fnmatch
    files = []
    files_exclude = []
    include_csv = glob_dict.get("include", None)
    exclude_csv = glob_dict.get("exclude", None)

    if include_csv:
        pattern_list = [item.strip() for item in include_csv.split(',')]

        for pat in pattern_list:
            matching = fnmatch.filter(names, "*" + pat)
            files.extend(matching)
        print("Got the following files to include: %s" % str(files))

    if exclude_csv:
        pattern_list_exc = [item.strip() for item in exclude_csv.split(',')]

        for pat in pattern_list_exc:
            matching = fnmatch.filter(names, "*" + pat)
            files_exclude.extend(matching)

        files_exclude = list(set(files_exclude))
        print("Got the following files to exclude: %s" % str(files_exclude))

    # unique list
    files_final = [x for x in files if x not in files_exclude]
    retfiles_set = set(files_final)
    print("Got the following files: %s" % str(retfiles_set))
    return list(files_final)


if __name__ == "__main__":
    '''
    Main program of wget_script
    '''
    # encoding to a JSON object
    query = json.loads(sys.argv[1])
    emails = sys.argv[2]
    rule_name = sys.argv[3]

    # get the glob
    try:
        context_file = '_context.json'
        with open(context_file, 'r') as fin:
            context = json.load(fin)
    except Exception as e:
        raise Exception('unable to parse _context.json from work directory: {}'.format(str(e)))

    glob_dict = None
    if "include_glob" in context and "exclude_glob" in context:
        glob_dict = {"include": context["include_glob"], "exclude": context["exclude_glob"]}

    # getting the script

    wget_script(query, glob_dict)
    if emails == "unused":
        make_product(rule_name, query)
    else:
        # now email the query
        email(query, emails, rule_name)
