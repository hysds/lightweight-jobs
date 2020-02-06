#!/usr/bin/env python
import os
import sys
import getpass
import requests
import json
import types
import base64
import socket
from smtplib import SMTP
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.MIMEBase import MIMEBase
from email.Header import Header
from email.Utils import parseaddr, formataddr, COMMASPACE, formatdate
from email import Encoders

from hysds.celery import app
from hysds_commons.net_utils import get_container_host_ip
from hysds_commons.elasticsearch_utils import ElasticsearchUtility


def read_context():
    with open('_context.json', 'r') as f:
        cxt = json.load(f)
        return cxt


def get_hostname():
    """Get hostname."""
    try:
        return socket.getfqdn()
    except Exception as e:
        print(e)
        print('socket.getfqdn() failed, passing...')
        pass
    try:
        return socket.gethostbyname(socket.gethostname())
    except Exception as e:
        print(e)
        raise RuntimeError("Failed to resolve hostname for full email address. Check system.")


def send_email(sender, cc, bcc, subject, body, attachments=None):
    """
    Send an email.

    All arguments should be Unicode strings (plain ASCII works as well).

    Only the real name part of sender and recipient addresses may contain
    non-ASCII characters.

    The email will be properly MIME encoded and delivered though SMTP to
    172.17.0.1.  This is easy to change if you want something different.

    The charset of the email will be the first one out of US-ASCII, ISO-8859-1
    and UTF-8 that can represent all the characters occurring in the email.
    """

    recipients = cc + bcc  # combined recipients

    # Header class is smart enough to try US-ASCII, then the charset we
    # provide, then fall back to UTF-8.
    header_charset = 'ISO-8859-1'

    # We must choose the body charset manually
    for body_charset in 'US-ASCII', 'ISO-8859-1', 'UTF-8':
        try:
            body.encode(body_charset)
        except UnicodeError:
            pass
        else:
            break

    # Split real name (which is optional) and email address parts
    sender_name, sender_addr = parseaddr(sender)
    parsed_cc = [parseaddr(rec) for rec in cc]
    parsed_bcc = [parseaddr(rec) for rec in bcc]

    # We must always pass Unicode strings to Header, otherwise it will
    # use RFC 2047 encoding even on plain ASCII strings.
    unicode_parsed_cc = []
    for recipient_name, recipient_addr in parsed_cc:
        recipient_name = str(Header(str(recipient_name), header_charset))

        # Make sure email addresses do not contain non-ASCII characters
        recipient_addr = recipient_addr.encode('ascii')
        unicode_parsed_cc.append((recipient_name, recipient_addr))

    unicode_parsed_bcc = []
    for recipient_name, recipient_addr in parsed_bcc:
        recipient_name = str(Header(str(recipient_name), header_charset))

        # Make sure email addresses do not contain non-ASCII characters
        recipient_addr = recipient_addr.encode('ascii')
        unicode_parsed_bcc.append((recipient_name, recipient_addr))

    # Create the message ('plain' stands for Content-Type: text/plain)
    msg = MIMEMultipart()
    msg['CC'] = COMMASPACE.join([formataddr((recipient_name, recipient_addr))
                                 for recipient_name, recipient_addr in unicode_parsed_cc])
    msg['BCC'] = COMMASPACE.join([formataddr((recipient_name, recipient_addr))
                                  for recipient_name, recipient_addr in unicode_parsed_bcc])
    msg['Subject'] = Header(str(subject), header_charset)
    msg['FROM'] = "no-reply@jpl.nasa.gov"
    msg.attach(MIMEText(body.encode(body_charset), 'plain', body_charset))

    # Add attachments
    if isinstance(attachments, dict):
        for fname in attachments:
            part = MIMEBase('application', "octet-stream")
            part.set_payload(attachments[fname])
            Encoders.encode_base64(part)
            part.add_header('Content-Disposition',
                            'attachment; filename="%s"' % fname)
            msg.attach(part)

    # Send the message via SMTP to docker host
    smtp_url = "smtp://%s:25" % get_container_host_ip()
    print("smtp_url : %s", smtp_url)
    smtp = SMTP(get_container_host_ip())
    smtp.sendmail(sender, recipients, msg.as_string())
    smtp.quit()


def get_source(es_host, idx, _id):
    """Return source metadata for object_id."""
    es = ElasticsearchUtility(es_host)
    query = {
        "sort": {
            "_timestamp": {
                "order": "desc"
            }
        },
        "query": {
            "term": {
                "_id": _id
            }
        }
    }
    print('get_source debug %s from index: %s' % (_id, idx))
    print('query: %s' % json.dumps(query, indent=2))
    result = es.search(idx, query)
    if result['hits']['total']['value'] == 0:
        return None
    else:
        return result['hits']['hits'][0]['_source']


def get_cities(src):
    """Return list of cities."""

    cities = []
    for city in src.get('city', []):
        cities.append("%s, %s" %
                      (city.get('name', ''), city.get('admin1_name', '')))
    return cities


def get_value(d, key):
    """Return value from source based on key."""

    for k in key.split('.'):
        if k in d:
            d = d[k]
        else:
            return None
    if isinstance(d, list):
        return ', '.join([str(i) for i in d])
    else:
        return d


def get_metadata_snippet(src, snippet_cfg):
    """Return body text for metadata snippet."""

    body = ""
    for k, label in snippet_cfg:
        val = get_value(src, k)
        if val is not None:
            body += "%s: %s\n" % (label, val)
    body += "location type: %s\n" % src.get('location', {}).get('type', None)
    body += "location coordinates: %s\n" % src.get('location', {}).get('coordinates', [])
    cities = get_cities(src)
    body += "Closest cities: %s" % "\n\t\t".join(cities)
    return body


def get_facetview_link(link, _id, version=None):
    """
    Return link to object_id in FacetView interface.
    :param link: str
    :param _id: str, _id for elasticsearch document
    :param version: str
    :return: constructed URL for facetview
    """
    if version is None:
        query = {
            'query': {
                'query_string': {
                    'query': '_id:%s' % _id
                }
            }
        }
        b64 = base64.urlsafe_b64encode(json.dumps(query))
    else:
        query = {
            'query': {
                'query_string': {
                    'query': '_id:%s AND system_versions:%s' % (_id, version)
                }
            }
        }
        b64 = base64.urlsafe_b64encode(json.dumps(query))
    if link.endswith('/'):
        link = link[:-1]
    return '%s/?base64=%s' % (link, b64)


if __name__ == "__main__":
    cwd = os.getcwd()
    settings_file = os.path.join(cwd, 'settings.json')
    settings_file = os.path.normpath(settings_file)  # normalizing the path
    settings = json.load(open(settings_file))

    context = read_context()

    object_id = context['objectid']
    url = context['url']
    emails = context['emails']
    rule_name = context['rule_name']
    component = context['component']

    if component == "mozart" or component == "figaro":
        es_url = app.conf["JOBS_ES_URL"]
        index = app.conf["STATUS_ALIAS"]
        facetview_url = app.conf["MOZART_URL"]
    else:  # "tosca"
        es_url = app.conf["GRQ_ES_URL"]
        index = app.conf["DATASET_ALIAS"]
        facetview_url = "https://aria-search-beta.jpl.nasa.gov/search"  # TODO: why is it hard coded

    cc_recipients = [i.strip() for i in emails.split(',')]
    bcc_recipients = []
    email_subject = "[monitor] (notify_by_email:%s) %s" % (rule_name, object_id)
    email_body = "Product with id %s was ingested." % object_id
    email_attachments = None

    doc = get_source(es_url, index, object_id)
    if doc is not None:
        email_body += "\n\n%s" % get_metadata_snippet(doc, settings['SNIPPET_CFG'])
        email_body += "\n\nThe entire metadata json for this product has been attached for your convenience.\n\n"
        email_attachments = {
            'metadata.json': json.dumps(doc, indent=2)  # attach metadata json
        }

        # attach browse images
        if len(doc['browse_urls']) > 0:
            browse_url = doc['browse_urls'][0]
            if len(doc['images']) > 0:
                email_body += "Browse images have been attached as well.\n\n"
                for i in doc['images']:
                    small_img = i['small_img']
                    small_img_url = os.path.join(browse_url, small_img)
                    r = requests.get(small_img_url)
                    if r.status_code != 200:
                        continue
                    email_attachments[small_img] = r.content
    else:
        email_body += "\n\n"

    email_body += "You may access the product here:\n\n%s" % url

    system_version = None if doc is None else doc.get('system_version')
    facet_url = get_facetview_link(facetview_url, object_id, system_version)

    if facet_url is not None:
        email_body += "\n\nYou may view this product in FacetView here:\n\n%s" % facet_url
        email_body += "\n\nNOTE: You may have to cut and paste the FacetView link into your "
        email_body += "browser's address bar to prevent your email client from escaping the curly brackets."

    username_email = "%s@%s" % (getpass.getuser(), get_hostname())
    send_email(username_email, bcc_recipients, email_subject, email_body, attachments=email_attachments)
