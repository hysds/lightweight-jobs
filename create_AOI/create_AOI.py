#!/bin/env python
import json
import logging
import sys, os, datetime
import hysds_commons.request_utils
import hysds_commons.metadata_rest_utils
import osaka.main
from string import Template
from hysds.celery import app

def get_met_file(AOI_name, priority):
	file_met = open('AOI.met.json')
	met_temp = Template( file_met.read())
	met_data = met_temp.substitute({'priority':priority})
	filename = AOI_name+'/AOI_'+AOI_name+'.met.json'
	with open(filename, 'w+') as f:
		f.write(met_data)

def get_dataset_file(AOI_name,version,label,coordinates):
	file_dataset = open('AOI.dataset.json')
	dataset_temp = Template(file_dataset.read())
	dataset_data = dataset_temp.substitute({'version':'"'+version+'"','label':'"'+label+'"','coordinates':coordinates})
	filename = AOI_name+'/AOI_'+AOI_name+'.dataset.json'
	with open(filename, 'w+') as f:
                f.write(dataset_data)

def get_browser_png():
	print "Trying to detect png in folder."
	for f in os.listdir(os.curdir): 
		if f.endswith(".png") or f.endswith(".jpeg") or f.endswith(".jpg"):
	        	print(f)
			os.system ("cp %s %s" % (f, AOI_name+"/browse.png"))
			os.system ("convert -resize 250x250 %s %s" % (AOI_name+"/browse.png", AOI_name+"/browse_small.png"))

def update_redis(AOI_name, event_time, days_back):
	#construct redis update commands
	event_date = datetime.datetime.strptime(event_time,"%Y-%m-%dT%H:%M:%S")
	start_time = event_date - datetime.timedelta(days = int(days_back))
	for types in ['unavco', 'scihub', 'asf']:
		redis_cmd =  'redis-cli set %s-%s-last_query %s' % (AOI_name, types, start_time.strftime("%Y-%m-%dT%H:%M:%S"))
		print redis_cmd
		#os.system(redis_cmd)
	

if __name__ == "__main__":
    '''
    Main program of purge_products
    '''
    AOI_name = sys.argv[1]
    label = sys.argv[2]
    coordinates = sys.argv[3]
    priority = sys.argv[4]
    version = sys.argv[5]
    event_time = sys.argv[6]
    days_back = sys.argv[7]
    days_fwd = sys.argv[8]
    #browse_img_URL = sys.argv[5]
   
    AOI_dir = AOI_name 
    # create the AOI folder
    if not os.path.exists(AOI_dir):
	    os.makedirs(AOI_dir)

    # create the metadata files for v2 
    get_met_file(AOI_name, priority)
    get_dataset_file(AOI_name,version,label,coordinates)
    get_browser_png()
    update_redis(AOI_name, event_time, days_back)
    # ~/verdi/ops/hysds/scripts/ingest_product.py ${AOI_DIR} ~/verdi/etc/datasets.json 2>&1 | tee -a ingest.log
