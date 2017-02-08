from hysds.celery import app

def get_component_config(component):
    '''
    From a component get the common configuration values
    @param component - component 
    '''
    if component=="mozart" or component=="figaro":
        es_url = app.conf["JOBS_ES_URL"]
        query_idx = app.conf["STATUS_ALIAS"]
        facetview_url = app.conf["MOZART_URL"]
    elif component=="tosca":
        es_url = app.conf["GRQ_ES_URL"]
        query_idx = app.conf["DATASET_ALIAS"]
        facetview_url = app.conf["GRQ_URL"]
    return (es_url, query_idx, facetview_url)
