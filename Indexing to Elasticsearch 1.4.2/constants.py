# -------------------------------------------------------------------------------
# Name:        constants
# Purpose:     collection of frequently used constants
#
# Author:      Animesh Pandey
#
# Created:     25/01/2015
# Copyright:   (c) Animesh Pandey 2015
# -------------------------------------------------------------------------------
import util, es_utility, glob
from elasticsearch import Elasticsearch

## For reading the documents
ADVANCED_PRE_PROCESSING = True
STREAM = True

## global values
JSON_FILE = "id_text_AP_p.json"
GLOBAL_JSON_FILE = "index_globals_final.json"
ES_HOST = {"host": "localhost", "post": 9200}
INDEX_NAME = 'ap_streamed_final'
TYPE_NAME = 'document'
ANALYZER_NAME = 'aplyzer'

STOP_LIST = util.getStopList("stoplist.txt")
DOC_LIST = util.getDocList('doclist.txt')
INDEX_CONSTANTS = util.loadJSON(GLOBAL_JSON_FILE)
CORPUS = glob.glob("ap89_collection/ap*")
RESULT_SIZE = 1000

ES_CLIENT = Elasticsearch(hosts=[ES_HOST], timeout=180)
