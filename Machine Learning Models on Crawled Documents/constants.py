import util, es_utility, glob
from elasticsearch import Elasticsearch

## For reading the documents
ADVANCED_PRE_PROCESSING = False
STREAM = False

## global values
JSON_FILE = "AP_DATA_FULL.json"
GLOBAL_JSON_FILE = "crawler.json"
ES_HOST = {"host": "localhost", "port": 9210}
##INDEX_NAME = 'ap_streamed_none'
INDEX_NAME = 'backup_vs'
TYPE_NAME = 'document'
ANALYZER_NAME = 'my_english'
##QREL_FILENAME = 'qrels.adhoc.51-100.AP89.txt'
QREL_FILENAME = 'QREL.txt'

STOP_LIST = util.getStopList("stoplist.txt")
DOC_LIST = util.getDocList('doclist.txt')
INDEX_CONSTANTS = util.loadJSON(GLOBAL_JSON_FILE)
CORPUS = glob.glob("ap89_collection/ap*")
RESULT_SIZE = 10

ES_CLIENT = Elasticsearch(hosts=[ES_HOST], timeout=180)
##print ES_CLIENT.count(index=INDEX_NAME)['count']
