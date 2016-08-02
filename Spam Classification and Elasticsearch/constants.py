# -------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      Animesh
#
# Created:     16/03/2015
# Copyright:   (c) Animesh 2015
# Licence:     <your licence>
# -------------------------------------------------------------------------------

from elasticsearch import Elasticsearch
import glob
from elasticsearch import client

ES_HOST = {"host": "localhost", "port": 9200}
INDEX_NAME = 'spam_crawl'
TYPE_NAME = 'document'
ANALYZER_NAME = 'english'
ES_CLIENT = Elasticsearch(hosts=[ES_HOST], timeout=1800)
EMAIL_DOC_LIST = glob.glob('data/in*')

##index_alyzer = client.IndicesClient(ES_CLIENT)
##res = index_alyzer.analyze(
##        index=INDEX_NAME,
##        text="hi i am animesh pandey",
##        analyzer=ANALYZER_NAME
##    )
##
##QUERY = \
##{
##    "query" : {
##        "filtered" : {
##            "filter" : {
##                "term" : {
##                    "exact_text" : "viagra"
##                }
##            }
##        }
##    }
##}
##
##TEMP = \
##{
##    "span_near" : {
##        "clauses" : [
##            { "span_term" : { "field" : "value1" } },
##            { "span_term" : { "field" : "value2" } },
##            { "span_term" : { "field" : "value3" } }
##        ],
##        "slop" : 12,
##        "in_order" : False,
##        "collect_payloads" : False
##    }
##}
##
####{"query": {"match": {"text": "amazing deals"}}}
##
####res1 = ES_CLIENT.search(
####            index = INDEX_NAME,
####            body = {"query": {"match_phrase": {"exact_text": "improves erection"}}},
####            size=300
####        )
##

##ALL_DOC_QUERY = {"query":{"match_all": {}}}
##PHRASE_QUERY = {"query": {"match_phrase": {"text": "Credit card"}}}
##
##count = 0
##rs = ES_CLIENT.search(
##        index=INDEX_NAME,
##        scroll='60s',
##        size=100,
##        body=ALL_DOC_QUERY
##    )
##
##scroll_size = rs['hits']['total']
##temp = []
##while scroll_size:
##    try:
##        scroll_id = rs['_scroll_id']
##        rs = ES_CLIENT.scroll(scroll_id=scroll_id, scroll='60s')
##        data = rs['hits']['hits']
##        for entry in data:
##            docid = entry['_id']
##            temp.append(docid)
##            count += 1
##        scroll_size = len(rs['hits']['hits'])
##    except Exception as e:
##        print e
##
##print count
