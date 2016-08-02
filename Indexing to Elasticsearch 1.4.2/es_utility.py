from __future__ import division

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from elasticsearch import client
import util
import constants
from math import log


def getIndexLength():
    return constants.ES_CLIENT.count(index=constants.INDEX_NAME)['count']


def getTotalTokens():
    val = 0
    temp = []
    scroll = scan \
        (constants.ES_CLIENT,
         query='{"fields": "_source"}',
         index=constants.INDEX_NAME, scroll='10s')
    for res in scroll:
        temp.append(res["_id"])
        val += res['_source']['doc_length']
    return val


def readQueryFile(filename):
    try:
        query_list = []
        with open(filename) as f:
            for query in f:
                query_list.append(query)
        return [util.removeStopWords \
                    (util.removePunctuation \
                         (" ".join([i for i in q.strip().split(" ") if i != ''][4:])).lower(), \
                     constants.STOP_LIST).split(" ") for q in query_list][:-2]
    except IOError:
        print "Query file could not be located..."
s

def getDocumentFreq(term):
    res = constants.ES_CLIENT.search \
        (index=constants.INDEX_NAME, body={"query": {"match": {"text": term}}})
    return res['hits']['total']


def getTermVector(term, doc_id):
    a = constants.ES_CLIENT.termvector \
        (index=constants.INDEX_NAME,
         doc_type=constants.TYPE_NAME,
         id=doc_id,
         field_statistics=True,
         fields=['text'],
         term_statistics=True)
    return a


def getInvDocumentFreq(term, doc_id):
    return getTermFreq(term, doc_id) * log(getIndexLength() / (1 + getDocumentFreq(term)))


def getDocumentList(term):
    doclist = []
    res = constants.ES_CLIENT.search \
        (index=constants.INDEX_NAME, body={"query": {"match": {"text": term}}}, \
         size=getDocumentFreq(term))
    for hit in res['hits']['hits']:
        doclist.append(hit["_id"])
    return doclist


def getDocumentLength(doc_id):
    a = constants.ES_CLIENT.mtermvectors \
        (index=constants.INDEX_NAME, doc_type=constants.TYPE_NAME,
         body=dict(
             ids=[doc_id],
             parameters=dict(
                 term_statistics=True,
                 field_statistics=True,
                 fields=['text'])))['docs']
    temp = a[0]['term_vectors']['text']['terms']
    return len(temp.keys())


def getTermFreq(term, doc_id):
    a = constants.ES_CLIENT.mtermvectors \
        (index=constants.INDEX_NAME, doc_type=constants.TYPE_NAME,
         body=dict(
             ids=[doc_id],
             parameters=dict(
                 term_statistics=True,
                 field_statistics=True,
                 fields=['text'])))['docs']
    try:
        tf = a[0]['term_vectors']['text']['terms'][term]['term_freq']
    except KeyError:
        tf = 0
    return tf


def getDocLenFor(term):
    doc_list = getDocumentList(term)
    val = 0
    temp = []
    scroll = scan \
        (constants.ES_CLIENT,
         query='{"fields": "_source"}',
         index=constants.INDEX_NAME, scroll='10s')
    for res in scroll:
        if res["_id"] in doc_list:
            val += res['_source']['doc_length']
    return val


def getDocLenForQuery(query):
    return [getDocLenFor(q) for q in query]


def vocabLength():
    body = \
        {"aggs": {
            "vocab_length": {
                "cardinality": {
                    "field": "text"}}}}
    res = constants.ES_CLIENT.search \
        (index=constants.INDEX_NAME,
         body=body,
         analyzer="aplyzer")

    return res["aggregations"]["vocab_length"]["value"]


def getRootQuery(query):
    index_alyzer = client.IndicesClient(constants.ES_CLIENT)
    res = index_alyzer.analyze(
        index=constants.INDEX_NAME,
        text=query,
        analyzer=constants.ANALYZER_NAME)
    return [t['token'] for t in res['tokens']]


def getDocumentUnique(doc_id):
    a = constants.ES_CLIENT.mtermvectors \
        (index=constants.INDEX_NAME, doc_type=constants.TYPE_NAME,
         body=dict(
             ids=[doc_id],
             parameters=dict(
                 term_statistics=True,
                 field_statistics=True,
                 fields=['text'])))['docs']
    temp = a[0]['term_vectors']['text']['terms']
    return set(temp.keys())
