# -------------------------------------------------------------------------------
# Name:        es_utility
# Purpose:     contains utility functions related to ES indices
#
# Author:      Animesh Pandey
#
# Created:     23/01/2015
# Copyright:   (c) Animesh Pandey 2015
# -------------------------------------------------------------------------------

from __future__ import division

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from elasticsearch import client
from elasticsearch import exceptions
import util
import constants
from math import log

"""
Creating a backup of an index in other cluster

initialise new index then,
reindex(
    client=ES_CLIENT,
    source_index=INDEX_NAME,
    target_index=BACKUP_INDEX,
    target_client=ES_CLIENT2,
    chunk_size=100,
    scroll='300m'
)

"""


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
        val += res['_source']['doc_len']
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


def getDocumentFreq(term):
    res = constants.ES_CLIENT.search \
        (index=constants.INDEX_NAME, body={"query": {"match": {"text": term}}})
    return res['hits']['total']


def getTermVector(doc_id, req_fields):
    a = constants.ES_CLIENT.termvector(
        index=constants.INDEX_NAME,
        doc_type=constants.TYPE_NAME,
        id=doc_id,
        field_statistics=True,
        fields=req_fields,
        term_statistics=True
    )
    return a


def getInvDocumentFreq(term, doc_id):
    return getTermFreq(term, doc_id) \
           * log(getIndexLength() / (1 + getDocumentFreq(term)))


def getDocumentList(term):
    doclist = []
    res = constants.ES_CLIENT.search(
        index=constants.INDEX_NAME,
        body={"query": {"match": {"text": term}}},
        size=20
    )
    for hit in res['hits']['hits']:
        print hit["_id"], hit['_score']
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


def get_source(docid):
    try:
        return constants.ES_CLIENT.get(constants.INDEX_NAME, id=docid, \
                                       doc_type=constants.TYPE_NAME)['_source']
    except exceptions.TransportError as e:
        if e.status_code == 404:
            return 0


def getRootQuery(query):
    index_alyzer = client.IndicesClient(constants.ES_CLIENT)
    res = index_alyzer.analyze(
        index=constants.INDEX_NAME,
        text=query,
        analyzer=constants.ANALYZER_NAME)
    return [str(t['token']) for t in res['tokens']]


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

##a = getTermVector("http://en.wikipedia.org/wiki/HMS_Grenade_(H86)", ['text'])

##source = get_source("http://en.wikipedia.org/wiki/Paddle_steamer")
##if source:
##    print source['text']

##getRootQuery(a)
