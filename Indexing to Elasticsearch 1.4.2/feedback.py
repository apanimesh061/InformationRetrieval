# -------------------------------------------------------------------------------
# Name:        feedback
# Purpose:     implementing the Psuedo-Relevance Feedback
#
# Author:      Animesh Pandey
#
# Created:     30/01/2015
# Copyright:   (c) Animesh Pandey 2015
# -------------------------------------------------------------------------------

from __future__ import division

import constants, search, es_utility, util
from math import log

QUERY = es_utility.readQueryFile("query_desc.51-100.short.txt")
qno = [85, 59, 56, 71, 64, 62, 93, 99, 58, 77, 54, 87, 94, 100, 89, 61, 95, 68, 57, 97, 98, 60, 80, 63, 91]

QUERY_MAP = zip(qno, QUERY)
temp = dict()


def getTopDocsForQuery(score_fuction, k):
    for n, q in QUERY_MAP:
        doc_list = []
        whole_text = []
        root = es_utility.getRootQuery(" ".join(q))
        if score_fuction == "okapi":
            body = search.OKAPI_BODY(root)
        elif score_fuction == "tfidf":
            body = search.TFIDF_BODY(root)
        elif score_fuction == "bm25":
            body = search.BM25_BODY(root)
        elif score_fuction == "lm":
            body = search.LM_BODY(root)
        elif score_fuction == 'jm':
            body = search.JM_BODY(root)
        else:
            print "undefined..."
        res = constants.ES_CLIENT.search \
            (index=constants.INDEX_NAME,
             body=body,
             size=constants.RESULT_SIZE,
             analyzer="aplyzer")
        for hit in res['hits']['hits']:
            doc_list.append(hit["_id"])

        yield doc_list[:k], n


def termImportance(term, doc_list):
    total_score = 0.0
    for doc in doc_list:
        importance = \
            es_utility.getTermFreq(term, doc) * \
            log(len(doc_list) / (1 + es_utility.getDocumentFreq(term)))
        total_score += importance
    return total_score


TEMP_BODY = \
    {
        "query": {
            "match": {
                "_all": 'allegations measures taken corrupt public officials governmental jurisdiction worldwide'
            }
        }
    }

res1 = constants.ES_CLIENT.search \
    (index=constants.INDEX_NAME,
     body=TEMP_BODY,
     size=constants.RESULT_SIZE,
     analyzer="aplyzer",
     explain=True)

from elasticsearch import client

index_alyzer = client.IndicesClient(constants.ES_CLIENT)
res = index_alyzer.analyze(
    index=constants.INDEX_NAME,
    text='allegations measures taken corrupt public officials governmental jurisdiction worldwide',
    analyzer=constants.ANALYZER_NAME)

if __name__ == "__main__":
   top_terms = dict()

   for l in getTopDocsForQuery('tfidf', 25):
       unique_terms = set()
       for d in l[0]:
           [unique_terms.add(t) for t in list(es_utility.getDocumentUnique(d))]

       td = dict()
       count = 0
       for term in list(unique_terms):
           count += 1
           td.update({term: termImportance(term, l[0])})
           if count % 100 == 0:
               print "Reached term " + str(count) + " ..."

       sort_by_val = sorted(td.items(), key = lambda x: x[1], reverse = True)
       top_terms.update({l[1] : [val[0] for val in sort_by_val]})

   util.saveJSON('prf_data1.json', top_terms)
