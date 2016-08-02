from __future__ import division
from elasticsearch import Elasticsearch
import util, es_utility, math
import constants, search

QUERY = es_utility.readQueryFile("query_desc.51-100.short.txt")
qno = [85, 59, 56, 71, 64, 62, 93, 99, 58, 77, 54, 87, 94, 100, 89, 61, 95, 68, 57, 97, 98, 60, 80, 63, 91]

QUERY_MAP = zip([str(q) for q in qno], QUERY)

expanded_terms = util.loadJSON('prf_data.json')


def saveResults(score_fuction):
    filename = score_fuction + "_prf_results.txt"
    with open(filename, 'wb') as f:
        for n, q in QUERY_MAP:
            stemmed_terms = es_utility.getRootQuery(" ".join(q))
            new_terms = [t for t in expanded_terms[n] if t not in stemmed_terms]
            root = stemmed_terms + new_terms
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

            rank = 1
            for hit in res['hits']['hits']:
                line = "%s  Q0  %s  %d  %s  Exp\n" % (n, str(hit["_id"]), rank, str(hit["_score"]))
                f.write(line)
                rank += 1
    print filename + " save complete ..."


if __name__ == "__main__":
    saveResults("okapi")
    saveResults("tfidf")
    saveResults("bm25")
    saveResults("lm")
    saveResults("jm")
