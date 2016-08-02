from __future__ import division

from elasticsearch import Elasticsearch
import util, es_utility, math
import constants

QUERY = es_utility.readQueryFile("query_desc.51-100.short.txt")
qno = [85, 59, 56, 71, 64, 62, 93, 99, 58, 77, 54, 87, 94, 100, 89, 61, 95, 68, 57, 97, 98, 60, 80, 63, 91]

QUERY_MAP = zip(qno, QUERY)


def OKAPI_BODY(q):
    return {
        "query": {
            "function_score": {
                "boost_mode": "replace",
                "query": {
                    "match": {
                        "_all": " ".join(q)
                    }
                },
                "script_score": {
                    "params": {
                        "field": "text",
                        "terms": q,
                        "len_field": "doc_length"
                    },
                    "script": "okapi",
                    "lang": "groovy"
                }
            }
        }
    }


def TFIDF_BODY(q):
    return {
        "query": {
            "function_score": {
                "boost_mode": "replace",
                "query": {
                    "match": {
                        "_all": " ".join(q)
                    }
                },
                "script_score": {
                    "params": {
                        "field": "text",
                        "terms": q,
                        "len_field": "doc_length",
                        "avg_doc_len": \
                            constants.INDEX_CONSTANTS \
                                [constants.INDEX_NAME]['stats']['avg_doc_length'],
                        "no_of_docs": \
                            constants.INDEX_CONSTANTS[constants.INDEX_NAME]['total_docs']
                    },
                    "script": "tfidf",
                    "lang": "groovy"
                }
            }
        },
        "explain": True
    }


def BM25_BODY(q):
    return {
        "query": {
            "function_score": {
                "boost_mode": "replace",
                "query": {
                    "match": {
                        "_all": " ".join(q)
                    }
                },
                "script_score": {
                    "params": {
                        "field": "text",
                        "terms": q,
                        "len_field": "doc_length",
                        "avg_doc_len": \
                            constants.INDEX_CONSTANTS \
                                [constants.INDEX_NAME]['stats']['avg_doc_length'],
                        "no_of_docs": \
                            constants.INDEX_CONSTANTS[constants.INDEX_NAME]['total_docs'],
                        "k1": 1.5,
                        "k2": 80,
                        "b": 0.75
                    },
                    "script": "bm25",
                    "lang": "groovy"
                }
            }
        }
    }


def LM_BODY(q):
    return {
        "query": {
            "function_score": {
                "boost_mode": "replace",
                "query": {
                    "match": {
                        "_all": " ".join(q)
                    }
                },
                "script_score": {
                    "params": {
                        "field": "text",
                        "terms": q,
                        "len_field": "doc_length",
                        "V": constants.INDEX_CONSTANTS[constants.INDEX_NAME] \
                            ['stats']['vocab_len']
                    },
                    "script": "lm",
                    "lang": "groovy"
                }
            }
        }
    }


def JM_BODY(q):
    return {
        "query": {
            "function_score": {
                "boost_mode": "replace",
                "query": {
                    "match": {
                        "_all": " ".join(q)
                    }
                },
                "script_score": {
                    "params": {
                        "field": "text",
                        "terms": q,
                        "len_field": "doc_length",
                        "lambda": 0.5,
                        "total_tokens": \
                            constants.INDEX_CONSTANTS[constants.INDEX_NAME] \
                                ['stats']['total_tokens']
                    },
                    "script": "jm",
                    "lang": "groovy"
                }
            }
        }
    }


def saveResults(score_fuction):
    filename = score_fuction + "_results.txt"
    with open(filename, 'wb') as f:
        for n, q in QUERY_MAP:
            root = es_utility.getRootQuery(" ".join(q))
            if score_fuction == "okapi":
                body = OKAPI_BODY(root)
            elif score_fuction == "tfidf":
                body = TFIDF_BODY(root)
            elif score_fuction == "bm25":
                body = BM25_BODY(root)
            elif score_fuction == "lm":
                body = LM_BODY(root)
            elif score_fuction == 'jm':
                body = JM_BODY(root)
            else:
                print "undefined..."
            res = constants.ES_CLIENT.search \
                (index=constants.INDEX_NAME,
                 body=body,
                 size=constants.RESULT_SIZE,
                 analyzer="aplyzer")

            rank = 1
            for hit in res['hits']['hits']:
                line = "%d  Q0  %s  %d  %s  Exp\n" % (n, str(hit["_id"]), rank, str(hit["_score"]))
                f.write(line)
                rank += 1
    print filename + " save complete ..."


if __name__ == "__main__":
    saveResults("okapi")
    saveResults("tfidf")
    saveResults("bm25")
    saveResults("lm")
    saveResults("jm")
