# -------------------------------------------------------------------------------
# Name:        email_indexer
# Purpose:     will index all email text
#
# Author:      Animesh Pandey
#
# Created:     21/04/2015
# Copyright:   (c) Animesh Pandey 2015
# -------------------------------------------------------------------------------

from __future__ import division
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
import util, es_utility, parsemail
import constants


def init_index():
    constants.ES_CLIENT.indices.create(
        index=constants.INDEX_NAME,
        body={
            "settings": {
                "index": {
                    "type": "default"
                },
                "number_of_shards": 1,
                "number_of_replicas": 1,
                "analysis": {
                    "filter": {
                        "ap_stop": {
                            "type": "stop",
                            "stopwords_path": "stoplist.txt"
                        },
                        "shingle_filter": {
                            "type": "shingle",
                            "min_shingle_size": 2,
                            "max_shingle_size": 5,
                            "output_unigrams": True
                        }
                    },
                    "analyzer": {
                        constants.ANALYZER_NAME: {
                            "type": "custom",
                            "tokenizer": "standard",
                            "filter": ["standard",
                                       "ap_stop",
                                       "lowercase",
                                       "shingle_filter",
                                       "snowball"]
                        }
                    }
                }
            }
        }
    )

    press_mapping = {
        constants.TYPE_NAME: {
            "properties": {
                "subject": {
                    "type": "string",
                    "store": True,
                    "index": "analyzed",
                    "term_vector": "with_positions_offsets_payloads",
                    "search_analyzer": constants.ANALYZER_NAME,
                    "index_analyzer": constants.ANALYZER_NAME
                },
                "exact_subject": {
                    "include_in_all": False,
                    "type": "string",
                    "store": False,
                    "index": "not_analyzed"
                },
                "text": {
                    "type": "string",
                    "store": True,
                    "index": "analyzed",
                    "term_vector": "with_positions_offsets_payloads",
                    "search_analyzer": constants.ANALYZER_NAME,
                    "index_analyzer": constants.ANALYZER_NAME
                },
                "exact_text": {
                    "include_in_all": False,
                    "type": "string",
                    "store": False,
                    "index": "not_analyzed"
                },
                "raw_email": {
                    "type": "string",
                    "store": True,
                    "index": "not_analyzed"
                },
                "content_length": {
                    "type": "long",
                    "store": True,
                    "index": "not_analyzed"
                }
            }
        }
    }

    constants.ES_CLIENT.indices.put_mapping(
        index=constants.INDEX_NAME,
        doc_type=constants.TYPE_NAME,
        body=press_mapping
    )


def streamFromDoc():
    for response, result in streaming_bulk(
            constants.ES_CLIENT,
            parsemail.stream_emails(),
            index=constants.INDEX_NAME,
            doc_type=constants.TYPE_NAME,
            chunk_size=50):

        action, result = result.popitem()
        doc_id = '/%s/document/%s' % (constants.INDEX_NAME, result['_id'])
        if not response:
            print 'Failed to %s document %s: %r' % (action, doc_id, result)
        else:
            print doc_id


def streamIndexer():
    if constants.ES_CLIENT.indices.exists(constants.INDEX_NAME):
        print "As index already exists, what would you prefer?"
        print "Press D to delete the index..."
        print "Press C to exit..."
        choice = raw_input()
        if choice == "D" or choice == "d":
            print("deleting '%s' index..." % (constants.INDEX_NAME))
            res = constants.ES_CLIENT.indices.delete(index=constants.INDEX_NAME)
            print(" response: '%s'" % (res))
        else:
            exit()
    else:
        print constants.INDEX_NAME + " does not exist..."
        print "Initialising " + constants.INDEX_NAME + "..."
        init_index()

    print "Beginning stream indexing..."
    streamFromDoc()


if __name__ == "__main__":
    streamIndexer()
