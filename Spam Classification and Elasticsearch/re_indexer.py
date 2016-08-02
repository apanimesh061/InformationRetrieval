# -------------------------------------------------------------------------------
# Name:        re_indexer
# Purpose:     reindexing the old cralwed to ew index with n-grams
#
# Author:      Animesh Pandey
#
# Created:     29/04/2015
# Copyright:   (c) Animesh Pandey 2015
# -------------------------------------------------------------------------------

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import constants, util
from hashlib import md5
import cPickle

ALL_DOC_QUERY = {"query": {"match_all": {}}}

OTHER_ES_HOST = {"host": "localhost", "port": 9210}
OTHER_INDEX_NAME = 'backup_vs'
OTHER_TYPE_NAME = 'document'
OTHER_ANALYZER_NAME = 'english'
OTHER_ES_CLIENT = Elasticsearch(hosts=[OTHER_ES_HOST], timeout=180)

NEW_ES_HOST = {"host": "localhost", "port": 9200}
NEW_INDEX_NAME = 'spam_crawl'
NEW_TYPE_NAME = 'document'
NEW_ANALYZER_NAME = 'english'
NEW_ES_CLIENT = Elasticsearch(hosts=[NEW_ES_HOST], timeout=180)


def init_index():
    NEW_ES_CLIENT.indices.create(
        index=NEW_INDEX_NAME,
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
                        NEW_ANALYZER_NAME: {
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
        NEW_TYPE_NAME: {
            "properties": {
                "text": {
                    "type": "string",
                    "store": True,
                    "index": "analyzed",
                    "term_vector": "with_positions_offsets_payloads",
                    "search_analyzer": NEW_ANALYZER_NAME,
                    "index_analyzer": NEW_ANALYZER_NAME
                },
                "exact_text": {
                    "include_in_all": False,
                    "type": "string",
                    "store": False,
                    "index": "no",
                }
            }
        }
    }

    NEW_ES_CLIENT.indices.put_mapping(
        index=NEW_INDEX_NAME,
        doc_type=NEW_TYPE_NAME,
        body=press_mapping
    )


if __name__ == '__main__':
    count = 0
    ##    init_index()
    try:
        rs = OTHER_ES_CLIENT.search(
            index=OTHER_INDEX_NAME,
            scroll='60s',
            search_type='scan',
            size=100,
            body=ALL_DOC_QUERY
        )

        scroll_size = rs['hits']['total']
        while scroll_size:
            try:
                scroll_id = rs['_scroll_id']
                rs = OTHER_ES_CLIENT.scroll(scroll_id=scroll_id, scroll='60s')
                data = rs['hits']['hits']
                for entry in data:
                    docid = entry['_id']
                    body = {
                        "text": entry['_source']['text'],
                        "exact_text": entry['_source']['text']
                    }
                    NEW_ES_CLIENT.index(
                        index=NEW_INDEX_NAME,
                        doc_type=NEW_TYPE_NAME,
                        body=body,
                        id=docid
                    )
                    count += 1
                    print "Succesfully indexed document {0}\\{1}\\{2}". \
                        format(
                        NEW_INDEX_NAME,
                        NEW_TYPE_NAME,
                        docid
                    )
                scroll_size = len(rs['hits']['hits'])
            except Exception as e:
                print e
    except KeyboardInterrupt:
        print "Some interrupt..."
    finally:
        print count
