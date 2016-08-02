from __future__ import division
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
import util, es_utility, getTextFromDoc
import constants


def init_index():
    constants.ES_CLIENT.indices.create(
        index=constants.INDEX_NAME,
        body={
            "settings": {
                "index": {
                    "store": {
                        "type": "default"
                    },
                    "number_of_shards": 1,
                    "number_of_replicas": 1
                },
                "analysis": {
                    "analyzer": {
                        constants.ANALYZER_NAME: {
                            "type": "english",
                            "stopwords_path": "stoplist.txt"
                        }
                    }
                }
            }
        }
    )

    press_mapping = {
        constants.TYPE_NAME: {
            "properties": {
                "text": {
                    "type": "string",
                    "fields": {
                        "hash": {
                            "type": "murmur3"
                        }
                    },
                    "store": True,
                    "index": "analyzed",
                    "term_vector": "with_positions_offsets_payloads",
                    "analyzer": constants.ANALYZER_NAME
                },
                "doc_length": {
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


def parseJSON():
    data = util.loadJSON(constants.JSON_FILE)
    data_index = []
    for obj in data:
        data_dict = dict()
        temp_text = util.removePunctuation(str(data[obj]['text']))
        stopped_temp_text = util.removeStopWords(temp_text, constants.STOP_LIST)
        temp_length = len(temp_text.split(" "))
        data_dict['text'] = temp_text.lower()
        data_dict['doc_length'] = temp_length
        data_dict['doc_length_stopped'] = len(stopped_temp_text.split(" "))

        meta_data = {
            "index": {
                "_index": constants.INDEX_NAME,
                "_type": constants.TYPE_NAME,
                "_id": str(obj)
            }
        }

        data_index.append(meta_data)
        data_index.append(data_dict)

    print "Complete JSON parsed..."
    return data_index


def streamFromDoc():
    for response, result in streaming_bulk(
            constants.ES_CLIENT,
            getTextFromDoc.streamAllDocs(),
            index=constants.INDEX_NAME,
            doc_type=constants.TYPE_NAME,
            chunk_size=50,
            raise_on_error=True):

        action, result = result.popitem()
        doc_id = '/%s/document/%s' % (constants.INDEX_NAME, result['_id'])
        if not response:
            print('Failed to %s document %s: %r' % (action, doc_id, result))
        else:
            print(doc_id)


def indexFromFile():
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
            import sys
            sys.exit(0)
    else:
        print constants.INDEX_NAME + " does not exist..."
        print "Initialising " + constants.INDEX_NAME + "..."
        init_index()

    data_index = parseJSON()
    print "Data loaded!"
    print "beginning bulk indexing..."
    res = constants.ES_CLIENT.bulk \
        (index=constants.INDEX_NAME, body=data_index, refresh=True)


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
            import sys
            sys.exit(0)
    else:
        print constants.INDEX_NAME + " does not exist..."
        print "Initialising " + constants.INDEX_NAME + "..."
        init_index()

    print "Beginning stream indexing..."
    streamFromDoc()


if __name__ == "__main__":
    streamIndexer()

    TOTAL_TOKENS = es_utility.getTotalTokens()
    TOTAL_DOCS = es_utility.getIndexLength()
    VOCAB_LEN = es_utility.vocabLength()
    temp_d = {
        constants.INDEX_NAME: {
            "total_docs": TOTAL_DOCS,
            "stats": {
                "total_tokens": TOTAL_TOKENS,
                "avg_doc_length": TOTAL_TOKENS / TOTAL_DOCS,
                "vocab_len": VOCAB_LEN
            }
        }
    }

    util.saveJSON(constants.GLOBAL_JSON_FILE, temp_d)
