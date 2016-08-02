# -------------------------------------------------------------------------------
# Name:        read_from_db
# Purpose:     read data from DB and save to Corpus
#
# Author:      Animesh Pandey
#
# Created:     23/03/2015
# Copyright:   (c) Animesh Pandey 2015
# -------------------------------------------------------------------------------

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
import cleanText
from cgi import escape
from bs4 import BeautifulSoup

TEST_ID = '70f6c8b8555a0c320113e61c1559b5a7'


class url_mapping:
    def get_url(self, url_id):
        return map_table.find_one({'_id': url_id})['url']


class in_link_data:
    def get_data(self, url_id):
        data = il_table.find_one({'_id': url_id})
        try:
            inlinks = [i.keys()[0] for i in data['inlinks']]
        except:
            temp_inlinks = [{i[0]: i[1]} for i in data['inlinks'].items()]
            inlinks = [i.keys()[0] for i in temp_inlinks]
        return [str(hashmap.get_url(i)) for i in inlinks]


class out_link_data:
    def get_outlinks(self, hash_list):
        return [str(hashmap.get_url(i)) for i in hash_list]

    def get_clean_content(self, html):
        return cleanText.clean_text(html)

    def get_data(self, data):
        url_id = data['_id']
        text = self.get_clean_content(data['content']).encode('utf-8')
        header = "".join(data['header']).encode('utf-8')
        raw_html = data['content'].encode('utf-8')
        soup = BeautifulSoup(data['content'])
        try:
            title = soup.find('title').getText()
        except:
            title = ''
        outlinks = self.get_outlinks(set(data['outlinks']))
        inlinks = inlink_data.get_data(url_id)
        ##            '_id' : str(hashmap.get_url(url_id)),
        return {
            'docno': str(hashmap.get_url(url_id)),
            'title': str(title.encode('ascii', 'ignore')),
            'text': text,
            'outlinks': outlinks,
            'inlinks': inlinks,
            'header': str(header.encode('ascii', 'ignore')),
            'raw_html': raw_html
        }


def saveToFile(json_object, file_obj):
    f.write("<DOC>\n")

    f.write("<DOCNO>" + json_object.get('docno', '') + "</DOCNO>\n")

    f.write("<TITLE>" + json_object.get('title', '') + "</TITLE>\n")

    f.write("<HEADER>\n")
    f.write(json_object.get('header', '').decode('ascii', 'ignore'))
    f.write("\n</HEADER>\n")

    f.write("<TEXT>\n")
    f.write(json_object.get('text', '').decode('ascii', 'ignore'))
    f.write("\n</TEXT>\n")

    f.write("<CONTENT>\n")
    f.write(json_object.get('raw_html', '').decode('ascii', 'ignore'))
    f.write("\n</CONTENT>\n")

    f.write("<INLINK>" + json_object.get('inlinks', '') + "</INLINK>\n")

    f.write("<OUTLINK>" + json_object.get('outlinks', '') + "</OUTLINK>\n")

    f.write("</DOC>\n")


def stream_from_db():
    for count, datum in enumerate(ol_table.find()):
        try:
            json_object = outlink_data.get_data(datum)
            yield json_object
            if count == 100:
                exit()
        except Exception as e:
            err_url = hashmap.get_url(datum['_id'])
            print "Error at", err_url, e


def init_index():
    ES_CLIENT.indices.create(
        index=INDEX_NAME,
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
                        "my_english": {
                            "type": "english",
                            "stopwords_path": "stoplist.txt"
                        }
                    }
                }
            }
        }
    )

    mapping = {
        DOC_TYPE: {
            "properties": {
                "docno": {
                    "type": "string",
                    "store": True,
                    "index": "not_analyzed"
                },
                "title": {
                    "type": "string",
                    "store": True,
                    "index": "analyzed",
                    "term_vector": "with_positions_offsets_payloads",
                    "analyzer": "my_english"
                },
                "text": {
                    "type": "string",
                    "store": True,
                    "index": "analyzed",
                    "term_vector": "with_positions_offsets_payloads",
                    "analyzer": "my_english"
                },
                "in_links": {
                    "type": "string",
                    "store": True,
                    "index": "no"
                },
                "out_links": {
                    "type": "string",
                    "store": True,
                    "index": "no"
                },
                "header": {
                    "type": "string",
                    "store": True
                },
                "raw_html": {
                    "type": "string",
                    "store": True
                }
            }
        }
    }

    ES_CLIENT.indices.put_mapping(
        index=INDEX_NAME,
        doc_type=DOC_TYPE,
        body=mapping
    )


def bulk_stream():
    for response, result in streaming_bulk(
            ES_CLIENT,
            stream_from_db(),
            index=INDEX_NAME,
            doc_type=DOC_TYPE,
            chunk_size=100,
            raise_on_error=False):

        action, result = result.popitem()
        doc_id = '/%s/document/%s' % (INDEX_NAME, result['_id'])
        if not response:
            print('Failed to %s document %s: %r' % (action, doc_id, result))
        else:
            print(doc_id)


def indexFromFile():
    if ES_CLIENT.indices.exists(INDEX_NAME):
        print "As index already exists, what would you prefer?"
        print "Press D to delete the index..."
        print "Press R to retain and start indexing..."
        choice = raw_input()
        if choice == "D" or choice == "d":
            print "deleting '%s' index..." % (INDEX_NAME)
            res = ES_CLIENT.indices.delete(index=INDEX_NAME)
            print " response: '%s'" % (res)
        else:
            bulk_stream()
    else:
        print INDEX_NAME + " does not exist..."
        print "Initialising " + INDEX_NAME + "..."
        init_index()
        bulk_stream()


def doc_exists(doc_id):
    try:
        return ES_CLIENT.get(
            index=INDEX_NAME,
            id=doc_id,
            doc_type=DOC_TYPE
        )['found']
    except:
        return False


def get_from_index(doc_id):
    return ES_CLIENT.get(
        index=INDEX_NAME,
        id=doc_id,
        doc_type=DOC_TYPE
    )


def streamFromDoc():
    for data in stream_from_db():
        url = data['docno']
        if not doc_exists(url):
            print "Indexing URL", url
            ES_CLIENT.index(
                index=INDEX_NAME,
                doc_type=DOC_TYPE,
                id=url,
                body=data
            )
        else:
            print "Re-indexing URL", url
            ES_CLIENT.update(
                index=INDEX_NAME,
                doc_type=DOC_TYPE,
                id=url,
                body={
                    "detect_noop": True,
                    "script": "update_inlinks",
                    "params": {
                        "new_inlinks": data['in_links']
                    }
                }
            )


if __name__ == '__main__':
    IP_ADDRESS = "10.0.0.17"
    ES_HOST_MASTER = {"host": IP_ADDRESS, "port": 9200}
    ES_HOST_NODE2 = {"host": IP_ADDRESS, "port": 9201}
    ES_HOST_NODE3 = {"host": IP_ADDRESS, "port": 9202}
    ES_CLIENT = Elasticsearch(hosts=[ES_HOST_MASTER], timeout=7200)
    INDEX_NAME = 'vs_dataset'
    DOC_TYPE = 'document'

    client = MongoClient('mongodb://localhost:27017/')
    db = client['wiki_data2']
    ol_table = db['outlink_data']
    il_table = db['inlink_data']
    map_table = db['url_hash']
    hashmap = url_mapping()
    outlink_data = out_link_data()
    inlink_data = in_link_data()

    ##    init_index()
    ##    streamFromDoc()
    new_il = [u'http://www.worldwar-2.net/timelines/the-americas/the-americas-index.htm',
              u'http://www.worldwar-2.net/timelines/the-americas/the-americas-index-1939.htm',
              u'http://www.worldwar-2.net/timelines/the-americas/the-americas-index-1940.htm',
              u'http://www.worldwar-2.net/timelines/the-americas/the-americas-index-1941.htm',
              u'http://www.worldwar-2.net/timelines/the-americas/the-americas-index-1942.htm',
              u'http://www.worldwar-2.net/timelines/the-americas/the-americas-index-1978.htm',
              u'http://www.worldwar-2.net/timelines/the-americas/the-americas-index-2014.htm']
    data = \
        get_from_index('http://www.worldwar-2.net/timelines/war-in-europe/western-europe/western-europe-index.htm')[
            '_source']
    ##    ES_CLIENT.update(
    ##                index=INDEX_NAME,
    ##                doc_type=DOC_TYPE,
    ##                id='http://www.worldwar-2.net/timelines/war-in-europe/western-europe/western-europe-index.htm',
    ##                body= {
    ##                    "detect_noop": True,
    ##                    "script": "update_inlinks",
    ##                    "params": {
    ##                        "new_inlinks": new_il
    ##                    }
    ##                }
    ##            )

    ##    error = open('error_urls.dat', 'a')
    ##    with open('final_corpus.dat', 'w') as f:
    ##        for count, datum in enumerate(ol_table.find()):
    ##            if count % 100 == 0:
    ##                print "Covered", count, "documents ..."
    ##            try:
    ##                json_object = outlink_data.get_data(datum)
    ##                saveToFile(json_object, f)
    ##            except Exception as e:
    ##                err_url = hashmap.get_url(datum['_id'])
    ##                print "Error at", err_url, e
    ##                error.write(err_url + "\t" + datum['_id'] + "\n")
    ##    error.close()
