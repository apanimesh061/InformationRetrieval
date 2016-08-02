# -------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      Animesh Pandey
#
# Created:     24/03/2015
# Copyright:   (c) Animesh Pandey 2015
# -------------------------------------------------------------------------------

from __future__ import division
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
from bs4 import BeautifulSoup
import re, json
from hashlib import md5


def read_corpus():
    countDoc = 0
    startDoc = False
    endDoc = False

    startText = False
    endText = False

    startHeader = False
    endHeader = False

    startHtml = False
    endHtml = False

    with open(CORPUS, 'r') as f:
        for line in f:
            if not startDoc:
                match = re.findall(r'<DOC>', line)
                if len(match) > 0:
                    if match[0] == '<DOC>':
                        countDoc += 1
                        if countDoc == 3:
                            pass
                        startDoc = True
                        endHeader = False
                        headerChunk = []
                        endText = False
                        textChunk = []
                        endHtml = False
                        htmlChunk = []

            if startDoc:
                id_match = re.findall(r"<DOCNO>(.*?)</DOCNO>", line)
                if len(id_match) > 0:
                    dict_id_val = id_match[0].strip()

                title_match = re.findall(r"<TITLE>(.*?)</TITLE>", line)
                if title_match:
                    page_title = title_match[0].strip()

                # Check for text between HEADER tags
                start_header_match = re.findall(r"<HEADER>", line)
                if len(start_header_match) > 0:
                    startHeader = True
                    endHeader = False

                if startHeader and (not endHeader):
                    if not (line.strip() == "<HEADER>" or line.strip() == "</HEADER>"):
                        headerChunk.append(line.strip())

                end_header_match = re.findall(r'</HEADER>', line)
                if len(end_header_match) > 0:
                    startHeader = False
                    endHeader = True

                # check for text between TEXT tags.
                start_text_match = re.findall(r"<TEXT>", line)
                if len(start_text_match) > 0:
                    startText = True
                    endText = False

                if startText and (not endText):
                    if not (line.strip() == "<TEXT>" or line.strip() == "</TEXT>"):
                        textChunk.append(line.strip())

                end_text_match = re.findall(r'</TEXT>', line)
                if len(end_text_match) > 0:
                    startText = False
                    endText = True

                # check for text between CONTENT tags.
                start_html_match = re.findall(r"<CONTENT>", line)
                if len(start_html_match) > 0:
                    startHtml = True
                    endHtml = False

                if startHtml and (not endHtml):
                    if not (line.strip() == "<CONTENT>" or line.strip() == "</CONTENT>"):
                        htmlChunk.append(line.strip())

                end_html_match = re.findall(r'</CONTENT>', line)
                if len(end_html_match) > 0:
                    startHtml = False
                    endHtml = True

                inlink_match = re.findall(r"<INLINK>(.*?)</INLINK>", line)
                if inlink_match:
                    inlinks = inlink_match[0].strip()

                outlink_match = re.findall(r"<OUTLINK>(.*?)</OUTLINK>", line)
                if outlink_match:
                    outlinks = outlink_match[0].strip()

                end_match = re.findall(r"</DOC>", line)

                if end_match:
                    endDoc = True
                    startDoc = False
                    yield {
                        "_id": dict_id_val,
                        "docno": dict_id_val,
                        "title": page_title,
                        "text": " ".join(textChunk),
                        "in_links": inlinks.split('\t'),
                        "out_links": outlinks.split('\t'),
                        "header": " ".join(headerChunk),
                        "raw_html": " ".join(htmlChunk)
                    }
                    headerChunk = []
                    textChunk = []
                    htmlChunk = []


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


def doc_exists(doc_id):
    try:
        return ES_CLIENT.get(
            index=INDEX_NAME,
            id=doc_id,
            doc_type=DOC_TYPE
        )['found']
    except:
        return False


def streamFromDoc():
    for data in read_corpus():
        url = data['docno']
        url_id = md5(url).hexdigest()
        if not doc_exists(url_id):
            print "Indexing URL", url, "with ID", url_id
            ES_CLIENT.index(
                index=INDEX_NAME,
                doc_type=DOC_TYPE,
                id=url_id,
                body=data
            )
        else:
            pass
            print "Re-indexing URL", url, "with ID", url_id
            ES_CLIENT.update(
                index=INDEX_NAME,
                doc_type=DOC_TYPE,
                id=url_id,
                body={
                    "detect_noop": True,
                    "script": "update_inlinks",
                    "params": {
                        "new_inlinks": data['in_links']
                    }
                }
            )


def streamFromDoc1():
    count = 0
    for data in read_corpus():
        url = data['docno']
        print url


##        if not doc_exists(url):
####            repeat.write(url + "\n")
##            count += 1
##            if count % 100 == 0:
##                print count
##        else:
##            pass

def bulk_stream():
    for response, result in streaming_bulk(
            ES_CLIENT,
            read_corpus(),
            index=INDEX_NAME,
            doc_type=DOC_TYPE,
            chunk_size=100,
            raise_on_error=True):

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


if __name__ == '__main__':
    IP_ADDRESS = "10.0.0.17"

    ES_HOST_MASTER = {"host": IP_ADDRESS, "port": 9200}
    ES_HOST_NODE2 = {"host": IP_ADDRESS, "port": 9201}
    ES_HOST_NODE3 = {"host": IP_ADDRESS, "port": 9202}
    ES_CLIENT = Elasticsearch(hosts=[ES_HOST_NODE2], timeout=7200)

    INDEX_NAME = 'vs_dataset'
    DOC_TYPE = 'document'
    CORPUS = 'corpus2.dat'

    ##    repeat = open("repeat.dat", 'a')
    ##    streamFromDoc()
    ##    indexFromFile()
    streamFromDoc1()
##    repeat.close()
