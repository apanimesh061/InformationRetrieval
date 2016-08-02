# -------------------------------------------------------------------------------
# Name:        search
# Purpose:     retrieve
#
# Author:      Animesh Pandey
#
# Created:     07/04/2015
# Copyright:   (c) Animesh Pandey 2015
# -------------------------------------------------------------------------------

from elasticsearch import Elasticsearch
from elasticsearch import client
from random import randint


def getRootQuery(query):
    index_alyzer = client.IndicesClient(ES_CLIENT)
    res = index_alyzer.analyze(
        index=INDEX_NAME,
        text=query,
        analyzer=ANALYZER_NAME)
    return [str(t['token']) for t in res['tokens']]


def getDocumentList():
    QUERIES = [('151801', 'what caused world war ii'),
               ('151802', 'United States battles won in WW2'),
               ('151803', 'battle of stalingrad')]

    f = open('result33.txt', 'w')
    for qno, query in QUERIES:
        doclist = []
        res = ES_CLIENT.search(
            index=INDEX_NAME,
            body=generateESRequest(query),
            analyzer=ANALYZER_NAME
        )

        rank = 1
        for hit in res['hits']['hits']:
            line = "%s  Q0  %s  %d  %s  Exp\n" % (qno, str(hit["_id"]).strip(), rank, str(hit["_score"]))
            ##            line = "%s  %s  %s  %d\n" % (qno, "A_Pande", str(hit["_id"]).strip(), randint(0, 2))
            print line
            f.write(line)
            rank += 1

    f.close()


def generateESRequest(query):
    root = getRootQuery(query)
    return {
        "query": {
            "match": {
                "_all": " ".join(root)
            }
        },
        "size": 200
    }


if __name__ == "__main__":
    INDEX_NAME = 'backup_vs'
    ANALYZER_NAME = 'my_english'

    CURRENT_IP = "10.103.34.251"
    ES_HOST_MASTER = {"host": CURRENT_IP, "port": 9210}
    ES_CLIENT = Elasticsearch(hosts=[ES_HOST_MASTER], timeout=720)

    getDocumentList()
