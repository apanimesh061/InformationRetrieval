# -------------------------------------------------------------------------------
# Name:        UI
# Purpose:     UI for Elasticsearch using Flask
#
# Author:      Animesh Pandey
#
# Created:     15/03/2015
# Copyright:   (c) Animesh Pandey 2015
# -------------------------------------------------------------------------------

from flask import (
    Flask,
    request,
    jsonify,
    render_template,
)

import yaml
import json
from elasticsearch import Elasticsearch
from elasticsearch import client
from pymongo import MongoClient

app = Flask(__name__)


def get_url(url_id):
    return map_table.find_one({'_id': url_id})['url']


def generateESRequest(text):
    return {
        "query": {
            "match": {
                "_all": text
            }
        },
        'size': data['result_size']
    }


def getIndexLength():
    return ES_CLIENT.count(index=data['index_name'])['count']


def getRootQuery(query):
    index_alyzer = client.IndicesClient(ES_CLIENT)
    res = index_alyzer.analyze(
        index=data['index_name'],
        text=query,
        analyzer=data['query_analyzer'])
    return [str(t['token']) for t in res['tokens']]


def getDocumentList(text):
    doclist = []
    res = ES_CLIENT.search(
        index=data['index_name'],
        body=generateESRequest(text)
    )
    for hit in res['hits']['hits']:
        doclist.append(
            {get_url(hit['_id']): \
                 {"Score": hit['_score'], \
                  "Title": hit['_source']['title'], \
                  "Text": hit['_source']['text'][:500] + '...'}}
        )
    return doclist


def getTermPositions(term, url):
    a = ES_CLIENT.mtermvectors \
        (index=data['index_name'], doc_type=data['doc_type'],
         body=dict(
             ids=[url],
             parameters=dict(
                 term_statistics=True,
                 field_statistics=True,
                 fields=['text'])))['docs']
    try:
        return [i['position'] for i in a[0]['term_vectors']['text']['terms'][term]['tokens']]
    except:
        return []


@app.route('/')
def main():
    return render_template("my-form.html")


@app.route('/search', methods=['POST'])
def search_request():
    username = request.form['username']
    root = getRootQuery(username)
    doc_list = getDocumentList(" ".join(root))
    return jsonify(username=doc_list)


if __name__ == "__main__":
    CURRENT_IP = '10.103.4.138'

    data = yaml.safe_load(open('search.yaml'))

    # connect to DB to retrieve URL from hash
    mongo_client = MongoClient('mongodb://localhost:27017/')
    db = mongo_client[data['db_name']]
    map_table = db[data['url_map_collection']]

    # initialise ES nodes
    ES_HOST_MASTER = {"host": CURRENT_IP, "port": 9200}
    ES_HOST_NODE2 = {"host": CURRENT_IP, "port": 9201}
    ES_HOST_NODE3 = {"host": CURRENT_IP, "port": 9202}
    ES_CLIENT = Elasticsearch(hosts=[ES_HOST_NODE2], timeout=720)
    ##    root = getRootQuery('royal air force')
    ##    doc_list = getDocumentList(" ".join(root))
    app.run(debug=True)
