# -------------------------------------------------------------------------------
# Name:        create_test_data
# Purpose:     getting feature values from crawled pages
#
# Author:      Animesh Pandey
#
# Created:     30/04/2015
# Copyright:   (c) Animesh Pandey 2015
# -------------------------------------------------------------------------------

import constants
import es_utility
import csv
import cPickle
from elasticsearch import exceptions

ALL_DOC_QUERY = {"query": {"match_all": {}}}


def get_urls():
    rs = constants.ES_CLIENT.search(
        index=constants.INDEX_NAME,
        scroll='60s',
        search_type='scan',
        size=1,
        body=ALL_DOC_QUERY
    )

    scroll_size = rs['hits']['total']
    while scroll_size:
        try:
            scroll_id = rs['_scroll_id']
            rs = constants.ES_CLIENT.scroll(scroll_id=scroll_id, scroll='60s')
            data = rs['hits']['hits']
            for entry in data:
                docid = entry['_id']
                yield docid
            scroll_size = len(rs['hits']['hits'])
        except Exception as e:
            print e


if __name__ == '__main__':
    try:

        FEATURE_FILE = open('crawl_features_LR.csv', 'wb')
        model = cPickle.load(open("lin_reg_SPAM.pkl", "rb"))

        spam_phrases = []
        with open('spam_terms.txt', 'rb') as sf:
            for phrase in sf:
                spam_phrases.append(es_utility.getRootQuery(phrase.strip()))

        SPAM_TERMS = set(sum(spam_phrases, []))
        csv.writer(FEATURE_FILE, delimiter=','). \
            writerow(["url"] + list(SPAM_TERMS) + ['label'])

        for count, document in enumerate(get_urls()):
            data = es_utility.get_source(document)['text']
            data = data.encode('ascii', 'ignore')
            if not data:
                continue
            try:
                text_terms = es_utility.getRootQuery(data)
            except exceptions.ConnectionError as e:
                ##                print e.info
                continue
            # create a the features
            feature = []
            for term in SPAM_TERMS:
                feature.append(int(term in text_terms))

            # add the label of the current document
            # HAM by default
            doc_label = model.predict(feature)
            feature.append(doc_label)
            line = [document] + feature
            # write the FEATURE values to a file
            csv.writer(FEATURE_FILE, delimiter=',').writerow(line)
            curr_file = count + 1
            if curr_file % 1000 == 0:
                print "Covered {0} documents.".format(curr_file)
    except KeyboardInterrupt:
        print "There was a keyboard interrupt"
    finally:
        print count
        FEATURE_FILE.close()
