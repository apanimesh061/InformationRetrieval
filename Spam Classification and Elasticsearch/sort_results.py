# -------------------------------------------------------------------------------
# Name:        sort_results
# Purpose:
#
# Author:      Animesh Pandey
#
# Created:     30/04/2015
# Copyright:   (c) Animesh Pandey 2015
# -------------------------------------------------------------------------------

import csv
import constants

ff = open('top_spam.txt', 'wb')

d = dict()


def csv_reader(file_obj):
    reader = csv.reader(file_obj)
    for row in reader:
        url = row[0]
        score = row[-1]
        d.update({url: score})


with open('crawl_features_LR.csv', "rb") as f_obj:
    f_obj.next()
    csv_reader(f_obj)

sorted_dict = sorted(d.iteritems(), key=lambda x: x[1], reverse=True)

for docid in sorted_dict[:10]:
    ff.write(constants.ES_CLIENT.get(
        index=constants.INDEX_NAME,
        id=docid[0],
        doc_type=constants.TYPE_NAME)['_source']['text'].encode('ascii', 'ignore')
             )

ff.close()
