# -------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      Animesh
#
# Created:     16/04/2015
# Copyright:   (c) Animesh 2015
# Licence:     <your licence>
# -------------------------------------------------------------------------------

import csv
import numpy as np
import pandas as pd
import cPickle
from collections import defaultdict

feature_matrix = np.empty((0, 9), dtype='float')
label_array = np.array([], dtype='int')
query_id = []


def csv_reader(file_obj):
    global feature_matrix
    global label_array
    global query_id
    reader = csv.reader(file_obj)
    for l, row in enumerate(reader):
        query_id.append((row[0], row[1]))
        features = row[2:-1]
        ##        if l % 4 == 0:
        ##            csv.writer(new_csv, delimiter=',').writerow(row)
        ##        else:
        ##            csv.writer(new_csv1, delimiter=',').writerow(row)

        feature_matrix = np.vstack((feature_matrix, np.array(features)))
        label = row[-1]
        label_array = np.append(label_array, label)


if __name__ == "__main__":
    csv_path = "test_svm_crawl.csv"
    ##    new_csv = open("test_svm_crawl.csv", 'wb')
    ##    new_csv1 = open("train_svm_crawl.csv", 'wb')
    with open(csv_path, "rb") as f_obj:
        f_obj.next()
        csv_reader(f_obj)

    ##    new_csv.close()
    ##    new_csv1.close()

    ##    exit()

    feature_matrix = feature_matrix.astype(float)
    label_array = label_array.astype(int)

    """
    document_length, sum_tf, sum_df, sum_ttf, tf_idf_score, bm_25_score,
        lm_laplace_score, lm_jelinek_mercer_score
    """
    model = cPickle.load(open("svm_model_crawl.pkl", "rb"))
    url_hash = cPickle.load(open("url_hash.pkl", "rb"))

    final = defaultdict(lambda: [])
    final2 = defaultdict(lambda: {})
    for (qid, docid), (row, rel) in zip(query_id, zip(feature_matrix, label_array)):
        label = model.predict(row)
        final[qid].append((docid, label))
        final2[qid].update({docid: rel})

    blah = open('testingSVM_train_crawl.txt', 'wb')
    for query in final:
        ranking = sorted(final[query], key=lambda x: x[1], reverse=True)
        for count, rank in enumerate(ranking):
            line = '{0} {1} {2} {3} {4} {5}\n'.format(
                query,
                'Q0',
                url_hash[rank[0]],
                count + 1,
                rank[1][0],
                'Exp'
            )
            blah.write(line)
    blah.close()
