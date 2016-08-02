# -------------------------------------------------------------------------------
# Name:        linear_regression_test
# Purpose:     testing the model genreated from training dataset
#
# Author:      Animesh Pandey
#
# Created:     15/04/2015
# Copyright:   (c) Animesh Pandey 2015
# -------------------------------------------------------------------------------

import csv
import numpy as np
import pandas as pd
import statsmodels.api as sm
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
    for row in reader:
        query_id.append((row[0], row[1]))
        features = row[2:-1]
        feature_matrix = np.vstack((feature_matrix, np.array(features)))
        label = row[-1]
        label_array = np.append(label_array, label)


if __name__ == "__main__":
    df_adv = pd.read_csv('crawl_test.csv', index_col=0)
    X = df_adv[['doc_len', 'sum_tf', 'sum_df', \
                'sum_ttf', 'tf_idf', 'bm_25', 'laplace', 'jm', 'page_ank']]
    y = df_adv['relevance']
    df_adv.head()
    X = sm.add_constant(X)
    est = sm.OLS(y, X).fit().params

    coef = np.asarray([i for i in est])

    csv_path = "crawl_test.csv"
    with open(csv_path, "rb") as f_obj:
        f_obj.next()
        csv_reader(f_obj)

    feature_matrix = feature_matrix.astype(float)
    label_array = label_array.astype(int)

    """
    document_length, sum_tf, sum_df, sum_ttf, tf_idf_score, bm_25_score,
        lm_laplace_score, lm_jelinek_mercer_score
    """
    model = cPickle.load(open("lin_reg.pkl", "rb"))

    final = defaultdict(lambda: [])
    final2 = defaultdict(lambda: {})
    for (qid, docid), (row, rel) in zip(query_id, zip(feature_matrix, label_array)):
        label = np.dot(coef[1:], row) + coef[0]
        final[qid].append((docid, label))
        final2[qid].update({docid: rel})

    url_hash = cPickle.load(open("url_hash.pkl", "rb"))

    blah = open('train_crawl.txt', 'wb')
    for query in final:
        ranking = sorted(final[query], key=lambda x: x[1], reverse=True)
        for count, rank in enumerate(ranking):
            line = '{0} {1} {2} {3} {4} {5}\n'.format(
                query,
                'Q0',
                url_hash[rank[0]],
                count + 1,
                rank[1],
                'Exp'
            )
            blah.write(line)
    blah.close()
