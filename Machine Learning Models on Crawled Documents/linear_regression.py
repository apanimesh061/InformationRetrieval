#-------------------------------------------------------------------------------
# Name:        linear_regression
# Purpose:     Apply linear regression to the training dataset
#
# Author:      Animesh Pandey
#
# Created:     15/04/2015
# Copyright:   (c) Animesh Pandey 2015
#-------------------------------------------------------------------------------

from sklearn import linear_model
import csv
import numpy as np
import cPickle

clf = linear_model.LinearRegression()
feature_matrix = np.empty((0, 9), dtype='float')
label_array = np.array([], dtype='int')

def csv_reader(file_obj):
    global feature_matrix
    global label_array
    reader = csv.reader(file_obj)
    for row in reader:
        features = row[2:-1]
        print features
        feature_matrix = np.vstack((feature_matrix, np.array(features)))
        label = row[-1]
        label_array = np.append(label_array, label)

if __name__ == "__main__":
    csv_path = "crawl_test.csv"
    with open(csv_path, "rb") as f_obj:
        f_obj.next()
        csv_reader(f_obj)

    feature_matrix = feature_matrix.astype(float)
    label_array = label_array.astype(int)

    clf.fit(feature_matrix, label_array)

    """
    document_length, sum_tf, sum_df, sum_ttf, tf_idf_score, bm_25_score,
        lm_laplace_score, lm_jelinek_mercer_score
    """
    s = cPickle.dump(clf, open("lin_reg_crawl.pkl", "wb"))
    coefficients = clf.coef_
