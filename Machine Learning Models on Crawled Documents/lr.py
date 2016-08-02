# -------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      Animesh Pandey
#
# Created:     16/04/2015
# Copyright:   (c) Animesh Pandey 2015
# -------------------------------------------------------------------------------

from sklearn import tree
from sklearn import svm
import numpy as np
import cPickle
import csv

##clf = tree.DecisionTreeClassifier()
svm_model = svm.SVC(kernel='rbf', gamma=0.7)
feature_matrix = np.empty((0, 9), dtype='float')
label_array = np.array([], dtype='int')


def csv_reader(file_obj):
    global feature_matrix
    global label_array
    reader = csv.reader(file_obj)
    for row in reader:
        features = row[2:-1]
        feature_matrix = np.vstack((feature_matrix, np.array(features)))
        label = row[-1]
        label_array = np.append(label_array, label)


if __name__ == "__main__":
    csv_path = "train_svm_crawl.csv"
    with open(csv_path, "rb") as f_obj:
        f_obj.next()
        csv_reader(f_obj)

    feature_matrix = feature_matrix.astype(float)
    label_array = label_array.astype(int)

    ##    clf.fit(feature_matrix, label_array)
    svm_model.fit(feature_matrix, label_array)

    """
    document_length, sum_tf, sum_df, sum_ttf, tf_idf_score, bm_25_score,
        lm_laplace_score, lm_jelinek_mercer_score
    """
    ##    s = cPickle.dump(clf, open("dec_tree2.pkl", "wb"))
    cPickle.dump(svm_model, open("svm_model_crawl.pkl", "wb"))
    ##    sv = svm_model.support_vectors_
