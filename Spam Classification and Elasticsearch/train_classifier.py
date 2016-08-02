# -------------------------------------------------------------------------------
# Name:        train_classifier
# Purpose:     train the SPAM classifier
#
# Author:      Animesh Pandey
#
# Created:     29/04/2015
# Copyright:   (c) Animesh Pandey 2015
# -------------------------------------------------------------------------------

from sklearn import tree
from sklearn import linear_model
import csv
import numpy as np
import cPickle

##clf = tree.DecisionTreeClassifier()
clf = linear_model.LinearRegression()
feature_matrix = np.empty((0, 78), dtype='int')
label_array = np.array([], dtype='int')


def csv_reader(file_obj):
    global feature_matrix
    global label_array
    reader = csv.reader(file_obj)
    for row in reader:
        features = row[1:-1]
        feature_matrix = np.vstack((feature_matrix, np.array(features)))
        label = row[-1]
        label_array = np.append(label_array, label)


if __name__ == "__main__":
    csv_path = "new_spam_features.csv"
    with open(csv_path, "rb") as f_obj:
        f_obj.next()
        csv_reader(f_obj)

    feature_matrix = feature_matrix.astype(int)
    label_array = label_array.astype(int)

    clf.fit(feature_matrix, label_array)

    s = cPickle.dump(clf, open("lin_reg_SPAM.pkl", "wb"))
