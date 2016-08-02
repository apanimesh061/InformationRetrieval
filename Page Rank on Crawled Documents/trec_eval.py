# -------------------------------------------------------------------------------
# Name:        trec_eval
# Purpose:     Evaluation of a retrival model using TREC conventions
#
# Author:      Animesh Pandey
#
# Created:     08/04/2015
# Copyright:   (c) Animesh Pandey 2015
# -------------------------------------------------------------------------------

from __future__ import division
import re
import sys
from math import log
from collections import defaultdict
import numpy as np
import pylab as pl

"""
Following statistics will be considered:
    1. Precision
    2. Recall
    3. F1 Measure
    4. Average Precision
    5. R-precision
    6. Normalized Discounted Cumulative Gain

F:\IR\HW4>perl trec_eval.pl QREL1.txt bm25_results.txt
Queryid (Num):       25
Total number of documents over all queries
    Retrieved:    24580
    Relevant:      1832
    Rel_ret:       1026
Interpolated Recall - Precision Averages:
    at 0.00       0.6485
    at 0.10       0.4024
    at 0.20       0.3415
    at 0.30       0.2933
    at 0.40       0.2555
    at 0.50       0.2190
    at 0.60       0.1714
    at 0.70       0.1391
    at 0.80       0.1101
    at 0.90       0.0803
    at 1.00       0.0275
Average precision (non-interpolated) for all rel docs(averaged over queries)
                  0.2230
Precision:
  At    5 docs:   0.3680
  At   10 docs:   0.3440
  At   15 docs:   0.3333
  At   20 docs:   0.3080
  At   30 docs:   0.2840
  At  100 docs:   0.1936
  At  200 docs:   0.1332
  At  500 docs:   0.0728
  At 1000 docs:   0.0410
R-Precision (precision after R (= num_rel for a query) docs retrieved):
    Exact:        0.2655

FOR Group 15180:
For all 25 Queries following are the statistics:
    Retrieved Documents: 24580
    Original Relevant Documents: 1832
    Retrieved Relevant Documents: 1026

Average Precision for Top K Documents for:
	K = 5		0.368
	K = 10		0.344
	K = 15		0.333333333333
	K = 20		0.308
	K = 30		0.284
	K = 100		0.1936


Average Recall for Top K Documents for:
	K = 5		0.0528188571617
	K = 10		0.104261555728
	K = 15		0.148235920767
	K = 20		0.17099884455
	K = 30		0.20668339669
	K = 100		0.374797995176


Average F1 Measure for Top K Documents for:
	K = 5		0.0825990849018
	K = 10		0.13198125025
	K = 15		0.162459406092
	K = 20		0.172032734113
	K = 30		0.186019316109
	K = 100		0.206992731381

Average R-Precision over all queries: 0.265495798206
Average Precision over all queries: 0.222999772295

"""

LIMIT = 1000

ARGS = sys.argv
VERBOSE_OUTPUT = False
GRAPHICAL_OUTPUT = False

if len(ARGS) < 3:
    print \
        """
        trec_eval commandline error.
        Use this way
        >>> python trec_eval.py <-q> <-g> <qrel_file> <test_file>
        Add '-q' options if verbose output is required
        Add '-g' options if graphical output is required.
        """
    sys.exit(-1)

if ARGS[1] == '-q':
    VERBOSE_OUTPUT = True
    if ARGS[2] == '-g':
        GRAPHICAL_OUTPUT = True
        QREL_FILE = ARGS[3]
        TEST_FILE = ARGS[4]
    else:
        QREL_FILE = ARGS[2]
        TEST_FILE = ARGS[3]
elif ARGS[1] == '-g':
    GRAPHICAL_OUTPUT = True
    QREL_FILE = ARGS[2]
    TEST_FILE = ARGS[3]
else:
    QREL_FILE = ARGS[1]
    TEST_FILE = ARGS[2]

print \
    """
    Using QREL file as {0}
    Using TREC file as {1}
    """.format(QREL_FILE, TEST_FILE)

# For testing purpose:
##QREL_FILE = 'QREL.txt'
##TEST_FILE = 'crawl_score.txt'
##VERBOSE_OUTPUT = True
##GRAPHICAL_OUTPUT = False

## ---------------------------------------------------------------------------##

qrel = defaultdict(lambda: {})
num_rel = defaultdict(lambda: 0)
trec = defaultdict(lambda: [])


def precision_interpolation(precision):
    interpolated_precision = []
    for i in range(len(precision)):
        temp = max(precision[i:])
        interpolated_precision.append(temp)
    return interpolated_precision


def create_graph(recall, precision, filename):
    new_prec = precision_interpolation(precision)

    pl.clf()
    pl.plot(recall, precision, label='Precision-Recall curve')
    pl.plot(recall, new_prec, linestyle='-', color='r', \
            label='Interpolated Precision-Recall Curve')
    pl.xlabel('Recall')
    pl.ylabel('Precision')
    pl.ylim([0.0, 1.05])
    pl.xlim([0.0, 1.0])
    pl.title('Precision-Recall Graph for {0}'.format(filename))
    pl.grid()
    pl.legend(loc="upper right", prop={'size': 6})
    pl.savefig(filename + '.png')
    pl.show()


def F1_Measure(p, r):
    if not p or not r:
        return 0.0
    else:
        return (2 * p * r) / (p + r)


def DCG(r_vector, k):
    return r_vector[0] + sum(map(lambda a: r_vector[a] / log(a + 1), range(1, k)))


QREL = open(QREL_FILE, 'r')
for line in QREL:
    try:
        query_id, _, url, relevance = re.compile(r"\s+").split(line.strip())
    except:
        pass
    qrel[query_id][url] = \
        {
            'relevance': min(1, int(relevance)),
            'grade': int(relevance)
        }
    num_rel[query_id] += min(1, int(relevance))
QREL.close()

TREC = open(TEST_FILE, 'r')
for line in TREC:
    query_id, _, url, rank, score, _ = re.compile(r"\s+").split(line.strip())
    trec[query_id].append((url, float(score)))
TREC.close()

cutoffs = [5, 10, 20, 50, 100]

num_of_topics = 0

sum_avg_prec = 0.0
sum_r_prec = 0.0
sum_ndcg = 0.0

sum_prec_at_cutoffs = [0] * len(cutoffs)
sum_recall_at_cutoffs = [0] * len(cutoffs)
sum_f1_at_cutoffs = [0] * len(cutoffs)

tot_num_ret = 0
tot_num_rel = 0
tot_num_rel_ret = 0

for topic in sorted(trec):
    if not num_rel[topic]:
        continue

    num_of_topics += 1
    doc_score_map = trec[topic]

    prec_list = dict()
    rec_list = dict()
    f1_list = dict()

    num_ret = 0
    num_rel_ret = 0
    sum_prec = 0
    all_grades = []

    recall_axis = []
    precision_axis = []
    for docid, score in doc_score_map:  # automatic lexicographic if key
        # is same
        num_ret += 1
        if num_ret > LIMIT:
            break
        try:
            curr_rel = qrel[topic][docid]['relevance']
            all_grades.append(qrel[topic][docid]['grade'])
            if curr_rel:
                sum_prec += curr_rel * (1 + num_rel_ret) / num_ret
                num_rel_ret += 1
        except Exception as e:
            pass

        prec_list[num_ret] = num_rel_ret / num_ret
        rec_list[num_ret] = num_rel_ret / num_rel[topic]
        f1_list[num_ret] = F1_Measure(prec_list[num_ret], rec_list[num_ret])
        recall_axis.append(rec_list[num_ret])
        precision_axis.append(prec_list[num_ret])

    if GRAPHICAL_OUTPUT:
        create_graph(recall_axis, precision_axis, str(topic))

    topic_ndcg = DCG(all_grades, num_ret) / \
                 DCG(sorted(all_grades, reverse=True), num_ret)

    sum_ndcg += topic_ndcg

    avg_prec = sum_prec / num_rel[topic]
    final_recall = num_rel_ret / num_rel[topic]

    # include leftovers
    for i in range(num_ret + 1, LIMIT + 1):
        current_precision = num_rel_ret / i
        current_recall = final_recall
        prec_list[i] = current_precision
        rec_list[i] = current_recall
        f1_list[i] = F1_Measure(current_precision, current_recall)

    prec_at_cutoffs = []
    recall_at_cutoffs = []
    f1_at_cutoffs = []
    for cutoff in cutoffs:
        prec_at_cutoffs.append(prec_list[cutoff])
        recall_at_cutoffs.append(rec_list[cutoff])
        f1_at_cutoffs.append(f1_list[cutoff])

    r_prec = prec_list[int(num_rel[topic])]

    tot_num_ret += num_ret
    tot_num_rel += num_rel[topic]
    tot_num_rel_ret += num_rel_ret

    for i in range(len(cutoffs)):
        sum_prec_at_cutoffs[i] += prec_at_cutoffs[i]
        sum_recall_at_cutoffs[i] += recall_at_cutoffs[i]
        sum_f1_at_cutoffs[i] += f1_at_cutoffs[i]

    if VERBOSE_OUTPUT:
        doc_related = \
            """
            For Query {0} following are the statistics:
                Retrieved Documents: {1}
                Original Relevant Documents: {2}
                Retrieved Relevant Documents: {3}
            """.format(topic, num_ret, num_rel[topic], num_rel_ret)
        print doc_related

        print "Precision for Top K Documents for:"
        for i, j in zip(cutoffs, prec_at_cutoffs):
            print "\tK = {0}\t\t{1}".format(i, j)
        print "\n"
        print "Recall for Top K Documents for:"
        for i, j in zip(cutoffs, recall_at_cutoffs):
            print "\tK = {0}\t\t{1}".format(i, j)
        print "\n"
        print "F1 Measure for Top K Documents for:"
        for i, j in zip(cutoffs, f1_at_cutoffs):
            print "\tK = {0}\t\t{1}".format(i, j)

        query_related = \
            """
            Average R-Precision over Query {0}: {1}
            Average Precision over Query {0}: {2}
            Average nDCG over Query {0}: {3}
            """.format(topic, r_prec, avg_prec, topic_ndcg)
        print query_related

    sum_avg_prec += avg_prec
    sum_r_prec += r_prec

# Following are statistics averaged over all Queries.

# Precision @ K
avg_prec_at_cutoffs = [0] * len(cutoffs)
# Recall @ K
avg_recall_at_cutoffs = [0] * len(cutoffs)
# F1 Merasure @ K
avg_f1_at_cutoffs = [0] * len(cutoffs)
for i in range(len(cutoffs)):
    avg_prec_at_cutoffs[i] = sum_prec_at_cutoffs[i] / num_of_topics
    avg_recall_at_cutoffs[i] = sum_recall_at_cutoffs[i] / num_of_topics
    avg_f1_at_cutoffs[i] = sum_f1_at_cutoffs[i] / num_of_topics

# Average Precision
mean_avg_prec = sum_avg_prec / num_of_topics

# R-Precision
mean_r_prec = sum_r_prec / num_of_topics

# nDCG value
avg_ndcg = sum_ndcg / num_of_topics

print \
    """
    -------------------------------------------------------------------------------
    """

all_doc_related = \
    """
    For all {0} Queries following are the statistics:
        Retrieved Documents: {1}
        Original Relevant Documents: {2}
        Retrieved Relevant Documents: {3}
    """.format(num_of_topics, tot_num_ret, tot_num_rel, tot_num_rel_ret)
print all_doc_related

print "Average Precision for Top K Documents for:"
for i, j in zip(cutoffs, avg_prec_at_cutoffs):
    print "\tK = {0}\t\t{1}".format(i, j)
print "\n"
print "Average Recall for Top K Documents for:"
for i, j in zip(cutoffs, avg_recall_at_cutoffs):
    print "\tK = {0}\t\t{1}".format(i, j)
print "\n"
print "Average F1 Measure for Top K Documents for:"
for i, j in zip(cutoffs, avg_f1_at_cutoffs):
    print "\tK = {0}\t\t{1}".format(i, j)
print "\n"

all_query_related = \
    """
    Average R-Precision over all queries: {0}
    Average Precision over all queries: {1}
    Average nDCG over all queries: {2}
    """.format(mean_r_prec, mean_avg_prec, avg_ndcg)
print all_query_related
