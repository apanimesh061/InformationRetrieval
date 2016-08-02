# -------------------------------------------------------------------------------
# Name:        tfidf
# Purpose:     performs search by rank using a retrieval model
#
# Author:      Animesh Pandey
#
# Created:     24/02/2015
# Copyright:   (c) Animesh Pandey 2015
# -------------------------------------------------------------------------------

import searchConstants
import math
import retrieval_util
from collections import Counter
import util, constants


def okapi_tf(term, doc, tf):
    return tf / (tf + 0.5 + (1.5 * searchConstants.DOC_LEN_MAP[doc] / searchConstants.META_DATA['average_doc_len']))


def tfidf_score(doc, query):
    score = 0.0
    for token in query:
        D = searchConstants.META_DATA['total_docs']
        term_stats = retrieval_util.getTermStats(token)
        df = term_stats['DF']
        try:
            tf = term_stats['TF_vector'][doc]
        except KeyError:
            tf = 0
        score += (okapi_tf(token, doc, tf) * math.log(D / df))
    return score


def bm25_score(doc, query):
    score = 0.0
    k1 = 1.5
    k2 = 85
    b = 0.75

    all_tfs = Counter(query)
    for token in query:
        D = searchConstants.META_DATA['total_docs']
        df = retrieval_util.getTermStats(token)['DF']
        doc_len = searchConstants.DOC_LEN_MAP[doc]
        avg_doc_len = searchConstants.META_DATA['average_doc_len']
        term_stats = retrieval_util.getTermStats(term)
        try:
            tf = term_stats['TF_vector'][doc]
        except KeyError:
            tf = 0
        exp1 = math.log((D + 0.5) / (df + 0.5))
        exp2 = ((1 + k1) * tf) / \
               (tf + k1 * ((1 - b + (b * (doc_len / avg_doc_len)))))
        exp3 = (all_tfs[token] * (1 + k2)) / (all_tfs[token] + k2)
        score += (exp1 * exp2 * exp3)
    return score


def laplace_lm_score(doc, query):
    score = 0.0
    for token in query:
        doc_len = searchConstants.DOC_LEN_MAP[doc]
        term_stats = retrieval_util.getTermStats(term)
        try:
            tf = term_stats['TF_vector'][doc]
        except KeyError:
            tf = 0
        max_likelihood_term = tf + 1 / doc_len + \
                              searchConstants.META_DATA['vocab_len']
        score += math.log(max_likelihood_term)
    return score


def score(doc, query, model):
    if model == 'tfidf':
        return tfidf_score(doc, query)
    elif model == 'bm25':
        return bm25_score(doc, query)
    elif model == 'laplace':
        return laplace_lm_score(doc, query)
    elif model == 'prox':
        return retrieval_util.RSV(doc, query)


def getTopDocsFor(query, model):
    retrieved_docs = []
    doc_list = retrieval_util.getDocsForQuery(query)
    for doc in doc_list:
        retrieved_docs.append((doc, score(int(doc), query, model)))
    return sorted(retrieved_docs, \
                  key=lambda x: x[1], reverse=True) \
        [:searchConstants.NO_OF_TOP_RESULTS]


def worker(data):
    doc = int(data[0])
    query = data[1]
    return (doc, tfidf_score(doc, query))


if __name__ == '__main1__':
    query = [u'a', u'computer', u'application', u'to', u'crime', u'solving.']
    if searchConstants.REMOVE_STOP_WORDS_QUERY:
        query = util.removeStopWords(query, constants.STOP_LIST)
    if searchConstants.STEM_QUERY:
        query = util.stemTokens(query)
    all_docs = list(getDocsForQuery(query))
    mp.freeze_support()
    p = mp.Pool()
    stuff = p.map(worker, ((doc, query) for doc in all_docs))
    p.close()
    p.join()
