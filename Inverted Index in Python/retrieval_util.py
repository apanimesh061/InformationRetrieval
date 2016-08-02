# -------------------------------------------------------------------------------
# Name:        retrieval_util
# Purpose:     utility functions for retrival models
#
# Author:      Animesh Pandey
#
# Created:     24/02/2015
# Copyright:   (c) Animesh Pandey 2015
# -------------------------------------------------------------------------------

from __future__ import division
import re, constants, searchConstants, json, collections, math, util
from itertools import product
import multiprocessing as mp
from collections import Counter
from decimal import *


def getInvListFromOffsets(offsets):
    with open(searchConstants.INDEX, "r") as f:
        f.seek(offsets[0])
        data = f.read(offsets[1])
    return data


def readQueryFile(filename):
    try:
        query_list = []
        with open(filename) as f:
            for query in f:
                # query list normalized as done for the index
                tokens = re.findall(constants.TOKENIZING_REGEX, \
                                    unicode(query.strip().lower()))
                tokens = tokens[4:]
                query_list.append(tokens)
        return query_list[:-2]
    except IOError:
        print "Query file could not be located..."


def getDBlocks(term):
    inv_list = getInvListFromOffsets(searchConstants.CATALOG[term])
    return inv_list


def getDocsForQuery(query):
    total_docs = []
    doc_set = []
    for token in query:
        d_blocks = getDBlocks(token)
        doc_list = [x.split('=')[0] for x in d_blocks.split(";")]
        ##        doc_list = map(lambda x : x.split('=')[0], d_blocks.split(';'))
        total_docs += doc_list
        doc_set.append(doc_list)
    return set(total_docs)


def getDocsForTerm(term):
    d_blocks = getDBlocks(term)
    return map(lambda x: x.split('=')[0], d_blocks.split(';'))


def keyWordPairs(query):
    cart = list(product(query, query))
    return filter(lambda x: (x[0] < x[1] and x[0] != x[1]), cart)


def getTermStats(term):
    d_blocks = getDBlocks(term)
    d_block_data = d_blocks.split(';')
    ttf = 0
    df = 0
    all_tfs = dict()
    for x in d_block_data:
        curr_tf = len(x.split('=')[1].split(','))
        ttf += curr_tf
        df += 1
        all_tfs.update({int(x.split("=")[0]): curr_tf})
    ##    ttf = reduce(lambda a, b : a + b, \
    ##            map(lambda x : len(x.split('=')[1].split(',')), d_block_data))
    ##    df = len(d_block_data)
    ##    all_tfs = zip(map(lambda x : x.split('=')[0], d_block_data), \
    ##            map(lambda x : len(x.split('=')[1].split(',')), d_block_data))
    return {"CF": ttf, "DF": df, "TF_vector": all_tfs}


def tf_idf(query):
    temp = dict()
    tfidf = 0.0
    D = searchConstants.META_DATA['total_docs']
    for token in query:
        offsets = searchConstants.CATALOG[token]
        with open(searchConstants.INDEX, "r") as f:
            f.seek(offsets[0])
            data = f.read(offsets[1])
        d_blocks = data.split(';')
        df = len(d_blocks)
        tf = 0
        for d_block in d_blocks:
            vals = d_block.split('=')
            tf = len(vals[1].split(','))
            docid = int(vals[0])
            otf = (tf / (tf + 0.5 + \
                         (1.5 * searchConstants.DOC_LEN_MAP[docid] / 454.25654833604949)))
            tfidf = otf + math.log(D / df)
            real_doc = searchConstants.ID_DOC_MAP[docid]
            try:
                temp[real_doc] = temp[real_doc] + tfidf
            except:
                temp[real_doc] = tfidf
    return temp


def bm25(query):
    temp = dict()
    score = 0.0

    k1 = 1.5
    k2 = 85
    b = 0.75
    D = searchConstants.META_DATA['total_docs']
    avg_doc_len = searchConstants.META_DATA['average_doc_len']

    all_tfs = Counter(query)
    for token in query:
        offsets = searchConstants.CATALOG[token]
        with open(searchConstants.INDEX, "r") as f:
            f.seek(offsets[0])
            data = f.read(offsets[1])
        d_blocks = data.split(';')
        df = len(d_blocks)
        tf = 0
        for d_block in d_blocks:
            vals = d_block.split('=')
            tf = len(vals[1].split(','))
            docid = int(vals[0])
            doc_len = searchConstants.DOC_LEN_MAP[docid]
            exp1 = math.log((D + 0.5) / (df + 0.5))
            exp2 = ((1 + k1) * tf) / \
                   (tf + k1 * ((1 - b + (b * (doc_len / avg_doc_len)))))
            exp3 = (all_tfs[token] * (1 + k2)) / (all_tfs[token] + k2)
            score = (exp1 * exp2 * exp3)
            real_doc = searchConstants.ID_DOC_MAP[docid]
            try:
                temp[real_doc] = temp[real_doc] + score
            except:
                temp[real_doc] = score
    return temp


def laplace(query):
    doc_score_map = dict()
    score = 0.0
    for token in query:
        offsets = searchConstants.CATALOG[token]
        with open(searchConstants.INDEX, "r") as f:
            f.seek(offsets[0])
            data = f.read(offsets[1])
        d_blocks = data.split(';')
        df = len(d_blocks)
        tf = 0
        for d_block in d_blocks:
            vals = d_block.split('=')
            tf = len(vals[1].split(','))
            docid = int(vals[0])
            doc_len = searchConstants.DOC_LEN_MAP[docid]
            max_likelihood_term = Decimal(tf + 1 / doc_len + \
                                          searchConstants.META_DATA['vocab_len'])
            score = Decimal(math.log(max_likelihood_term))
            real_doc = searchConstants.ID_DOC_MAP[docid]
            try:
                doc_score_map[real_doc] += score
            except:
                doc_score_map[real_doc] = score
    return doc_score_map


def getTermPos(term, doc):
    d_blocks = getDBlocks(term)
    d_block_data = d_blocks.split(';')
    for d_block in d_block_data:
        real_doc = searchConstants.ID_DOC_MAP[int(d_block.split('=')[0])]
        if int(d_block.split('=')[0]) == doc:
            return map(lambda a: int(a), d_block.split('=')[1].split(','))


def termPairWeight(pair_list, doc):
    total_tpi = 0.0
    weight = 0.0
    k1 = 1.2
    k = 2
    b = 0.9
    d = searchConstants.DOC_LEN_MAP[doc]
    avg_doc_len = searchConstants.META_DATA['average_doc_len']
    K = Decimal(k) * Decimal((1 - b) + (b * (d / avg_doc_len)))
    for pair in pair_list:
        total_tpi += 1.0 / (pair[0] - pair[1]) ** 2
    total_tpi = Decimal(total_tpi)
    pair_weight = Decimal(k1 + 1) * Decimal(total_tpi / (K + total_tpi))
    return pair_weight


def queryWeight(query, term):
    k3 = 1000
    tf = collections.Counter(query)[term]
    term_stats = getTermStats(term)
    df = term_stats['DF']
    return Decimal(tf / k3 + tf) * \
           Decimal(math.log((searchConstants.META_DATA['total_docs'] - df) / df))


def RSV(query):
    temp = dict()
    score = 0.0
    k1 = 1.2
    k3 = 1000
    k = 2
    b = 0.9
    D = searchConstants.META_DATA['total_docs']
    avg_doc_len = searchConstants.META_DATA['average_doc_len']

    all_tfs = Counter(query)
    for token in query:
        offsets = searchConstants.CATALOG[token]
        with open(searchConstants.INDEX, "r") as f:
            f.seek(offsets[0])
            data = f.read(offsets[1])
        d_blocks = data.split(';')
        df = len(d_blocks)
        tf = 0
        for d_block in d_blocks:
            vals = d_block.split('=')
            tf = len(vals[1].split(','))
            docid = int(vals[0])
            doc_len = searchConstants.DOC_LEN_MAP[docid]
            K = k * ((1 - b) + (b * (doc_len / avg_doc_len)))
            doc_weight = Decimal(k1 + 1) * Decimal(tf / (K + tf))
            query_weight = Decimal(all_tfs[token] / (k3 + all_tfs[token])) * \
                           Decimal(math.log((D - df) / df))
            score = doc_weight * query_weight
            real_doc = searchConstants.ID_DOC_MAP[docid]
            try:
                temp[real_doc] += score
            except:
                temp[real_doc] = score
    return temp


def TPRSV(doc, query, rsv_score):
    score = 0.0
    for pair in keyWordPairs(query):
        list1 = getTermPos(pair[0], doc)
        list2 = getTermPos(pair[1], doc)
        if not (list1 and list2):
            continue
        pos_pairs = filter(lambda x: ((abs(x[0] - x[1]) \
                                       <= searchConstants.PROXIMITY_WINDOW)), \
                           list(product(list1, list2)))
        if pos_pairs:
            pair_weight = termPairWeight(pos_pairs, int(doc))
            exp2 = min(queryWeight(query, pair[0]), \
                       queryWeight(query, pair[1]))
            score = Decimal(score) + pair_weight * exp2
    rsv_score_new = Decimal(score) + rsv_score
    return rsv_score_new


def proximityModel(query):
    doc_score_map = dict()
    retrieved_docs = RSV(query)
    print "RSV complete!"
    top_docs = sorted(retrieved_docs.items(), key=lambda x: x[1], \
                      reverse=True)[:searchConstants.NO_OF_TOP_RESULTS]
    for doc in top_docs:
        proximity_score = float(doc[1]) + TPRSV(doc[0], query)
        doc_score_map.update({doc[0]: proximity_score})
    return doc_score_map


def scorer(query, model):
    if model == 'tfidf':
        return tf_idf(query)
    elif model == 'bm25':
        return bm25(query)
    elif model == 'laplace':
        return laplace(query)
    elif model == 'rsv':
        return RSV(query)
    else:
        return proximityModel(query)


def finalScore(all_docs, query):
    score_list = []
    for doc in all_docs:
        score_list.append(RSV(int(doc), query))


def worker(data):
    doc = int(data[0])
    query = data[1]
    return (doc, scorer(doc, query))


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
    print stuff

if __name__ == '__main__':
    query = [u'a', u'computer', u'application', u'to', u'crime', u'solving.']
    if searchConstants.REMOVE_STOP_WORDS_QUERY:
        query = util.removeStopWords(query, constants.STOP_LIST)
    if searchConstants.STEM_QUERY:
        query = util.stemTokens(query)
    all_docs = list(getDocsForQuery(query))
    doc_score = scorer(query)
