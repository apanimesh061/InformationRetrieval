# -------------------------------------------------------------------------------
# Name:        dataset
# Purpose:     creates the training and testing dataset
#
# Author:      Animesh Pandey
#
# Created:     15/04/2015
# Copyright:   (c) Animesh Pandey 2015
# -------------------------------------------------------------------------------

from __future__ import division
from elasticsearch import Elasticsearch
import util, es_utility, math, re
import constants
from collections import (defaultdict, Counter)
from decimal import Decimal
from hashlib import md5
import cPickle


def get_page_ranks():
    pr_dict = dict()
    with open('page_ranks.txt', 'rb') as prf:
        for line in prf:
            page, page_rank = line.split('_____')
            pr_dict.update({page: Decimal(page_rank)})
    return pr_dict


def save_meta_data():
    TOTAL_TOKENS = es_utility.getTotalTokens()
    TOTAL_DOCS = es_utility.getIndexLength()
    VOCAB_LEN = es_utility.vocabLength()
    temp_d = {
        constants.INDEX_NAME: {
            "total_docs": TOTAL_DOCS,
            "stats": {
                "total_tokens": TOTAL_TOKENS,
                "avg_doc_length": TOTAL_TOKENS / TOTAL_DOCS,
                "vocab_len": VOCAB_LEN
            }
        }
    }

    util.saveJSON(constants.GLOBAL_JSON_FILE, temp_d)


def get_qrel_docs():
    ret_docs = defaultdict(lambda: {})
    with open(constants.QREL_FILENAME, 'rb') as qff:
        for line in qff:
            try:
                ##                qid, _, docid, relevance = line.split()
                qid, _, docid, relevance = re.compile(r"\s+").split(line.strip())
            except:
                pass
            temp = ret_docs[int(qid)]
            temp.update({docid: int(relevance)})
            ret_docs[int(qid)] = temp
    return ret_docs


def get_good_ids(q_list):
    qq = get_qrel_docs()
    f = []
    for qid in q_list:
        rel_docs = qq[qid]
        f.append((qid, len(rel_docs)))
    return [i[0] for i in sorted(f, key=lambda a: a[1], reverse=True)]


def tf_idf(query, docid):
    root = es_utility.getRootQuery(query)
    term_vector = es_utility.getTermVector(docid)
    if not term_vector['found']:
        return 0.0
    average_doc_len = \
        constants.INDEX_CONSTANTS[constants.INDEX_NAME]['stats']['avg_doc_length']
    doc_len = len(term_vector['term_vectors']['text']['terms'])
    score = 0.0
    for term in root:
        try:
            tf = term_vector['term_vectors']['text']['terms'][term]['term_freq']
        except KeyError:
            continue
        df = term_vector['term_vectors']['text']['terms'][term]['doc_freq']
        okapi_tf = tf / (tf + 0.5 + (1.5 * (doc_len / average_doc_len)))
        score += okapi_tf * math.log(constants.INDEX_CONSTANTS[constants.INDEX_NAME]['total_docs'] / df)
    return score


def bm_25(query, docid):
    k1 = 1.4
    k2 = 70
    b = 0.8
    root = es_utility.getRootQuery(query)
    query_vector = Counter(root)
    term_vector = es_utility.getTermVector(docid)
    if not term_vector['found']:
        return 0.0
    average_doc_len = \
        constants.INDEX_CONSTANTS[constants.INDEX_NAME]['stats']['avg_doc_length']
    doc_len = len(term_vector['term_vectors']['text']['terms'])
    score = 0.0
    for term in root:
        try:
            tf = term_vector['term_vectors']['text']['terms'][term]['term_freq']
        except KeyError:
            continue
        df = term_vector['term_vectors']['text']['terms'][term]['doc_freq']
        tfq = query_vector[term]

        log_term = math.log(12316 + 0.5 / df + 0.5)
        doc_term = (tf * (1 + k1)) / (tf + (k1 * ((1 - b) + b * (doc_len / average_doc_len))))
        query_term = (tfq * (1 + k2)) / (tfq + k2)
        score += (log_term * doc_term * query_term)
    return score


def lm_laplace(query, docid):
    root = es_utility.getRootQuery(query)
    term_vector = es_utility.getTermVector(docid)
    if not term_vector['found']:
        return 0.0
    V = constants.INDEX_CONSTANTS[constants.INDEX_NAME]['stats']['vocab_len']
    doc_len = len(term_vector['term_vectors']['text']['terms'])
    score = 0.0
    for term in root:
        try:
            tf = term_vector['term_vectors']['text']['terms'][term]['term_freq']
        except KeyError:
            tf = 0
        max_likelihood = tf + 1 / doc_len + V
        score += math.log(max_likelihood)
    return score


def get_ttf_df(term):
    rs = constants.ES_CLIENT.search(
        index=constants.INDEX_NAME,
        size=1,
        body={"query": {"match": {"text": term}}}
    )
    try:
        term_vector = es_utility.getTermVector(rs['hits']['hits'][0]['_id'])
        return term_vector['term_vectors']['text']['terms'][term]['ttf'], \
               term_vector['term_vectors']['text']['terms'][term]['doc_freq']
    except IndexError:
        return (0, 0)


def lm_jelinek_mercer(query, docid):
    root = es_utility.getRootQuery(query)
    term_vector = es_utility.getTermVector(docid)
    if not term_vector['found']:
        return 0.0
    V = constants.INDEX_CONSTANTS[constants.INDEX_NAME]['stats']['vocab_len']
    doc_len = len(term_vector['term_vectors']['text']['terms'])
    y = 0.75
    score = 0.0
    for term in root:
        try:
            tf = term_vector['term_vectors']['text']['terms'][term]['term_freq']
        except KeyError:
            tf = 0
        try:
            ttf = term_vector['term_vectors']['text']['terms'][term]['ttf']
        except KeyError:
            ttf = get_ttf_df(term)[0]
            if not ttf:
                continue
        max_likelihood = y * tf / doc_len + (1 - y) * ttf / V
        score += math.log(max_likelihood)
    return score


def get_field_stats(query, docid):
    root = es_utility.getRootQuery(query)
    term_vector = es_utility.getTermVector(docid)
    if not term_vector['found']:
        return 0.0, 0.0, 0.0, 0.0
    doc_len = len(term_vector['term_vectors']['text']['terms'])
    sum_tf = 0
    sum_ttf = 0
    sum_df = 0
    for term in root:
        try:
            tf = term_vector['term_vectors']['text']['terms'][term]['term_freq']
            ttf = term_vector['term_vectors']['text']['terms'][term]['ttf']
            df = term_vector['term_vectors']['text']['terms'][term]['doc_freq']
        except KeyError:
            tf = 0
            df, ttf = get_ttf_df(term)
        sum_tf += tf
        sum_ttf += ttf
        sum_df += df
    return doc_len, sum_tf, sum_ttf, sum_df


if __name__ == '__main__':
    QUERY = es_utility.readQueryFile("query_desc.51-100.short.txt")
    qids = [85, 59, 56, 71, 64, 62, 93, 99, 58, 77, 54, 87, 94, 100, 89, 61, 95, 68, 57, 97, 98, 60, 80, 63, 91]

    CRAWL_QUERY = [('151801', 'what caused world war ii'),
                   ('151802', 'United States battles won in WW2'),
                   ('151803', 'battle of stalingrad')]

    QUERY = map(lambda a: a[1], CRAWL_QUERY)
    qids = map(lambda a: int(a[0]), CRAWL_QUERY)
    ##    qno = get_good_ids(qids)
    QUERY_MAP = dict(zip(qids, QUERY))
    pr_dict = get_page_ranks()
    try:
        qq = get_qrel_docs()
        training_data = open('crawl_test.csv', 'wb')
        url_hash = dict()
        training_data.write \
            ("qid,url,doc_len,sum_tf,sum_df,sum_ttf,tf_idf,bm_25,laplace,jm,page_ank,relevance\n")
        for qid in qids:
            print "Starting with query", qid
            rel_docs = qq[qid]
            for doc in rel_docs:
                try:
                    doc_hash = md5(doc).hexdigest()
                    url_hash.update({doc_hash: doc})
                    query = es_utility.getRootQuery(QUERY_MAP[qid])
                    tf_idf_score = tf_idf(query, doc)
                    bm_25_score = bm_25(query, doc)
                    lm_laplace_score = lm_laplace(query, doc)
                    lm_jelinek_mercer_score = lm_jelinek_mercer(query, doc)
                    doc_len, sum_tf, sum_ttf, sum_df = \
                        get_field_stats(query, doc)
                    page_rank = pr_dict[doc]
                    relevace = qq[qid][doc]
                    line = \
                        "{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11}\n".format(
                            str(qid),
                            doc_hash,
                            doc_len,
                            sum_tf,
                            sum_df,
                            sum_ttf,
                            tf_idf_score,
                            bm_25_score,
                            lm_laplace_score,
                            lm_jelinek_mercer_score,
                            page_rank,
                            relevace
                        )
                    training_data.write(line)
                except (KeyError, IOError, IndexError) as ex:
                    print "@", doc, "for", qid
                    template = "An exception of type {0} occured. Arguments:\n{1!r}"
                    message = template.format(type(ex).__name__, ex.args)
                    print message
                    continue
    except KeyboardInterrupt as e:
        print "Interrupt by keyboard..."
    finally:
        training_data.close()
        cPickle.dump(url_hash, open("url_hash.pkl", "wb"))
