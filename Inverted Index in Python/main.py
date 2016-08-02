# -------------------------------------------------------------------------------
# Name:        main
# Purpose:     runs the retrieval models
#
# Author:      Animesh Pandey
#
# Created:     25/02/2015
# Copyright:   (c) Animesh Pandey 2015
# -------------------------------------------------------------------------------

import searchConstants, constants
import retrieval_util
import score
import util
import multiprocessing as mp
from decimal import *
import itertools

qno = [85, 59, 56, 71, 64, 62, 93, 99, 58, 77, 54, 87, 94, 100, 89, 61, 95, 68, 57, 97, 98, 60, 80, 63, 91]
QUERY_LIST = retrieval_util.readQueryFile(searchConstants.QUERY_FILE)


if __name__ == "__main1__":
    mp.freeze_support()
    for model in searchConstants.MODELS[:3]:
        result_filename = model + '_results_index_' + \
                          str(searchConstants.INDEX_TYPE) + '.txt'
        with open(result_filename, 'w') as r:
            print "beginning with", model, "model..."
            for query_no, query in zip(qno, QUERY_LIST):
                print "starting retrieval for query", query_no
                if searchConstants.REMOVE_STOP_WORDS_QUERY:
                    query = util.removeStopWords(query, constants.STOP_LIST)
                if searchConstants.STEM_QUERY:
                    query = util.stemTokens(query)
                retrieved_docs = retrieval_util.scorer(query, model)
                final_docs = sorted(retrieved_docs.items(), key=lambda x: x[1], \
                                    reverse=True)[:searchConstants.NO_OF_TOP_RESULTS]
                rank = 0
                for doc in final_docs:
                    rank += 1
                    line = "{queryno}\tQ0\t{docno}\t{rank}\t{score}\tExp" \
                        .format(queryno=query_no, docno=doc[0], \
                                rank=rank, score=doc[1])
                    r.write(line + constants.ENDLINE)

if __name__ == '__main__':
    data = []
    try:
        with open('rsv_results_prox.txt', 'r') as f:
            for line in f:
                query_no, _, docid, _, rsv_score, _ = line.split('\t')
                data.append((query_no, {docid: Decimal(rsv_score)}))

        super_stuff = dict()
        for q in qno:
            temp = dict()
            for stuff in data:
                if int(stuff[0]) == q:
                    temp.update(stuff[1])
            super_stuff.update({q: temp})

        with open('prox_results_index_4.txt', 'w') as r:
            for query_no, query in zip(qno, QUERY_LIST):
                print "with query number", query_no
                new_stuff = dict()
                if searchConstants.REMOVE_STOP_WORDS_QUERY:
                    query = util.removeStopWords(query, constants.STOP_LIST)
                if searchConstants.STEM_QUERY:
                    query = util.stemTokens(query)
                docs = super_stuff[query_no]
                for doc in docs:
                    rsv_score = super_stuff[query_no][doc]
                    new_stuff.update({doc: retrieval_util.TPRSV(searchConstants.DOC_ID_MAP[doc], \
                                                                query, rsv_score)})

                final_docs = sorted(new_stuff.items(), key=lambda x: x[1], \
                                    reverse=True)

                rank = 0
                for doc in final_docs:
                    rank += 1
                    line = "{queryno}\tQ0\t{docno}\t{rank}\t{score}\tExp" \
                        .format(queryno=query_no, docno=doc[0], \
                                rank=rank, score=doc[1])
                    r.write(line + constants.ENDLINE)
                print "Query", query_no, "is complete!"

    except KeyboardInterrupt:
        print "There was a keyboard interrupt."
