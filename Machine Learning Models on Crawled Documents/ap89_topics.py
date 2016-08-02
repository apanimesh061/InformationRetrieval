# -------------------------------------------------------------------------------
# Name:        ap89_topics
# Purpose:     apply topic modelling ot the AP89 Corpus.
#
# Author:      Animesh Pandey
#
# Created:     18/04/2015
# Copyright:   (c) Animesh Pandey 2015
# -------------------------------------------------------------------------------

import gensim
from gensim import corpora
import logging
import constants, es_utility, util, string

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)


class APCorpus(object):
    def __iter__(self):
        for counter, docid in enumerate(constants.DOC_LIST):
            if counter % 1000 == 0 and counter:
                print "Covered {0} documents".format(counter)
            yield es_utility.get_source(docid)['text']


def createDictFromRawText(filename):
    stoplist = constants.STOP_LIST + list(string.ascii_lowercase)
    dictionary = corpora.Dictionary \
        (util.removePunctuation(line.encode('utf-8', 'ignore') \
                                .lower()).split() for line in ap_corpus)
    stop_ids = [dictionary.token2id[stopword] for stopword in stoplist if stopword in dictionary.token2id]
    once_ids = [tokenid for tokenid, docfreq in dictionary.dfs.iteritems() if docfreq == 1]
    dictionary.filter_tokens(stop_ids + once_ids)
    print "Removal of stop words done."
    dictionary.compactify()
    print "Dictionary created."
    dictionary.save(filename)


def createMMCorpusFromDict(filename):
    dictionary = corpora.Dictionary.load(filename)
    corpus = [dictionary.doc2bow(text.split()) for text in ap_corpus]
    corpora.MmCorpus.serialize(filename[:-4] + ".mm", corpus)
    print "Market Matrix created."


def performLDA(dictionaryname, corpus, chunksize, num_topics, passes):
    print "Hold your horses, LDA now begins ...."
    mm = corpora.MmCorpus(corpus)
    dictionary = corpora.Dictionary.load(dictionaryname)
    lda = gensim.models.ldamodel.LdaModel(
        corpus=mm,
        id2word=dictionary,
        num_topics=num_topics,
        update_every=0,
        chunksize=chunksize,
        passes=passes
    )
    lda.save('ap_89_{0}.model'.format(num_topics))


if __name__ == '__main__':
    ##    FILENAME = 'ap_89.dict'
    # courpus/model creation
    ##    ap_corpus = APCorpus()
    ##    createDictFromRawText(FILENAME)
    ##    createMMCorpusFromDict(FILENAME)
    ##    performLDA(FILENAME, FILENAME[:-5]+'.mm', 10000, 300, 200)

    # get topics
    lda_model = gensim.models.ldamodel.LdaModel.load('ap_89_300.model')
    # shows 20 topics with probable 20 words
    lda_model.show_topics(20, 20)

    dictionary = corpora.Dictionary.load('ap_89.dict')
    ques_vec = []
    ques_vec = dictionary.doc2bow("encryption".split(" "))
    ##    ques_vec = dictionary.doc2bow(['war'])
    topic_vec = []
    topic_vec = lda_model[ques_vec]

    for topic in topic_vec:
        print dictionary[topic[0]]
