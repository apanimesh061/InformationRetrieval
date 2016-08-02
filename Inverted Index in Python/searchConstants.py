#-------------------------------------------------------------------------------
# Name:        searchConstants
# Purpose:     this stores the constants required to be kept in memory during
#              the search/retrieval process
#
# Author:      Animesh Pandey
#
# Created:     24/02/2015
# Copyright:   (c) Animesh Pandey 2015
#-------------------------------------------------------------------------------

import util

INDEX_TYPE = 4
INDEX_DIR = "./Indices/INDEX{0}/".format(INDEX_TYPE)

REMOVE_STOP_WORDS_QUERY = False
STEM_QUERY = False
if INDEX_TYPE == 2:
    REMOVE_STOP_WORDS_QUERY = True
elif INDEX_TYPE == 3:
    STEM_QUERY = True
elif INDEX_TYPE >= 4:
    REMOVE_STOP_WORDS_QUERY = True
    STEM_QUERY = True

INDEX = INDEX_DIR + "INDEX.dat"
CATALOG = util.load_obj(INDEX_DIR + "CATALOG.pkl")
DOC_LEN_MAP = util.load_obj(INDEX_DIR + "DOC_LEN_MAP.pkl")
META_DATA = util.load_obj(INDEX_DIR + "META.pkl")
DOC_ID_MAP = util.load_obj('DOC_ID_MAP.pkl')
ID_DOC_MAP = {v : k for k, v in DOC_ID_MAP.items()}

AVG_DOC_LENGTH = META_DATA['average_doc_len']
TOTAL_DOCS = META_DATA['total_docs']
TOTAL_TOKENS = META_DATA['total_tokens']
VOCAB_LEN = META_DATA['vocab_len']

QUERY_FILE = "query_desc.51-100.short.txt"

NO_OF_TOP_RESULTS = 1000

MODELS = ['tfidf', 'bm25', 'laplace', 'rsv', 'prox_rsv']

PROXIMITY_WINDOW = 4