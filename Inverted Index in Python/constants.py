# -------------------------------------------------------------------------------
# Name:        constants
# Purpose:     collection of frequently used constants
#
# Author:      Animesh Pandey
#
# Created:     25/01/2015
# Copyright:   (c) Animesh Pandey 2015
# -------------------------------------------------------------------------------
import util, glob, os

INDEX_TYPE = 6
MAIN_INDEX_ID = 1
AUX_INDEX_ID = "temp"

REMOVE_STOP_WORDS = False
STEM_DATA = False
if INDEX_TYPE == 2:
    REMOVE_STOP_WORDS = True
if INDEX_TYPE == 3:
    STEM_DATA = True
if INDEX_TYPE >= 4:
    REMOVE_STOP_WORDS = True
    STEM_DATA = True

STOP_LIST = util.getStopList("stoplist_2.txt")
DOC_LIST = util.getDocList('doclist.txt')
CORPUS_PATH = "F:/AP_DATA/ap89_collection/"
CORPUS = glob.glob(CORPUS_PATH + "ap*")
CORPUS_CHUNK_SIZE = 10
DOC_CHUNK = 10

TEMP_DIR = "./tmp{0}/".format(INDEX_TYPE)
if not os.path.exists(TEMP_DIR):
    os.makedirs(TEMP_DIR)

FINAL_DIR = "./Indices/INDEX{0}/".format(INDEX_TYPE)
if not os.path.exists(FINAL_DIR):
    os.makedirs(FINAL_DIR)

BACKUP_DIR = "./BackUp/INDEX{0}/".format(INDEX_TYPE)
if not os.path.exists(BACKUP_DIR):
    os.makedirs(BACKUP_DIR)

ENDLINE = "\n"

TOKENIZING_REGEX = r"\w+[\.?\w+]*"
