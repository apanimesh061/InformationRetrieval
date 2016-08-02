# -------------------------------------------------------------------------------
# Name:        getTextFromDoc
# Purpose:     Read data from file containing tags and extract DOCNO and TEXT
#
# Author:      Animesh Pandey
#
# Created:     18/02/2015
# Copyright:   (c) Animesh Pandey 2015
# -------------------------------------------------------------------------------

import sys
import re
import glob
import string
import util
import constants, tokenizer


def advancedWarfare(textChunk):
    temp_text = \
        util.removeStopWords \
            (util.removePunctuation \
                 (" ".join(textChunk)).lower(), constants.STOP_LIST)
    temp_text = util.alterSpaces(temp_text)
    temp_text = " ".join([s for s in temp_text.split(" ") if not util.num_there(s)])
    return temp_text, len(temp_text.split(" ")), util.ngramsLength(temp_text, 2)


def streamAllDocs():
    chunks = lambda lst, sz: [lst[i: i + sz] for i in range(0, len(lst), sz)]
    for chunk in chunks(constants.CORPUS, 4):
        countDoc = 0
        for doc_collection in chunk:
            currLine = ''
            blah = ''
            startDoc = False
            endDoc = False
            startText = False
            endText = False

            with open(doc_collection) as f:
                for line in f:
                    if not startDoc:
                        match = re.findall(r'<DOC>', line)
                        if len(match) > 0:
                            if match[0] == '<DOC>':
                                countChunks = 0
                                countDoc += 1
                                startDoc = True
                                endText = False
                                textChunk = []

                    if startDoc:
                        id_match = re.findall(r"<DOCNO>(.*?)</DOCNO>", line)
                        if len(id_match) > 0:
                            dict_id_val = id_match[0].strip()

                        start_text_match = re.findall(r"<TEXT>", line)
                        if len(start_text_match) > 0:
                            countChunks += 1
                            startText = True
                            endText = False

                        if startText and (not endText):
                            if not (line.strip() == "<TEXT>" or \
                                                line.strip() == "</TEXT>"):
                                currLine = line.strip()
                                currLine = currLine.lower()
                                currLine = unicode(currLine, 'utf-8')
                                currLine.decode("utf-8", 'ignore')
                                textChunk.append(currLine)

                        end_text_match = re.findall(r'</TEXT>', line)
                        if len(end_text_match) > 0:
                            startText = False
                            endText = True

                        end_match = re.findall(r"</DOC>", line)

                        if len(end_match) > 0:
                            endDoc = True
                            startDoc = False
                            tokens = re.findall(r"\w+\.?\w*", " ".join(textChunk))
                            textChunk = []
        print countDoc


def streamAllDocs1():
    countDoc = 0
    for doc_collection in constants.CORPUS:

        currLine = ''
        blah = ''
        startDoc = False
        endDoc = False
        startText = False
        endText = False

        with open(doc_collection) as f:
            for line in f:
                if not startDoc:
                    match = re.findall(r'<DOC>', line)
                    if len(match) > 0:
                        if match[0] == '<DOC>':
                            countChunks = 0
                            startDoc = True
                            endText = False
                            textChunk = []

                if startDoc:
                    id_match = re.findall(r"<DOCNO>(.*?)</DOCNO>", line)
                    if len(id_match) > 0:
                        dict_id_val = id_match[0].strip()

                    start_text_match = re.findall(r"<TEXT>", line)
                    if len(start_text_match) > 0:
                        countChunks += 1
                        startText = True
                        endText = False

                    if startText and (not endText):
                        if not (line.strip() == "<TEXT>" or \
                                            line.strip() == "</TEXT>"):
                            currLine = line.strip()
                            currLine = currLine.lower()
                            currLine = unicode(currLine, 'utf-8')
                            currLine.decode("utf-8", 'ignore')
                            textChunk.append(currLine)

                    end_text_match = re.findall(r'</TEXT>', line)
                    if len(end_text_match) > 0:
                        startText = False
                        endText = True

                    end_match = re.findall(r"</DOC>", line)

                    if len(end_match) > 0:
                        endDoc = True
                        startDoc = False
                        tokens = re.findall(r"\w+\.?\w*", " ".join(textChunk))
                        countDoc += 1
                        if countDoc == constants.DOC_CHUNK:
                            print dict_id_val
                            countDoc = 0
                        textChunk = []


def ap_tokenize(docno):
    filename = docno.split('-')[0].lower()
    currLine = ''
    blah = ''
    startDoc = False
    endDoc = False
    startText = False
    endText = False

    with open(constants.CORPUS_PATH + filename) as f:
        for line in f:
            if not startDoc:
                match = re.findall(r'<DOC>', line)
                if len(match) > 0:
                    if match[0] == '<DOC>':
                        countChunks = 0
                        startDoc = True
                        endText = False
                        textChunk = []

            if startDoc:
                id_match = re.findall(r"<DOCNO>(.*?)</DOCNO>", line)
                if len(id_match) > 0:
                    dict_id_val = id_match[0].strip()
                    if dict_id_val == docno:
                        pass

                start_text_match = re.findall(r"<TEXT>", line)
                if len(start_text_match) > 0:
                    countChunks += 1
                    startText = True
                    endText = False

                if startText and (not endText):
                    if not (line.strip() == "<TEXT>" or \
                                        line.strip() == "</TEXT>"):
                        currLine = line.strip()
                        currLine = currLine.lower()
                        currLine = unicode(currLine, 'utf-8')
                        currLine.decode("utf-8", 'ignore')
                        textChunk.append(currLine)

                end_text_match = re.findall(r'</TEXT>', line)
                if len(end_text_match) > 0:
                    startText = False
                    endText = True

                end_match = re.findall(r"</DOC>", line)

                if len(end_match) > 0:
                    endDoc = True
                    startDoc = False
                    if dict_id_val == docno:
                        print textChunk
                        tokens = re.findall(r"\w+[\.?\w+]*", " ".join(textChunk))
                    textChunk = []

    term_id_map = dict()
    tok_len = len(tokens)
    token_tuple = map(lambda x, y, z: (x, y, z), \
                      [tokenizer.createIDs(i) for i in tokens],
                      [docno] * tok_len,
                      range(1, tok_len + 1))
    map(lambda a, b: term_id_map.update({a: b}),
        [tokenizer.createIDs(i) for i in set(tokens)],
        set(tokens))
    return token_tuple, term_id_map


def writeVocabToFile(filename, l):
    with open(constants.VOCAB_FILE, 'ab') as v:
        [v.write(t + "\n") for t in l]


def convertToHashList(filename):
    LIST = util.getVocabList(filename)
    temp = dict()
    for token in LIST:
        temp.update({tokenizer.createIDs(token): token})
    util.createPickleFromDict(temp, "hash_vocab_map.pkl")


if __name__ == '__main__':
    ##    count = 0
    ##    if count == 0:
    ##        offset = 0
    streamAllDocs1()

##        [constants.VOCAB.add(t) for t in item]
##        count += 1
##        if count % 10000 == 0:
##            print "Reached document " + str(count)
##        if count % 20000 == 0:
##            print "Copying current vocab to", constants.VOCAB_FILE
##            writeVocabToFile(constants.VOCAB_FILE, list(constants.VOCAB)[offset : ])
##            offset = len(constants.VOCAB)
##    if len(list(constants.VOCAB)[offset : ]):
##        writeVocabToFile(constants.VOCAB_FILE, list(constants.VOCAB)[offset : ])
##    del(item)
##
##    convertToHashList(constants.VOCAB_FILE)

##    token_tuples, term_id_map = ap_tokenize("AP890101-0042")
