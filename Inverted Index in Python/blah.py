# -------------------------------------------------------------------------------
# Name:        indexer
# Purpose:
#
# Author:      Animesh Pandey
#
# Created:     21/02/2015
# Copyright:   (c) Animesh Pandey 2015
# -------------------------------------------------------------------------------

from __future__ import division
import itertools, constants, getTextFromDoc, util, tokenizer, re
from collections import Counter
import os, json
from stemming.porter2 import stem
from datetime import datetime
from time import time
import termStats
import sys, shutil, StringIO

DOC_ID_MAP = util.load_obj('DOC_ID_MAP.pkl')

vocab = set()
v = open(os.path.join(constants.TEMP_DIR, 'vocab.dat'), "w")
doc_len_map = dict()
total_tokens = 0
no_of_docs = 0

global_term_list = []
global_inv_index = dict()


def termPositions(lst, element):
    result = []
    offset = -1
    while True:
        try:
            offset = lst.index(element, offset + 1)
        except ValueError:
            return result
        result.append(offset)
    return result


def writeInvIndex(temp_ind, id):
    index = open(os.path.join(constants.TEMP_DIR, \
                              'index_set_' + str(id) + '.dat'), "w")
    catalog = open(os.path.join(constants.TEMP_DIR, \
                                'catalog_set_' + str(id) + '.dat'), "w")
    for term in sorted(temp_ind):
        index.seek(0, 2)
        file_start = index.tell()
        data = ";".join(["{z}={x}".format(z=i[0], \
                                          x=",".join(map(lambda val: str(val), i[1]))) \
                         for i in temp_ind[term]]) + constants.ENDLINE
        index.write(data.replace(' ', ''))
        file_end = index.tell()
        catalog_data = "{term}|{start}:{end}".format(term=term, \
                                                     start=file_start,
                                                     end=(file_end - file_start - 2)) + constants.ENDLINE
        catalog.write(catalog_data.replace(' ', ''))
    index.close()
    catalog.close()
    print "Index main written to disk."


def writeRealInvIndex(temp_ind):
    index = open(os.path.join(constants.TEMP_DIR, 'INDEX.dat'), "w")
    catalog = open(os.path.join(constants.TEMP_DIR, 'CATALOG.dat'), "w")
    for term in sorted(temp_ind):
        index.seek(0, 2)
        file_start = index.tell()
        data = ";".join(["{z}={x}".format(z=i[0], \
                                          x=",".join(map(lambda val: str(val), i[1]))) \
                         for i in temp_ind[term]]) + constants.ENDLINE
        index.write(data.replace(' ', ''))
        file_end = index.tell()
        catalog_data = "{term}|{start}:{end}".format(term=term, \
                                                     start=file_start,
                                                     end=(file_end - file_start - 2)) + constants.ENDLINE
        catalog.write(catalog_data.replace(' ', ''))
    index.close()
    catalog.close()
    print "Index real written to disk."


def writeVirtualInvInd(temp_ind):
    index = StringIO.StringIO()
    catalog = StringIO.StringIO()
    for term in sorted(temp_ind):
        index.seek(0, 2)
        file_start = index.tell()
        data = ";".join(["{z}={x}".format(z=i[0], \
                                          x=",".join(map(lambda val: str(val), i[1]))) \
                         for i in temp_ind[term]])
        index.write(data.replace(' ', ''))
        file_end = index.tell()
        catalog_data = "{term}|{start}:{end}".format(term=term, \
                                                     start=file_start, end=(file_end - file_start - 2))
        catalog.write(catalog_data.replace(' ', ''))
    index.seek(0)
    catalog.seek(0)
    print "Index written to memory."
    return index, catalog


def createTempIndexDoc():
    global vocab
    global v
    global no_of_docs
    global total_tokens

    visited = set()
    coll_count = 0
    iteration = 0
    temp_ind = dict()

    countDoc = 0
    for collection in constants.CORPUS[:1]:
        currLine = ''
        blah = ''
        startDoc = False
        endDoc = False
        startText = False
        endText = False
        coll_count += 1

        with open(collection) as f:
            for line in f:
                if not startDoc:
                    match = re.findall(r'<DOC>', line)
                    if len(match) > 0:
                        if match[0] == '<DOC>':
                            startDoc = True
                            endText = False
                            textChunk = []

                if startDoc:
                    id_match = re.findall(r"<DOCNO>(.*?)</DOCNO>", line)
                    if len(id_match) > 0:
                        curr_doc_no = id_match[0].strip()
                        dict_id_val = DOC_ID_MAP[curr_doc_no]
                        no_of_docs += 1

                    start_text_match = re.findall(r"<TEXT>", line)
                    if len(start_text_match) > 0:
                        startText = True
                        endText = False

                    if startText and (not endText):
                        if not (line.strip() == "<TEXT>" or \
                                            line.strip() == "</TEXT>"):
                            currLine = line.strip()
                            ## Text normalize
                            currLine = currLine.lower()
                            ##                            currLine = util.removePunctuation(currLine)
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
                        countDoc += 1
                        tokens = re.findall(constants.TOKENIZING_REGEX, \
                                            " ".join(textChunk))
                        real_tokens = tokens
                        if constants.REMOVE_STOP_WORDS:
                            tokens = util.removeStopWords(tokens, constants.STOP_LIST)
                        current_doc_len = len(tokens)
                        doc_len_map.update({dict_id_val: current_doc_len})
                        total_tokens += current_doc_len

                        if constants.STEM_DATA:
                            visited = set()
                            stemmed_tokens = util.stemTokens(tokens)
                            for token, stemmed_token in zip(tokens, stemmed_tokens):
                                if token not in vocab:
                                    vocab.add(token)
                                    v.write(token + constants.ENDLINE)
                                if stemmed_token not in visited:
                                    visited.add(stemmed_token)
                                    term_positions = termPositions(stemmed_tokens, stemmed_token)
                                    if not temp_ind.get(stemmed_token):
                                        temp_ind[stemmed_token] = \
                                            [[dict_id_val, term_positions]]
                                    else:
                                        temp_ind[stemmed_token].append \
                                            ([dict_id_val, term_positions])
                        else:
                            visited = set()
                            for token in tokens:
                                if token not in vocab:
                                    vocab.add(token)
                                    v.write(token + constants.ENDLINE)
                                if token not in visited:
                                    visited.add(token)
                                    term_positions = termPositions(real_tokens, token)
                                    if not temp_ind.get(token):
                                        temp_ind[token] = \
                                            [[dict_id_val, term_positions]]
                                    else:
                                        temp_ind[token].append \
                                            ([dict_id_val, term_positions])

                        if countDoc == 4678 and dict_id_val == 84678:
                            countDoc = 0
                            print "Will yield from collection", collection, "@", dict_id_val
                            yield temp_ind

                        if countDoc == constants.DOC_CHUNK:
                            countDoc = 0
                            print "Will yield from collection", collection, "@", dict_id_val
                            yield temp_ind
                        textChunk = []


def createTempIndex(corpusChunk):
    global vocab
    global v
    global no_of_docs
    global total_tokens

    visited = set()
    coll_count = 0
    iteration = 0
    temp_ind = dict()

    countDoc += 1
    for collection in corpusChunk:
        print "In collection", collection
        currLine = ''
        blah = ''
        startDoc = False
        endDoc = False
        startText = False
        endText = False
        coll_count += 1

        with open(collection) as f:
            for line in f:
                if not startDoc:
                    match = re.findall(r'<DOC>', line)
                    if len(match) > 0:
                        if match[0] == '<DOC>':
                            startDoc = True
                            endText = False
                            textChunk = []

                if startDoc:
                    id_match = re.findall(r"<DOCNO>(.*?)</DOCNO>", line)
                    if len(id_match) > 0:
                        curr_doc_no = id_match[0].strip()
                        dict_id_val = DOC_ID_MAP[curr_doc_no]
                        no_of_docs += 1

                    start_text_match = re.findall(r"<TEXT>", line)
                    if len(start_text_match) > 0:
                        startText = True
                        endText = False

                    if startText and (not endText):
                        if not (line.strip() == "<TEXT>" or \
                                            line.strip() == "</TEXT>"):
                            currLine = line.strip()
                            ## Text normalize
                            currLine = currLine.lower()
                            currLine = util.removePunctuation(currLine)
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
                        if countDoc == constants.DOC_CHUNK:
                            countDoc = 0
                        tokens = re.findall(constants.TOKENIZING_REGEX, \
                                            " ".join(textChunk))
                        real_tokens = tokens
                        if constants.REMOVE_STOP_WORDS:
                            tokens = util.removeStopWords(tokens, constants.STOP_LIST)
                        current_doc_len = len(tokens)
                        doc_len_map.update({dict_id_val: current_doc_len})
                        total_tokens += current_doc_len

                        if constants.STEM_DATA:
                            visited = set()
                            stemmed_tokens = util.stemTokens(tokens)
                            for token, stemmed_token in zip(tokens, stemmed_tokens):
                                if token not in vocab:
                                    vocab.add(token)
                                    v.write(token + constants.ENDLINE)
                                if stemmed_token not in visited:
                                    visited.add(stemmed_token)
                                    term_positions = termPositions(stemmed_tokens, stemmed_token)
                                    if not temp_ind.get(stemmed_token):
                                        temp_ind[stemmed_token] = \
                                            [[dict_id_val, term_positions]]
                                    else:
                                        temp_ind[stemmed_token].append \
                                            ([dict_id_val, term_positions])
                        else:
                            visited = set()
                            for token in tokens:
                                if token not in vocab:
                                    vocab.add(token)
                                    v.write(token + constants.ENDLINE)
                                if token not in visited:
                                    visited.add(token)
                                    ##                                    tf = all_tfs[token]
                                    term_positions = termPositions(real_tokens, token)
                                    if not temp_ind.get(token):
                                        temp_ind[token] = \
                                            [[dict_id_val, term_positions]]
                                    else:
                                        temp_ind[token].append \
                                            ([dict_id_val, term_positions])
                        textChunk = []
    return temp_ind


def mergeToMain(temp_ind):
    if not util.isFileEmpty(os.path.join(constants.TEMP_DIR, 'INDEX.dat')):
        writeInvIndex(temp_ind)
    else:
        writeVirtualInvInd(temp_ind)
    for id in range(2, no_of_files + 1):
        main_index = \
            open(os.path.join(constants.TEMP_DIR, 'index_set_1.dat'), "r")
        main_catalog = \
            open(os.path.join(constants.TEMP_DIR, 'catalog_set_1.dat'), "r")

        current_index = \
            open(os.path.join(constants.TEMP_DIR, \
                              'index_set_' + str(id) + '.dat'), "r")
        current_catalog = \
            open(os.path.join(constants.TEMP_DIR, \
                              'catalog_set_' + str(id) + '.dat'), "r")

        aux_index = \
            open(os.path.join(constants.TEMP_DIR, \
                              'index_set_temp.dat'), "w")
        aux_catalog = \
            open(os.path.join(constants.TEMP_DIR, \
                              'catalog_set_temp.dat'), "w")

        main_catalog_data = main_catalog.readline()
        current_catalog_data = current_catalog.readline()

        ## Compaing the Catalog data
        while main_catalog_data and current_catalog_data:
            main_term, main_term_offsets = getOffsets(main_catalog_data)
            current_term, current_term_offsets = getOffsets(current_catalog_data)
            if main_term == current_term:
                main_data = getInvListFromOffsets(main_term_offsets, constants.MAIN_INDEX_ID)
                current_data = getInvListFromOffsets(current_term_offsets, id)
                aux_index, aux_catalog = \
                    concatData(main_term, main_data, current_data, \
                               aux_index, aux_catalog, "eq")
                main_catalog_data = main_catalog.readline()
                current_catalog_data = current_catalog.readline()
            elif main_term < current_term:
                main_data = getInvListFromOffsets(main_term_offsets, constants.MAIN_INDEX_ID)
                aux_index, aux_catalog = \
                    concatData(main_term, main_data, None, \
                               aux_index, aux_catalog, "lt")
                main_catalog_data = main_catalog.readline()
            else:
                current_data = getInvListFromOffsets(current_term_offsets, id)
                aux_index, aux_catalog = \
                    concatData(current_term, None, current_data, \
                               aux_index, aux_catalog, "gt")
                current_catalog_data = current_catalog.readline()

        if main_catalog_data != '':
            while main_catalog_data:
                main_term, main_term_offsets = getOffsets(main_catalog_data)
                main_data = getInvListFromOffsets(main_term_offsets, constants.MAIN_INDEX_ID)
                aux_index, aux_catalog = \
                    concatData(main_term, main_data, None, \
                               aux_index, aux_catalog, "lt")
                main_catalog_data = main_catalog.readline()

        if current_catalog_data != '':
            while current_catalog_data:
                current_term, current_term_offsets = getOffsets(current_catalog_data)
                current_data = getInvListFromOffsets(current_term_offsets, id)
                aux_index, aux_catalog = \
                    concatData(current_term, None, current_data, \
                               aux_index, aux_catalog, "gt")
                current_catalog_data = current_catalog.readline()

        aux_index.close()
        aux_catalog.close()
        main_index.close()
        main_catalog.close()
        current_index.close()
        current_catalog.close()

        main_index = \
            open(os.path.join(constants.TEMP_DIR, 'index_set_1.dat'), "w")
        main_catalog = \
            open(os.path.join(constants.TEMP_DIR, 'catalog_set_1.dat'), "w")
        with open(os.path.join(constants.TEMP_DIR, \
                               'catalog_set_temp.dat'), "r") as aux:
            for data in aux:
                term, offsets = getOffsets(data)
                final_data = getInvListFromOffsets(offsets, "temp")
                concatData(term, final_data, None, main_index, main_catalog, 'lt')
        main_catalog.close()
        main_index.close()
        print "Temporary index_set_" + str(id) + " has been merged to main index..."


def getInvListFromOffsets(offsets):
    with open(os.path.join(constants.TEMP_DIR, 'INDEX.dat'), "r") as f:
        f.seek(offsets[0])
        data = f.read(offsets[1])
    return data


def getOffsets(data):
    term = data.split("|")[0]
    offsets = (int(data.split("|")[1].split(":")[0]), \
               int(data.split("|")[1].split(":")[1]))
    return term, offsets


def mergeToMain1(temp_ind):
    if not util.isFileEmpty(os.path.join(constants.TEMP_DIR, 'INDEX.dat')):
        writeRealInvIndex(temp_ind)
    else:
        current_index, current_catalog = writeVirtualInvInd(temp_ind)
        main_index = \
            open(os.path.join(constants.TEMP_DIR, 'INDEX.dat'), "r")
        main_catalog = \
            open(os.path.join(constants.TEMP_DIR, 'CATALOG.dat'), "r")

        aux_index = open(os.path.join(constants.TEMP_DIR, 'TEMP_INDEX.dat'), "w")
        aux_catalog = open(os.path.join(constants.TEMP_DIR, 'TEMP_CATALOG.dat'), "w")

        main_catalog_data = main_catalog.readline()
        current_catalog_data = current_catalog.readline()

        while main_catalog_data and current_catalog_data:
            main_term, main_term_offsets = getOffsets(main_catalog_data)
            current_term, current_term_offsets = getOffsets(current_catalog_data)
            if main_term == current_term:
                main_data = getInvListFromOffsets(main_term_offsets)
                current_data = getInvListFromOffsets(current_term_offsets)
                print "For", main_term, "$$$", main_data, "<===>", current_data
                aux_index, aux_catalog = \
                    concatData(main_term, main_data, current_data, \
                               aux_index, aux_catalog, "eq")
                main_catalog_data = main_catalog.readline()
                current_catalog_data = current_catalog.readline()
            elif main_term < current_term:
                main_data = getInvListFromOffsets(main_term_offsets)
                aux_index, aux_catalog = \
                    concatData(main_term, main_data, None, \
                               aux_index, aux_catalog, "lt")
                main_catalog_data = main_catalog.readline()
            else:
                current_data = getInvListFromOffsets(current_term_offsets)
                aux_index, aux_catalog = \
                    concatData(current_term, None, current_data, \
                               aux_index, aux_catalog, "gt")
                current_catalog_data = current_catalog.readline()

        if main_catalog_data != '':
            while main_catalog_data:
                main_term, main_term_offsets = getOffsets(main_catalog_data)
                main_data = getInvListFromOffsets(main_term_offsets)
                aux_index, aux_catalog = \
                    concatData(main_term, main_data, None, \
                               aux_index, aux_catalog, "lt")
                main_catalog_data = main_catalog.readline()

        if current_catalog_data != '':
            while current_catalog_data:
                current_term, current_term_offsets = getOffsets(current_catalog_data)
                current_data = getInvListFromOffsets(current_term_offsets)
                aux_index, aux_catalog = \
                    concatData(current_term, None, current_data, \
                               aux_index, aux_catalog, "gt")
                current_catalog_data = current_catalog.readline()

        aux_index.truncate()
        aux_catalog.truncate()
        aux_index.close()
        aux_catalog.close()
        main_index.close()
        main_catalog.close()
        current_index.close()
        current_catalog.close()

        os.remove(os.path.join(constants.TEMP_DIR, 'INDEX.dat'))
        os.remove(os.path.join(constants.TEMP_DIR, 'CATALOG.dat'))

        os.rename(os.path.join(constants.TEMP_DIR, 'TEMP_INDEX.dat'), \
                  os.path.join(constants.TEMP_DIR, 'INDEX.dat'), )
        os.rename(os.path.join(constants.TEMP_DIR, 'TEMP_CATALOG.dat'), \
                  os.path.join(constants.TEMP_DIR, 'CATALOG.dat'), )


def concatData(term, main_data, current_data, aux_index, aux_catalog, flag):
    if flag == "eq":
        new_meta = main_data + ";" + current_data
    ##        print "For", term
    ##        print main_data, "+", current_data, "=====>", new_meta
    elif flag == "lt":
        new_meta = main_data
    ##        print "For", term
    ##        print new_meta
    else:
        new_meta = current_data
    ##        print "For", term
    ##        print new_meta

    aux_index.seek(0, 2)
    file_start = aux_index.tell()
    data = new_meta + constants.ENDLINE
    aux_index.write(data.replace(' ', ''))
    file_end = aux_index.tell()
    catalog_data = "{term}|{start}:{end}".format(term=term, \
                                                 start=file_start, end=(file_end - file_start - 2)) + \
                   constants.ENDLINE
    aux_catalog.write(catalog_data.replace(' ', ''))
    return aux_index, aux_catalog


def copyToMainDir():
    shutil.copy(constants.TEMP_DIR + 'index_set_' + \
                str(constants.MAIN_INDEX_ID) + '.dat', constants.FINAL_DIR + "INDEX.dat")
    shutil.copy(constants.TEMP_DIR + 'catalog_set_' + \
                str(constants.MAIN_INDEX_ID) + '.dat', constants.FINAL_DIR + "CATALOG.dat")
    temp = dict()
    with open(constants.FINAL_DIR + "CATALOG.dat", 'r') as f:
        for l in f:
            line = l.strip()
            temp.update({line.split('|')[0]: \
                             (int(line.split('|')[1].split(":")[0]), \
                              int(line.split('|')[1].split(":")[1]))})
    util.createPickleFromDict(temp, constants.FINAL_DIR + "CATALOG.pkl")
    shutil.copy(constants.TEMP_DIR + 'vocab.dat', \
                constants.FINAL_DIR + "VOCAB.dat")
    shutil.copy(constants.TEMP_DIR + 'META.pkl', \
                constants.FINAL_DIR + "META.pkl")
    shutil.copy(constants.TEMP_DIR + 'DOC_LEN_MAP.pkl', \
                constants.FINAL_DIR + "DOC_LEN_MAP.pkl")


if __name__ == "__main1__":
    try:
        st = time()
        l = constants.CORPUS
        chunks = [l[x: x + constants.CORPUS_CHUNK_SIZE] \
                  for x in xrange(0, len(l), constants.CORPUS_CHUNK_SIZE)]
        iteration = 0
        for chunk in chunks:
            iteration += 1
            writeInvIndex(createTempIndex(chunk), iteration)
        print "Saving Vocab..."
        v.close()
        util.createPickleFromDict(doc_len_map, constants.TEMP_DIR + "DOC_LEN_MAP.pkl")
        it = time()
        print "Text streaming took:", it - st, "seconds"

        if not os.path.exists(constants.BACKUP_DIR):
            os.makedirs(constants.BACKUP_DIR)
        util.backup_files_to(constants.TEMP_DIR, constants.BACKUP_DIR)
        print "Back up in ", constants.BACKUP_DIR

        mergeToMain(iteration)
        et = time()
        print "Merging took:", et - it, "seconds"

        index_meta = dict()
        index_meta.update({"total_tokens": total_tokens})
        index_meta.update({"total_docs": no_of_docs})
        index_meta.update({"average_doc_len": total_tokens / no_of_docs})
        index_meta.update({"vocab_len": len(vocab)})
        util.createPickleFromDict(index_meta, constants.TEMP_DIR + "META.pkl")
        print "Meta data for index written..."

        copyToMainDir()
        ft = time()
        print "Files copied to ", constants.FINAL_DIR, "in", ft - et, "seconds"

    except KeyboardInterrupt:
        print "There was a keyboard interrupt..."

if __name__ == '__main2__':
    try:
        st = time()
        for i in createTempIndexDoc():
            writeInvIndex(i[0], i[1])
        print "Saving Vocab..."
        v.close()
        util.createPickleFromDict(doc_len_map, constants.TEMP_DIR + "DOC_LEN_MAP.pkl")

    except KeyboardInterrupt:
        print "There was a keyboard interrupt..."

if __name__ == '__main__':
    try:
        for i in createTempIndexDoc():
            mergeToMain1(i)
    except KeyboardInterrupt:
        print "There was an interrupt..."
    finally:
        v.close()
