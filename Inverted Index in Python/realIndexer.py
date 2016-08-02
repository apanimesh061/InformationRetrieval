# -------------------------------------------------------------------------------
# Name:        module2
# Purpose:
#
# Author:      Animesh
#
# Created:     03/03/2015
# Copyright:   (c) Animesh 2015
# Licence:     <your licence>
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
        ##        index.seek(file_start)
        ##        print index.read(file_end - file_start)
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
                         for i in temp_ind[term]]) + constants.ENDLINE
        print data
        index.write(data.replace(' ', ''))
        file_end = index.tell()
        ##        index.seek(file_start)
        ##        print index.read(file_end - file_start)
        catalog_data = "{term}|{start}:{end}".format(term=term, \
                                                     start=file_start, end=(file_end - file_start)) + constants.ENDLINE
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
                            temp_ind.clear()

                        if countDoc == 1:
                            countDoc = 0
                            print "Will yield from collection", collection, "@", dict_id_val
                            yield temp_ind
                            temp_ind.clear()


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
                ##                print "For", main_term, "$$$", main_data, "<===>", current_data
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
    ##    print "post concat", data
    aux_index.write(data.replace(' ', ''))
    file_end = aux_index.tell()
    catalog_data = "{term}|{start}:{end}".format(term=term, \
                                                 start=file_start, end=(file_end - file_start - 2)) + \
                   constants.ENDLINE
    aux_catalog.write(catalog_data.replace(' ', ''))
    return aux_index, aux_catalog


if __name__ == '__main__':
    iteration = 0
    try:
        for i in createTempIndexDoc():
            mergeToMain1(i)
            iteration += 1
            if iteration == 2:
                exit()
            ##            writeVirtualInvInd(i)
    except KeyboardInterrupt:
        print "There was an interrupt..."
    finally:
        v.close()
