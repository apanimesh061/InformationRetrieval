# -------------------------------------------------------------------------------
# Name:        termStats
# Purpose:     calculate CF anf DF
#
# Author:      Animesh Pandey
#
# Created:     23/02/2015
# Copyright:   (c) Animesh Pandey 2015
# -------------------------------------------------------------------------------

import constants, os, json, shutil


def getInvListFromOffsets(offsets):
    with open(os.path.join(constants.TEMP_DIR, 'index_set_1.dat'), "r") as f:
        f.seek(offsets[0])
        data = f.read(offsets[1])
    return data


def getOffsets(data):
    term = data.split("|")[0]
    offsets = (int(data.split("|")[1].split(":")[0]), \
               int(data.split("|")[1].split(":")[1]))
    return term, offsets


def createFinalIndex():
    if not os.path.exists(constants.FINAL_DIR):
        os.makedirs(constants.FINAL_DIR)
    final_index = open(os.path.join(constants.FINAL_DIR, 'INDEX.dat'), "w")
    final_catalog = open(os.path.join(constants.FINAL_DIR, 'CATALOG.dat'), "w")

    final_index.truncate()
    final_catalog.truncate()

    with open(os.path.join(constants.TEMP_DIR, 'catalog_set_1.dat'), "r") as aux:
        for data in aux:
            term, offsets = getOffsets(data)
            inv_list = getInvListFromOffsets(offsets)
            doc_list = json.loads(inv_list.split("|")[1])
            df = len(doc_list)
            cf = sum([d[1] for d in doc_list])

            final_index.seek(0, 2)
            file_start = final_index.tell()
            ind_data = "{term}:{cf}:{df}|{metadata}". \
                           format(term=term, cf=cf, df=df, \
                                  metadata=json.dumps(doc_list)) + constants.ENDLINE
            final_index.write(ind_data.replace(' ', ''))
            file_end = final_index.tell()
            catalog_data = "{term}|{start}:{end}".format(term=term, \
                                                         start=file_start, end=(file_end - file_start - 2)) + \
                           constants.ENDLINE
            final_catalog.write(catalog_data.replace(' ', ''))

    final_catalog.close()
    final_index.close()

    shutil.copy(constants.TEMP_DIR + 'vocab.dat', constants.FINAL_DIR + "VOCAB.dat")
    shutil.copy(constants.TEMP_DIR + 'meta.dat', constants.FINAL_DIR + "META.dat")
    shutil.copy(constants.TEMP_DIR + 'doc_len_map.dat', constants.FINAL_DIR + "DOC_LEN_MAP.dat")
