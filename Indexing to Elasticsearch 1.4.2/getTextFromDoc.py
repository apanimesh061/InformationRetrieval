import sys
import re
import glob
import string
import util
import constants


def advancedWarfare(textChunk):
    temp_text = \
        util.removeStopWords \
            (util.removePunctuation \
                 (" ".join(textChunk)).lower(), constants.STOP_LIST)
    temp_text = util.alterSpaces(temp_text)
    temp_text = " ".join([s for s in temp_text.split(" ") if not util.num_there(s)])
    return temp_text, len(temp_text.split(" ")), util.ngramsLength(temp_text, 2)


def streamAllDocs():
    for doc_collection in constants.CORPUS:
        countDoc = 0
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
                        if not (line.strip() == "<TEXT>" or line.strip() == "</TEXT>"):
                            textChunk.append(line.strip())

                    end_text_match = re.findall(r'</TEXT>', line)
                    if len(end_text_match) > 0:
                        startText = False
                        endText = True

                    end_match = re.findall(r"</DOC>", line)

                    if len(end_match) > 0:
                        endDoc = True
                        startDoc = False
                        _, psw_text_len, psw_text_bilen = advancedWarfare(textChunk)
                        if constants.ADVANCED_PRE_PROCESSING:
                            final_text, _, _ = advancedWarfare(textChunk)
                        else:
                            final_text = util.alterSpaces(util.removePunctuation(" ".join(textChunk)))
                        if constants.STREAM:
                            yield {
                                "_index": constants.INDEX_NAME,
                                "_type": constants.TYPE_NAME,
                                "_id": dict_id_val,
                                "_source": {
                                    "text": " ".join(textChunk),
                                    ##                                    "bi_doc_length" : psw_text_bilen,
                                    "doc_length": psw_text_len,
                                }
                            }
                        else:
                            yield {
                                dict_id_val: {
                                    'text': final_text
                                }
                            }
                        textChunk = []


if __name__ == '__main__':
    count = 0
    final_dict = dict()
    for item in streamAllDocs():
        count += 1
        print item
        if count % 10000 == 0:
            print "Reached document " + str(count)
        final_dict.update(item)

    util.saveJSON(constants.JSON_FILE, final_dict)
