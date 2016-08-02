# -------------------------------------------------------------------------------
# Name:        util
# Purpose:     general purpose functions
#
# Author:      Animesh Pandey
#
# Created:     19/01/2015
# Copyright:   (c) Animesh Pandey 2015
# -------------------------------------------------------------------------------

import json, string, re


def num_there(s):
    return any(i.isdigit() for i in s)


def saveJSON(filename, data):
    with open(filename, 'wb') as outfile:
        json.dump(data, outfile)
    print "Data saved in " + filename


def loadJSON(filename):
    try:
        with open(filename) as infile:
            data = json.load(infile)
        return data
    except IOError:
        print filename + " could not be located..."


def sliceList(n, iterable):
    from itertools import islice
    return list(islice(iterable, n))


def getStopList(filename):
    try:
        stop = []
        with open(filename) as f:
            for word in f:
                stop.append(word)
        return [w.strip() for w in stop]
    except IOError:
        print filename + " could not be located..."


def getDocList(filename):
    try:
        stop = []
        with open(filename) as f:
            f.next()
            for word in f:
                stop.append(word)
        return [[w.split(" ")[1].strip()] for w in stop]
    except IOError:
        print filename + " could not be located..."


def removePunctuation(text):
    replace_punctuation = string.maketrans(string.punctuation, ' ' * len(string.punctuation))
    return text.translate(replace_punctuation)


def removeStopWords(text, stoplist):
    return " ".join([i for i in text.split() if i not in stoplist])


def sortDictByValue(dictionary, limit):
    temp = dict(sorted(dictionary.items(), key=lambda x: x[1], reverse=True))
    print temp
    return temp.items()[:limit]


def alterSpaces(text):
    return re.sub('\s+', ' ', text).strip()


def ngramsLength(input, n):
    input = input.split(' ')
    output = []
    for i in range(len(input) - n + 1):
        output.append(input[i: i + n])
    return len(output)
