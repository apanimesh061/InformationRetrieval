# -------------------------------------------------------------------------------
# Name:        tokenizer
# Purpose:     here the tet yielded by doc is tokeized
#
# Author:      Animesh Pandey
#
# Created:     18/02/2015
# Copyright:   (c) Animesh Pandey 2015
# -------------------------------------------------------------------------------

import re, hashlib


def tokenizeByRegex(line, regex):
    line = unicode(line.lower(), "utf-8")
    line.decode('utf-8', 'ignore')
    return re.findall(regex, line)


def createIDs(term):
    temp = hashlib.md5(term)
    return temp.hexdigest()
