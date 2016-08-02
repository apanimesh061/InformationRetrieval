# -------------------------------------------------------------------------------
# Name:        getMetaData
# Purpose:     calculate constants for an INDEX
#
# Author:      Animesh Pandey
#
# Created:     23/02/2015
# Copyright:   (c) Animesh Pandey 2015
# -------------------------------------------------------------------------------

from __future__ import division

total_tokens = 0

with open("INDEX1.dat", "r") as index:
    for data in index:
        total_tokens += int(data.split("|")[0].split(":")[1])

print total_tokens
print total_tokens / 84678
