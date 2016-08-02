# -------------------------------------------------------------------------------
# Name:        page_rank
# Purpose:     Page Rank
#
# Author:      Animesh Pandey
#
# Created:     04/04/2015
# Copyright:   (c) Animesh Pandey 2015
# -------------------------------------------------------------------------------

import math, sys, pprint
from collections import defaultdict
import numpy as np

d = .85  # This is the Damping/Teleportation Factor


def calculatePagerank(allpages, inlinks, outlinks, count):
    nooutlinks = []  # all the sink nodes will be included here
    for page in allpages:
        if page not in outlinks:
            nooutlinks.append(page)

    pagerank = dict()
    for page in allpages:  # Initial values of page rank for each page (Uniform)
        pagerank[page] = 1.0 / count

    prev_perplexity = 0.0
    current_perplexity = perplex(pagerank)
    while not hasconverged(prev_perplexity, current_perplexity):
        newpagerank = dict()
        sink_node_pagerank = 0
        for page in nooutlinks:
            sink_node_pagerank += pagerank[page]  # calculate sink node pagerank
        for page in pagerank.keys():
            newpagerank[page] = (1 - d) / count
            newpagerank[page] += d * sink_node_pagerank / count
            for inlink in inlinks[page]:
                try:
                    newpagerank[page] += d * pagerank[inlink] / outlinks[inlink]
                except:
                    # reject this page as this page was not visited during crawl
                    # but is in the inlinks. So, outlinks[inlink] will give an
                    # IndexError
                    continue

        # the current pagerank distribution will now become the previous distribution
        # as it will be compared for convergence in next iteration
        pagerank = newpagerank
        prev_perplexity = current_perplexity
        current_perplexity = perplex(pagerank)
    return pagerank


# convergence is being checked to 4 places of decimal
def hasconverged(prevPerplexity, currentPerplexity):
    r1 = round(prevPerplexity, 4)
    r2 = round(currentPerplexity, 4)
    return r1 == r2


# This just calculates the how the probability is spread across
# whole sample space
def perplex(pagerank):
    return pow(2, entropy(pagerank))


# This will calculate the Entropy of the current PR distribution
def entropy(pagerank):
    s = 0
    for doc in pagerank:
        p = pagerank[doc]
        s += p * math.log(p, 2)
    return -1 * s


def read_inlink_file():
    count = 0
    inlinks = dict()
    outlinks = dict()
    allpages = []
    f = open('wt2g_inlinks.txt', 'r')
    content = f.readlines()

    for line in content:
        line = line.strip()
        links = line.split(" ")
        node = links[0]
        links.remove(node)
        inlinks[node] = links
        allpages.append(node)

        for link in links:
            if link in outlinks:  # this does not store links
                # it just stores the number of outlinks
                outlinks[link] += 1
            else:
                outlinks[link] = 1
        count += 1

    return allpages, inlinks, outlinks, count


def pagerankedDocs(pageranks):
    temp = open('ir_bukkar.txt', 'w')
    for key, value in sorted(pageranks.iteritems(), key=lambda (k, v): (v, k), reverse=True):
        temp.write(key + '_____' + '%.40f' % (value) + '\n')


##allpages, inlinks, outlinks, count = read_inlink_file()
##print "There are a total of", count, "documents"
##pagerank = calculatePagerank(allpages, inlinks, outlinks, count)
##pagerankedDocs(pagerank)

import re

with open('QREL.txt', 'rb') as ff:
    for count, line in enumerate(ff):
        print re.compile(r"\s+").split(line.strip()), count + 1
