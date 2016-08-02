# -------------------------------------------------------------------------------
# Name:        crawlDS
# Purpose:     Data Structures required for Crawling
#
# Author:      Animesh Pandey
#
# Created:     12/03/2015
# Copyright:   (c) Animesh Pandey 2015
# -------------------------------------------------------------------------------

from collections import defaultdict


class Queue:
    def __init__(self):
        self.items = []

    def isEmpty(self):
        return self.items == []

    def enqueue(self, item):
        self.items.insert(0, item)

    def dequeue(self):
        return self.items.pop()

    def size(self):
        return len(self.items)

    def get(self):
        return self.items


class Visited:
    def __init__(self):
        self.items = []

    def add(self, url):
        self.items.append(url)

    def size(self):
        return len(self.items)

    def isEmpty(self):
        return self.size() == 0

    def get(self):
        return self.items


class outLinkData:
    def __init__(self):
        self.url_data = \
            defaultdict(
                lambda: {
                    '_id': None,
                    'content': None,
                    'header': None,
                    'outlinks': [],  # This will also include hashes of URLs
                    'outlink_count': 0
                })

    def update(self, url, meta_tag, meta_value):
        self.url_data[url][meta_tag] = meta_value

    def size(self):
        return len(self.url_data)

    def isEmpty(self):
        return self.size() == 0

    def __getitem__(self, i):
        return self.url_data[i]

    def __iter__(self):
        return self.url_data.iteritems()

    def getValues(self):
        return self.url_data.values()


class urlHash:
    def __init__(self):
        self.url_data = \
            defaultdict(
                lambda: {
                    '_id': None,
                    'url': None,
                    'canon_url': None
                })

    def size(self):
        return len(self.url_data)

    def isEmpty(self):
        return self.size() == 0

    def __getitem__(self, i):
        return self.url_data[i]

    def __iter__(self):
        return self.url_data.iteritems()

    def getValues(self):
        return self.url_data.values()


class inLinkData:
    def __init__(self):
        self.url_data = \
            defaultdict(
                lambda:
                {
                    '_id': None,
                    'inlinks': [],
                    'inlink_count': 0
                })

    def update(self, url, meta_tag, meta_value):
        self.url_data[url][meta_tag] = meta_value

    def size(self):
        return len(self.url_data)

    def isEmpty(self):
        return self.size() == 0

    def __getitem__(self, i):
        return self.url_data[i]

    def __iter__(self):
        return self.url_data.iteritems()

    def getValues(self):
        return self.url_data.values()
