# -------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      Animesh
#
# Created:     13/03/2015
# Copyright:   (c) Animesh 2015
# Licence:     <your licence>
# -------------------------------------------------------------------------------

import urllib2
import urlparse
import yaml
from reppy.cache import RobotsCache
from time import sleep
from bs4 import BeautifulSoup
from crawlDS import *
from saveToDB import *
from hashlib import md5
from pymongo import MongoClient
from heapq import *
import itertools


def enqueue(task, priority=0):
    priority = priority * -1
    if task in entry_finder:
        remove_task(task)
    count = next(counter)
    entry = [priority, count, task]
    entry_finder[task] = entry
    heappush(urls, entry)


def remove_task(task):
    entry = entry_finder.pop(task)
    entry[-1] = REMOVED


def dequeue():
    while urls:
        priority, count, task = heappop(urls)
        if task is not REMOVED:
            del entry_finder[task]
            return task
    raise KeyError('pop from an empty priority queue')


def normalizeURL(url):
    url = url.lower()
    url = urlparse.urldefrag(url)[0]

    # split the URL
    link_parts = urlparse.urlparse(url)

    # link has been updated, so resplitting is required
    link_parts = urlparse.urlparse(url)
    if link_parts.path == '/':
        temp = list(link_parts[:])
        temp[2] = ''
        url = urlparse.urlunparse(tuple(temp))

    # link has been updated, so resplitting is required
    link_parts = urlparse.urlparse(url)
    try:
        if link_parts.netloc.split(':')[1] == '80' or \
                        link_parts.netloc.split(':')[1] == '443':
            temp = list(link_parts[:])
            temp[1] = temp[1].split(':')[0]
            url = urlparse.urlunparse(tuple(temp))
    except IndexError:
        pass

    url = url.decode('utf-8', 'ignore')

    return url


def isRelativePath(url):
    # link has been updated, so resplitting is required
    link_parts = urlparse.urlparse(url)
    return link_parts.netloc == '' and link_parts.path


def getValidOutLinks(url):
    domain = '{uri.scheme}://{uri.netloc}/'.format(uri=urlparse.urlparse(url))

    try:
        req = urllib2.Request(url, headers={'User-Agent': AGENT_NAME})
        response = urllib2.urlopen(req)
    except ValueError, e:
        print "An invalid URL was entered:", e
        return False
    except (urllib2.HTTPError, urllib2.URLError) as e:
        print "Some HTTPS issue was there:", e
        ##        new_link_parts = urlparse.urlparse(url)
        ##        if new_link_parts.scheme == 'https':
        ##            temp_url = "".join(['http://'] + url.split('//')[1:])
        ##        print "Changing to HTTP", temp_url
        try:
            req = urllib2.Request(url, headers={'User-Agent': AGENT_NAME})
            response = urllib2.urlopen(req)
            print "HTTP request was successful."
        except urllib2.HTTPError, e:
            print "Some HTTP error has occurred:", e
            return False

    if not robots.allowed(url, AGENT_NAME):
        return False
    if response.info().type != 'text/html':
        return False

    html = response.read()
    headers = response.info().headers
    soup = BeautifulSoup(html)
    possible_out_links = soup.find_all('a', href=True)
    outlinks = []
    for link in map(lambda u: u['href'], possible_out_links):
        link_parts = urlparse.urlparse(link)
        ##        if link_parts.scheme == 'http':
        ##            link = "".join(['https://'] + link.split('//')[1:])
        if isRelativePath(link):
            link = urlparse.urljoin(url, link_parts.path)
        curr_domain = '{uri.scheme}://{uri.netloc}/'.format(uri=urlparse.urlparse(link))
        if curr_domain != domain:
            continue
        ##        if SEED_PATH not in link_parts.path:
        ##            continue
        if not filter(lambda z: z != '', \
                      set(SEED_PATH.strip().split('/')).intersection \
                                  (set(link_parts.path.split('/')))):
            continue
        encoded_link = link.decode('utf-8', 'ignore')
        in_link_data[encoded_link]['_id'] = md5(encoded_link).hexdigest()
        in_link_data[encoded_link]['url'] = encoded_link
        in_link_table.update(in_link_data[encoded_link], table2)
        outlinks.append(link)

    url = url.decode('utf-8', 'ignore')
    link_meta_data[url]['url'] = url
    link_meta_data[url]['_id'] = md5(url).hexdigest()
    link_meta_data[url]['outlink_count'] = len(outlinks)
    link_meta_data[url]['outlinks'] = outlinks
    link_meta_data[url]['header'] = headers
    link_meta_data[url]['content'] = html.decode('utf-8', 'ignore')
    out_link_table.update(link_meta_data[url], table1)

    return outlinks


def bfs(SEED):
    visited.add(normalizeURL(SEED))
    in_link_data[SEED]['_id'] = md5(SEED.decode('utf-8', 'ignore')).hexdigest()
    in_link_data[SEED]['url'] = SEED.decode('utf-8', 'ignore')
    in_link_table.update(in_link_data[SEED], table2)
    enqueue(SEED, in_link_table.getInlink(in_link_data[SEED], table2))

    while len(urls):
        new_url = dequeue()
        print "Current:", new_url
        # Politeness policy
        try:
            delay = robots.delay(new_url, AGENT_NAME)
            if not delay:
                sleep(1.0)
            else:
                sleep(delay)

        except KeyboardInterrupt:
            print "Keyboard interrupt was detected."

        except:
            print "Probably a robot parser error."

        outlinks = getValidOutLinks(new_url)
        if outlinks:
            for child_link in outlinks:
                canon_child_link = normalizeURL(child_link)
                if canon_child_link not in visited:
                    visited.add(canon_child_link)
                    if len(visited) == MAX_URL:
                        print "Ending now"
                        exit()
                    enqueue(child_link, \
                            in_link_table.getInlink(in_link_data[child_link], table2))
        else:
            print "Skipping link", new_url


if __name__ == '__main__':
    try:
        data = yaml.safe_load(open('app.yaml'))
        AGENT_NAME = data['crawler_agent']
        MAX_URL = data['page_limit']

        urls = []
        entry_finder = {}
        REMOVED = '<removed-url>'
        counter = itertools.count()

        visited = set()

        for SEED in map(lambda d: d['url'], data['seed_urls'])[3:4]:
            print "@ Seed", SEED
            robots = RobotsCache()
            SEED_PATH = urlparse.urlparse(SEED).path

            client = MongoClient('mongodb://localhost:27017/')
            db = client['crawl_data']

            table1 = db['outlink_data']
            out_link_table = outLinkDb()
            link_meta_data = outLinkData()

            table2 = db['inlink_data']
            in_link_table = inLinkDb()
            in_link_data = inLinkData()

            bfs(SEED)

    except KeyboardInterrupt:
        print "There was a keyboard interrupt!"
##        print table1.count()
##        print table2.count()
##        table1.drop()
##        table2.drop()
