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


def relevance_by_heuristic(url):
    val = url_heuristic(url)
    if val:
        return val
    ##    try:
    ##        delay = robots.delay(url, AGENT_NAME)
    ##        if not delay:
    ##            sleep(0.0)
    ##        else:
    ##            sleep(delay)
    ##    except:
    ##        print "Probably a robot parser error."

    try:
        req = urllib2.Request(url, headers={'User-Agent': AGENT_NAME})
        response = urllib2.urlopen(req)
    except ValueError, e:
        print "An invalid URL was entered:", e
        return False
    except (urllib2.HTTPError, urllib2.URLError) as e:
        print "Some HTTP issue was there:", e
        return False

    if not robots.allowed(url, AGENT_NAME):
        return False
    if response.info().type != 'text/html':
        return False

    content = response.read()
    content = content.decode('utf-8', 'ignore')
    return reduce(lambda x, y: x | y, \
                  [key_word in content.lower() for key_word in KEY_WORDS])


def normalizeURL(url):
    ##    url = url.lower()
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


def handleWikiPage(soup):
    data = soup.find_all('div', attrs={'id': 'bodyContent'})
    rejection_data = soup.find_all('div', attrs={'id': 'catlinks'})
    citation_data = soup.find_all('div', \
                                  attrs={'class': 'reflist columns references-column-width'})
    reference_data = soup.find_all('div', \
                                   attrs={'class': 'refbegin columns references-column-width'})
    external_link_data = soup.find_all('div', attrs={'class': 'noprint navbox'})
    possible_links = []
    possible_reject_links = []

    for div in data:
        links = div.find_all('a', href=True)
        for a in links:
            possible_links.append(a['href'])
    for div in rejection_data:
        links = div.findAll('a', href=True)
        for a in links:
            possible_reject_links.append(a['href'])
    for div in citation_data:
        links = div.findAll('a', href=True)
        for a in links:
            possible_reject_links.append(a['href'])
    for div in reference_data:
        links = div.findAll('a', href=True)
        for a in links:
            possible_reject_links.append(a['href'])
    for div in external_link_data:
        links = div.findAll('a', href=True)
        for a in links:
            possible_reject_links.append(a['href'])

    return filter(lambda url: url not in possible_reject_links, possible_links)


def url_heuristic(url):
    return reduce(lambda x, y: x | y, \
                  [key_word in " ".join(url.lower().split('/')[-1].split('_')) \
                   for key_word in KEY_WORDS])


def getValidOutLinks(url):
    domain = '{uri.scheme}://{uri.netloc}/'.format(uri=urlparse.urlparse(url))
    try:
        req = urllib2.Request(url, headers={'User-Agent': AGENT_NAME})
        response = urllib2.urlopen(req)
    except ValueError, e:
        print "An invalid URL was entered:", e
        return False
    except (urllib2.HTTPError, urllib2.URLError) as e:
        print "Some HTTP issue was there:", e
        return False

    if not robots.allowed(url, AGENT_NAME):
        return False
    if response.info().type != 'text/html':
        return False

    html = response.read()
    headers = response.info().headers
    soup = BeautifulSoup(html)
    possible_out_links = []
    if domain != 'http://en.wikipedia.org':
        possible_out_links = handleWikiPage(soup)
    else:
        data = soup.find_all('a', href=True)
        possible_out_links = map(lambda u: u['href'], data)

    outlinks = []
    for link in possible_out_links:
        link_parts = urlparse.urlparse(link)
        if isRelativePath(link):
            link = urlparse.urljoin(url, link_parts.path)
        link = urlparse.urldefrag(link)[0]
        try:
            encoded_link = link.decode('utf-8', 'ignore')
        except UnicodeEncodeError, e:
            continue

        ##        if encoded_link != '':
        ##            if not relevance_by_heuristic(encoded_link):
        ####                print encoded_link
        ##                continue
        ##        else:
        ##            continue

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
        ##        print len(visited)
        # Politeness policy
        ##        try:
        ##            delay = robots.delay(new_url, AGENT_NAME)
        ##            if not delay:
        ##                sleep(1.0)
        ##            else:
        ##                sleep(delay)
        ##
        ##        except KeyboardInterrupt:
        ##            print "Keyboard interrupt was detected."
        ##
        ##        except:
        ##            print "Probably a robot parser error."

        outlinks = getValidOutLinks(new_url)
        if outlinks:
            for child_link in outlinks:
                canon_child_link = normalizeURL(child_link)
                if canon_child_link not in visited:
                    visited.add(canon_child_link)
                    if table1.count() == MAX_URL:
                        print "Ending now"
                        exit()
                    ##                    print in_link_table.getInlink(in_link_data[child_link], table2), child_link
                    enqueue(child_link, \
                            in_link_table.getInlink(in_link_data[child_link], table2))
        else:
            print "Skipping link", new_url


if __name__ == '__main1__':
    try:
        data = yaml.safe_load(open('app.yaml'))
        AGENT_NAME = data['crawler_agent']
        MAX_URL = data['page_limit']
        KEY_WORDS = data['key_words']

        urls = []
        entry_finder = {}
        REMOVED = '<removed-url>'
        counter = itertools.count()

        visited = set()
        for SEED in map(lambda d: d['url'], data['seed_urls'])[5:7]:
            print "@ Seed", SEED
            robots = RobotsCache()
            SEED_PATH = urlparse.urlparse(SEED).path

            client = MongoClient('mongodb://localhost:27017/')
            db = client['wiki_data']

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


if __name__ == '__main__':
    from pymongo import MongoClient

    client = MongoClient('mongodb://localhost:27017/')
    db = client['wiki_data']
    table1 = db['outlink_data']
    table2 = db['inlink_data']
    print table1.count()
    print table2.count()
