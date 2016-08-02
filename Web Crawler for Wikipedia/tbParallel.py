# -------------------------------------------------------------------------------
# Name:        tbParallel
# Purpose:     non-threaded version of the crawler
#
# Author:      Animesh Pandey
#
# Created:     20/03/2015
# Copyright:   (c) Animesh Pandey 2015
# -------------------------------------------------------------------------------

import urllib2
import urlparse
import yaml
from reppy.cache import RobotsCache
from reppy import exceptions
from time import sleep
from bs4 import BeautifulSoup
from hashlib import md5
from pymongo import MongoClient
from heapq import *
import itertools
import time

from crawlDS import *
from saveToDB import *


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
            if a:
                possible_links.append(a['href'])
    for div in rejection_data:
        links = div.findAll('a', href=True)
        for a in links:
            if a:
                possible_reject_links.append(a['href'])
    for div in citation_data:
        links = div.findAll('a', href=True)
        for a in links:
            if a:
                possible_reject_links.append(a['href'])
    for div in reference_data:
        links = div.findAll('a', href=True)
        for a in links:
            if a:
                possible_reject_links.append(a['href'])
    for div in external_link_data:
        links = div.findAll('a', href=True)
        for a in links:
            if a:
                possible_reject_links.append(a['href'])

    return filter(lambda url: url not in possible_reject_links, possible_links)


def domain_restrict(link, domain):
    link_parts = urlparse.urlparse(link)
    if 'youtube' in link_parts.netloc or 'facebook' in link_parts.netloc \
            or 'foursquare' in link_parts.netloc:
        return False
    curr_domain = \
        '{uri.scheme}://{uri.netloc}/'.format(uri=urlparse.urlparse(link.encode('utf-8')))
    if curr_domain == domain:
        return True
    else:
        return False


def handleOtherPage(soup, domain):
    data = soup.find_all('a', href=True)
    possible_out_links = [u['href'] for u in data if u['href'] if domain_restrict(u['href'], domain)]
    return possible_out_links


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


def isRelativePath(url):
    # link has been updated, so resplitting is required
    link_parts = urlparse.urlparse(url)
    return link_parts.netloc == '' and link_parts.path


def getValidOutLinks(url):
    main_url_encoded = url.decode('utf-8', 'ignore')
    url_hash = md5(url).hexdigest()
    try:
        req = urllib2.Request(url, headers={'User-Agent': AGENT_NAME})
        response = urllib2.urlopen(req)
    except ValueError, e:
        print "An invalid URL was entered:", e
        return False
    except (urllib2.HTTPError, urllib2.URLError) as e:
        print "Some HTTP issue was there:", e
        return False

    try:
        if not robots.allowed(url, AGENT_NAME):
            return False
    except exceptions.ServerError, e:
        return False

    if response.info().type != 'text/html':
        return False

    html = response.read()
    headers = response.info().headers
    soup = BeautifulSoup(html)
    possible_out_links = []
    if 'en.wikipedia.org' in url:
        possible_out_links = handleWikiPage(soup)
    else:
        domain = '{uri.scheme}://{uri.netloc}/'.format(uri=urlparse.urlparse(url.encode('utf-8')))
        possible_out_links = handleOtherPage(soup, domain)

    outlinks = []
    outlinks_hash = []
    no_of_ol = 0
    for link in possible_out_links:
        link_parts = urlparse.urlparse(link)
        if isRelativePath(link):
            link = urlparse.urljoin(url, link_parts.path)
        link = urlparse.urldefrag(link)[0]
        try:
            encoded_link = link.decode('utf-8', 'ignore')
        except UnicodeEncodeError, e:
            continue

        if encoded_link == '':
            continue

        link_hash = md5(encoded_link).hexdigest()  # current page's outlink's hash

        in_link_data[link_hash]['_id'] = link_hash

        url_hash_data[link_hash]['_id'] = link_hash
        url_hash_data[link_hash]['url'] = encoded_link
        url_hash_data[link_hash]['canon_url'] = normalizeURL(encoded_link)

        # inlink table updation
        in_link_table.update(in_link_data[link_hash], url_hash)
        url_hash_table.update(url_hash_data[link_hash])

        outlinks.append(encoded_link)
        outlinks_hash.append(link_hash)
        no_of_ol += 1

    out_link_data[url_hash]['_id'] = url_hash
    out_link_data[url_hash]['outlink_count'] = no_of_ol
    out_link_data[url_hash]['outlinks'] = outlinks_hash
    out_link_data[url_hash]['header'] = headers
    out_link_data[url_hash]['content'] = html.decode('utf-8', 'ignore')
    out_link_table.update(out_link_data[url_hash])

    return outlinks


def bfs():
    visited.add(md5(normalizeURL(SEED)).hexdigest())
    SEED_HASH = md5(SEED.decode('utf-8', 'ignore')).hexdigest()

    # Data dictionary of inlink table
    in_link_data[SEED_HASH]['_id'] = SEED_HASH

    url_hash_data[SEED_HASH]['_id'] = SEED_HASH
    url_hash_data[SEED_HASH]['url'] = SEED.decode('utf-8', 'ignore')
    url_hash_data[SEED_HASH]['canon_url'] = normalizeURL(SEED)

    # Data dictionary sent to respective table
    in_link_table.update(in_link_data[SEED_HASH], SEED_HASH)
    url_hash_table.update(url_hash_data[SEED_HASH])

    # current URL queued for popping
    enqueue(SEED, in_link_table.get_inlink_count(in_link_data[SEED_HASH]))

    while urls:
        new_url = dequeue()
        try:
            delay = robots.delay(new_url, AGENT_NAME)
            if not delay:
                sleep(1.0)
            else:
                sleep(delay)
        except:
            print "Probably a robot parser error."

        outlinks = getValidOutLinks(new_url)
        if outlinks:
            print "Current:", new_url
            for child_link in outlinks:
                if child_link:
                    canon_child_link = normalizeURL(child_link)
                    curr_hash = md5(canon_child_link).hexdigest()
                    if curr_hash not in visited:
                        visited.add(curr_hash)
                        if out_link_table.size() == MAX_URL:
                            print "Ending now"
                            exit()
                        # queues contain the original links
                        enqueue(child_link, \
                                in_link_table.get_inlink_count(in_link_data[md5(child_link).hexdigest()]))
        else:
            print "Skipping:", new_url


if __name__ == '__main__':
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
        for SEED in map(lambda d: d['url'], data['seed_urls'])[:4]:
            print "@ Seed", SEED
            robots = RobotsCache()
            SEED_PATH = urlparse.urlparse(SEED).path

            out_link_table = outLinkDb()
            out_link_data = outLinkData()

            in_link_table = inLinkDb()
            in_link_data = inLinkData()

            url_hash_table = urlHashDb()
            url_hash_data = urlHash()

            bfs()

    except KeyboardInterrupt:
        print "There was a keyboard interrupt!"
        print out_link_table.size()
        print in_link_table.size()


##url_hash_table.clear()
##in_link_table.clear()
##out_link_table.clear()
