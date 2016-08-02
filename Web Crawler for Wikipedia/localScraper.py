# -------------------------------------------------------------------------------
# Name:        localScraper
# Purpose:     Crawl specific domain
#
# Author:      Animesh Pandey
#
# Created:     11/03/2015
# Copyright:   (c) Animesh Pandey 2015
# -------------------------------------------------------------------------------

from gevent import monkey

monkey.patch_all()

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


def validateURL(link, main_url):
    # convert all URLs to lower case
    link = link.lower()

    # remove fragments from the URL
    link = urlparse.urldefrag(link)[0]

    link_parts = urlparse.urlparse(link)

    # change all URLs to https type
    if link_parts.scheme == 'http':
        link = "".join(['https://'] + link.split('//')[1:])

    # This means that we have encountered same base URL
    link_parts = urlparse.urlparse(link)
    if link_parts.path == '/':
        temp = list(link_parts[:])
        temp[2] = ''
        link = urlparse.urlunparse(tuple(temp))

    # check iff the relative URL exists without a base.
    # use urljoin to concatenate the relative path to base path
    if link_parts.netloc == '' and link_parts.path:
        # join the portless base URL to the relative URL
        link = urlparse.urljoin(main_url, link_parts.path)
        link_parts = urlparse.urlparse(link)

        # remove port number from the URL
        try:
            if link_parts.netloc.split(':')[1] == '80':
                temp = list(link_parts[:])
                temp[1] = temp[1].split(':')[0]
                link = urlparse.urlunparse(tuple(temp))
        except IndexError:
            return link

    return link


def crawlPage(main_url):
    print main_url
    if main_url not in visited.get():
        unicode_main_url = main_url.decode('utf-8', 'ignore')
        link_meta_data[unicode_main_url]['url'] = unicode_main_url

        domain = '{uri.scheme}://{uri.netloc}/'.format(uri=urlparse.urlparse(main_url))
        no_of_out_links = 0
        outlinks = []
        print "Crawling URL:", main_url
        ##            print "Size of the queue is", len(urls.get())
        ##            print "Size of the frontier is", len(visited.get())

        # send a request only when allowed in robots.txt
        if robots.allowed(main_url, AGENT_NAME):
            # for politeness get the Crawl-delay from robots.txt
            delay = robots.delay(main_url, AGENT_NAME)
            if not delay:
                sleep(1.0)
            else:
                sleep(delay)

            try:
                req = urllib2.Request(main_url, headers={'User-Agent': AGENT_NAME})
                response = urllib2.urlopen(req)

                if response.info().type == 'text/html':
                    # convert URL to UUID code
                    link_meta_data[unicode_main_url]['_id'] = md5(unicode_main_url).hexdigest()

                    # encode headers to unicode (utf-8)
                    link_meta_data[unicode_main_url]['header'] = \
                        "".join(response.info().headers).decode('utf-8', 'ignore')
                    # get full HTML from the response
                    html = response.read()
                    # encode HTML content to unicode (utf-8)
                    link_meta_data[unicode_main_url]['content'] = \
                        html.decode('utf-8', 'ignore')

                    # convert HTML to Soup object
                    soup = BeautifulSoup(html)

                    # extract all anchors from the Soup
                    possible_out_links = soup.find_all('a', href=True)
                    for link in map(lambda u: u['href'], possible_out_links):
                        canon_link = validateURL(link, main_url)
                        link_parts = urlparse.urlparse(canon_link)
                        # reject the URL that have different domain than that of the base URL
                        curr_domain = '{p.scheme}://{p.netloc}/'.format(p=link_parts)
                        if domain != curr_domain:
                            continue

                        # forcing the crawler to search only in the relative
                        # path of the SEED
                        if SEED_PATH not in link_parts.path:
                            continue

                        # reject empty URLs
                        if link_parts.netloc == '' and link_parts.path == '':
                            continue

                        # if link is valid then add to visited and enqueue in the
                        # queue
                        if urls.size() <= MAX_PAGES:
                            urls.enqueue(link)

                        # canonicalize URLs on the current page
                        outlinks.append(canon_link)
                        visited.add(canon_link)

                        # update the inlink data for the current link
                        encoded_link = canon_link.decode('utf-8', 'ignore')
                        in_link_data[encoded_link]['_id'] = md5(encoded_link).hexdigest()
                        in_link_data[encoded_link]['url'] = encoded_link

                        # update/insert inlink data in DB
                        if urls.size() <= MAX_PAGES:
                            in_link_table.update(in_link_data[encoded_link], table2)
                        else:
                            in_link_table.postMax(in_link_data[encoded_link], table2)
                        no_of_out_links += 1

                    link_meta_data[unicode_main_url]['outlink_count'] = no_of_out_links
                    link_meta_data[unicode_main_url]['outlinks'] = outlinks
                    ##                        print "There were a total of {0} outlinks.".format(no_of_out_links)
                    ##                        print "Size of the inlink data is", table2.count()

                    # Update full content in DB
                    out_link_table.update(link_meta_data[unicode_main_url], table1)

                    # exit if required number of pages have been parsed
                    if urls.isEmpty() or (visited.size() == MAX_PAGES):
                        exit()
                    next_url = urls.dequeue()
                    crawlPage(next_url)
                else:
                    print "Current URL's content type was not HTML"
                    next_url = urls.dequeue()
                    crawlPage(next_url)
            except Exception, e:
                print "Ignoring current URL due to", e
                next_url = urls.dequeue()
                crawlPage(next_url)
        else:
            print "Disallowed URL as per robots.txt"
            next_url = urls.dequeue()
            crawlPage(next_url)
    else:
        print "Skipping URL", main_url
        next_url = urls.dequeue()
        crawlPage(next_url)


if __name__ == '__main__':
    try:
        HEADERS = {'User-Agent': 'TEST_BOT'}
        client = MongoClient('mongodb://localhost:27017/')
        db = client['crawl_data']
        table1 = db['outlink_data']
        table2 = db['inlink_data']

        in_link_table = inLinkDb()
        out_link_table = outLinkDb()

        # get the seed URL from YAML
        data = yaml.safe_load(open('app.yaml'))

        robots = RobotsCache()
        link_meta_data = outLinkData()
        in_link_data = inLinkData()
        visited = Visited()
        urls = Queue()

        SEED = data['seed_urls'][0]['url']
        MAX_PAGES = data['page_limit_per_url']
        AGENT_NAME = data['crawler_agent']

        # add the seed/first URL to Queue
        urls.enqueue(SEED)
        SEED_PATH = urlparse.urlparse(SEED).path
        # update the inlink for the enqueued URL
        in_link_data[SEED]['_id'] = md5(SEED.decode('utf-8', 'ignore')).hexdigest()
        in_link_data[SEED]['url'] = SEED.decode('utf-8', 'ignore')

        in_link_table.update(in_link_data[SEED], table2)

        crawlPage(urls.dequeue())

    except KeyboardInterrupt:
        print "There was a key board interrupt!"
        print table1.count()
        print table2.count()
        table1.drop()
        table2.drop()
