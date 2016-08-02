from collections import defaultdict
from hashlib import md5
from pymongo import MongoClient
from bs4 import BeautifulSoup
from bs4 import UnicodeDammit
import lxml
import lxml.etree
from lxml.html.clean import Cleaner
from lxml import html
from textwrap import wrap
import warnings

warnings.filterwarnings("ignore")
import cStringIO


def clean_text(data):
    cleaner = Cleaner()
    cleaner.javascript = True
    cleaner.style = True
    cleaner.scripts = True
    cleaner.comments = True
    cleaner.meta = True
    cleaner.annoying_tags = True

    doc = UnicodeDammit(data, is_html=True)
    parser = html.HTMLParser(encoding=doc.original_encoding)
    root = html.document_fromstring(data, parser=parser)
    stuff = lxml.html.tostring(cleaner.clean_html(root))

    soup = BeautifulSoup(stuff.decode('utf-8', 'ignore'))
    all_text = ' '.join(filter(lambda val: val, \
                               map(lambda x: x.strip(), soup.findAll(text=True))))

    return all_text.encode('ascii', 'ignore')


'''
{
    'docno' : '<DOC>',
    'title' : '<TITLE>',
    'text' : '<TEXT>',
    'in_links' : '<INLINK>',
    'out_links' : '<OUTLINK>',
    'header' : '<HEADER>',
    'doclength' : '<DOCLEN>'
}
'''

if __name__ == '__main1__':
    try:
        client = MongoClient('mongodb://localhost:27017/')
        db_to = client['BU_crawl_data_2']
        db_from = client['bu_wiki']

        table1_to = db_to['outlink_data']
        table2_to = db_to['inlink_data']

        table1_from = db_from['outlink_data']
        table2_from = db_from['inlink_data']

        for data in table1_to.find():
            inlink = table2_to.find_one({"_id": data['_id']})['inlink_count']
            table1_from.find_and_modify({'_id': data['_id']}, \
                                        {'$set': {'outlinks': [link for link in data['outlinks'] if link]}})
        ##
        ##        for data in table1_from.find():
        ##            table1_from.find_and_modify({'_id' : data['_id']}, \
        ##                    {'$set' : {'outlink_count' : len(data['outlinks'])}})

        link_graph = open('link_graph.dat', 'w')
        with open('corpus.dat', 'w') as f:
            for datum in table1_from.find():
                data = table1_from.find_one({'_id': datum['_id']})

                f.write("<DOC>\n")

                f.write("<ID>{0}</ID>\n".format(data['_id']))

                f.write("<DOCNO>{0}</DOCNO>\n".format(data['url']))

                f.write("".join(data['header']).encode('utf-8'))

                soup = BeautifulSoup(data['content'])
                title = soup.find('title').getText()
                f.write("<TITLE>{0}</TITLE>\n".format(title.encode('ascii', 'ignore')))

                f.write("<INLINK>{0}</INLINK>\n".format(data['inlink_count']))

                f.write("<OUTLINK>{0}</OUTLINK>\n".format(data['outlink_data']))

                f.write("<TEXT>\n")
                f.write('\n'.join(['\n'.join(wrap(block, width=70)) \
                                   for block in clean_text(data).splitlines()]))
                f.write("\n")
                f.write("</TEXT>\n")

                f.write("<CONTENT>\n")
                f.write(data['content'].encode('utf-8'))
                f.write("\n")
                f.write("</CONTENT>\n")

                f.write("</DOC>\n")
                link_graph.write(data['url'] + "\t" + "\t".join(data['outlinks']) + "\n")
        link_graph.close()

    except KeyboardInterrupt:
        print "There was a keyboard interrupt!"
