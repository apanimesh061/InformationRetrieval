# -------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      Animesh
#
# Created:     20/04/2015
# Copyright:   (c) Animesh 2015
# Licence:     <your licence>
# -------------------------------------------------------------------------------

import email
from email.parser import Parser
import mimetypes
import glob
from bs4 import BeautifulSoup
from bs4 import UnicodeDammit
import lxml
import lxml.etree
from lxml.html.clean import Cleaner
from lxml import html

EMAIL_DOC_LIST = glob.glob('data/in*')


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


def get_data(message):
    for part in msg.walk():
        message_type = part.get_content_maintype()
        if message_type == 'multipart':
            filename = part.get_filename()
            if not filename:
                ext = mimetypes.guess_extension(part.get_content_type())
            if not ext:
                ext = '.bin'
        else:
            print part.get_content_type()
        print part.get_payload()
        print clean_text(part.get_payload())


if __name__ == '__main__':

    try:
        for email_file in EMAIL_DOC_LIST[:1]:
            ff = open(email_file, 'rb')
            msg = email.message_from_file(ff)
            headers = Parser().parse(ff)
            get_data(msg)
            ff.close()
    except KeyboardInterrupt:
        exit()
