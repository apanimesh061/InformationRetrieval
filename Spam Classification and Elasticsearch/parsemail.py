import sys, os, re, StringIO
import email, mimetypes
import constants
from bs4 import BeautifulSoup
from bs4 import UnicodeDammit
import lxml
import lxml.etree
from lxml.html.clean import Cleaner
from lxml import html
from elasticsearch import Elasticsearch
import constants, email_indexer, codecs

atom_rfc2822 = r"[a-zA-Z0-9_!#\$\%&'*+/=?\^`{}~|\-]+"
atom_posfix_restricted = r"[a-zA-Z0-9_#\$&'*+/=?\^`{}~|\-]+"
atom = atom_rfc2822
dot_atom = atom + r"(?:\." + atom + ")*"
quoted = r'"(?:\\[^\r\n]|[^\\"])*"'
local = "(?:" + dot_atom + "|" + quoted + ")"
domain_lit = r"\[(?:\\\S|[\x21-\x5a\x5e-\x7e])*\]"
domain = "(?:" + dot_atom + "|" + domain_lit + ")"
addr_spec = local + "\@" + domain

email_address_re = re.compile('^' + addr_spec + '$')


def clean_text(data):
    cleaner = Cleaner()
    cleaner.javascript = True
    cleaner.style = True
    cleaner.scripts = True
    cleaner.comments = True
    cleaner.meta = True
    cleaner.annoying_tags = True

    stuff = lxml.html.tostring(cleaner.clean_html(data))

    soup = BeautifulSoup(stuff.decode('utf-8', 'ignore'))
    all_text = ' '.join(filter(lambda val: val, \
                               map(lambda x: x.strip(), soup.findAll(text=True))))

    return all_text


class Attachment:
    def __init__(
            self,
            part,
            filename=None,
            type=None,
            payload=None,
            charset=None,
            content_id=None,
            description=None,
            disposition=None,
            sanitized_filename=None,
            is_body=None
    ):
        self.part = part
        self.filename = filename
        self.type = type
        self.payload = payload
        self.charset = charset
        self.description = description
        self.disposition = disposition
        self.sanitized_filename = sanitized_filename
        self.is_body = is_body
        self.content_id = content_id
        if self.content_id:
            if self.content_id.startswith('<') and self.content_id.endswith('>'):
                self.content_id = self.content_id[1:-1]


def getmailheader(header_text, default="ascii"):
    try:
        headers = email.Header.decode_header(header_text)
    except email.Errors.HeaderParseError:
        return header_text.encode('ascii', 'replace').decode('ascii')
    else:
        for i, (text, charset) in enumerate(headers):
            try:
                headers[i] = unicode(text, charset or default, errors='replace')
            except LookupError:
                headers[i] = unicode(text, default, errors='replace')
        return u"".join(headers)


def getmailaddresses(msg, name):
    addrs = email.utils.getaddresses(msg.get_all(name, []))
    for i, (name, addr) in enumerate(addrs):
        if not name and addr:
            name = addr
        try:
            addr = addr.encode('ascii')
        except UnicodeError:
            addr = ''
        else:
            if not email_address_re.match(addr):
                addr = ''
        addrs[i] = (getmailheader(name), addr)
    return addrs


def get_filename(part):
    filename = part.get_param('filename', None, 'content-disposition')
    if not filename:
        filename = part.get_param('name', None)
    if filename:
        filename = email.Utils.collapse_rfc2231_value(filename).strip()
    if filename and isinstance(filename, str):
        filename = getmailheader(filename)
    return filename


def _search_message_bodies(bodies, part):
    type = part.get_content_type()
    if type.startswith('multipart/'):
        if type == 'multipart/related':
            start = part.get_param('start', None)
            related_type = part.get_param('type', None)
            for i, subpart in enumerate(part.get_payload()):
                if (not start and i == 0) or \
                        (start and start == subpart.get('Content-Id')):
                    _search_message_bodies(bodies, subpart)
                    return
        elif type == 'multipart/alternative':
            for subpart in part.get_payload():
                _search_message_bodies(bodies, subpart)
        elif type in ('multipart/report', 'multipart/signed'):
            try:
                subpart = part.get_payload()[0]
            except IndexError:
                return
            else:
                _search_message_bodies(bodies, subpart)
                return
        elif type == 'multipart/signed':
            return
        else:
            for subpart in part.get_payload():
                tmp_bodies = dict()
                _search_message_bodies(tmp_bodies, subpart)
                for k, v in tmp_bodies.iteritems():
                    if not subpart.get_param \
                                ('attachment', None, 'content-disposition') == '':
                        bodies.setdefault(k, v)
            return
    else:
        bodies[part.get_content_type().lower()] = part
        return
    return


def search_message_bodies(mail):
    bodies = dict()
    _search_message_bodies(bodies, mail)
    return bodies


def get_mail_contents(msg):
    attachments = []

    bodies = search_message_bodies(msg)
    parts = dict((v, k) for k, v in bodies.iteritems())
    stack = [msg, ]
    while stack:
        part = stack.pop(0)
        type = part.get_content_type()
        if type.startswith('message/'):
            from email.Generator import Generator
            fp = StringIO.StringIO()
            g = Generator(fp, mangle_from_=False)
            g.flatten(part, unixfrom=False)
            payload = fp.getvalue()
            filename = 'mail.eml'
            attachments.append(
                Attachment(
                    part,
                    filename=filename,
                    type=type,
                    payload=payload,
                    charset=part.get_param('charset'),
                    description=part.get('Content-Description')
                )
            )
        elif part.is_multipart():
            stack[:0] = part.get_payload()
        else:
            payload = part.get_payload(decode=True)
            charset = part.get_param('charset')
            filename = get_filename(part)

            disposition = None
            if part.get_param('inline', None, 'content-disposition') == '':
                disposition = 'inline'
            elif part.get_param('attachment', None, 'content-disposition') == '':
                disposition = 'attachment'

            attachments.append(
                Attachment(
                    part,
                    filename=filename,
                    type=type,
                    payload=payload,
                    charset=charset,
                    content_id=part.get('Content-Id'),
                    description=part.get('Content-Description'),
                    disposition=disposition,
                    is_body=parts.get(part)
                )
            )
    return attachments


def decode_text(payload, charset, default_charset):
    if charset:
        try:
            return payload.decode(charset), charset
        except UnicodeError:
            pass
        except LookupError:
            codecs.register(lambda name: codecs.lookup('utf-8') \
                if name == charset else None)
            try:
                return payload.decode(charset), charset
            except LookupError:
                pass
    if default_charset and default_charset != 'auto':
        try:
            return payload.decode(default_charset), default_charset
        except UnicodeError:
            pass
    for chset in ['ascii', 'utf-8', 'utf-16', 'windows-1252', 'cp850']:
        try:
            return payload.decode(chset), chset
        except UnicodeError:
            pass
    return payload, None


def stream_emails():
    try:
        indexed_docs = 0
        failed_log = open('error.log', 'wb')
        for email_file in constants.EMAIL_DOC_LIST:
            try:
                ext_reject = False
                ff = open(email_file, 'rb')
                raw_email = ff.read()
                msg = email.message_from_string(raw_email)
                attachments = get_mail_contents(msg)

                subject = getmailheader(msg.get('Subject', ''))
                from_ = getmailaddresses(msg, 'from')
                from_ = ('', '') if not from_ else from_[0]
                tos = getmailaddresses(msg, 'to')

                full_text = ''
                for attach in attachments:
                    if attach.is_body == 'text/plain':
                        payload, used_charset = decode_text(attach.payload, attach.charset, 'auto')
                        plain_text = payload
                        full_text += plain_text
                    if attach.is_body == 'text/html':
                        payload, used_charset = decode_text(attach.payload, attach.charset, 'auto')
                        try:
                            html_doc = html.fromstring(payload)
                        except:
                            ext_reject = True
                            break
                        html_text = clean_text(html_doc)
                        full_text += html_text
                ff.close()
                content_length = 0 \
                    if attach.payload == None else len(attach.payload)
                doc_id = email_file.split('\\')[1]
                if ext_reject:
                    print "Failed to index document {0}\\{1}\\{2}". \
                        format(
                        constants.INDEX_NAME,
                        constants.TYPE_NAME,
                        doc_id
                    )
                    continue
                full_text = full_text.encode('utf-8').strip()
                body = {
                    "subject": subject,
                    "exact_subject": subject,
                    "text": full_text,
                    "exact_text": full_text,
                    "raw_email": raw_email,
                    "content_length": content_length
                }

                constants.ES_CLIENT.index(
                    index=constants.INDEX_NAME,
                    doc_type=constants.TYPE_NAME,
                    body=body,
                    id=doc_id
                )
                indexed_docs += 1
                print "Succesfully indexed document {0}\\{1}\\{2}". \
                    format(
                    constants.INDEX_NAME,
                    constants.TYPE_NAME,
                    doc_id
                )
            except Exception as e:
                print "Failed to index document {0}\\{1}\\{2}". \
                    format(
                    constants.INDEX_NAME,
                    constants.TYPE_NAME,
                    doc_id
                )
                failed_log.write(doc_id + '\n')
                continue

    except KeyboardInterrupt:
        exit()
    finally:
        print
        print "Successfully indexed {0} documents".format(indexed_docs)
        failed_log.close()


if __name__ == '__main__':
    email_indexer.init_index()
    stream_emails()
