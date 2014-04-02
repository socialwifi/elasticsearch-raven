import base64
import collections
import datetime
import json
import logging
import re
import zlib

import elasticsearch

from elasticsearch_raven.postfix import postfix_encoded_data

elasticsearch_logger = logging.getLogger('elasticsearch')
elasticsearch_logger.setLevel(logging.ERROR)


BaseMessage = collections.namedtuple('BaseMessage', ['headers', 'body'])


class SentryMessage(BaseMessage):
    @classmethod
    def create_from_udp(cls, data):
        byte_headers, data = data.split(b'\n\n')
        headers = cls.parse_headers(str(byte_headers.decode('utf-8')))
        data = base64.b64decode(data)
        return cls(headers, data)

    @classmethod
    def create_from_http(cls, raw_headers, data):
        headers = cls.parse_headers(raw_headers)
        data = base64.b64decode(data)
        return cls(headers, data)

    @staticmethod
    def parse_headers(raw_headers):
        m = re.search(r'sentry_key=(?P<sentry_key>[^=]+), sentry_secret='
                      r'(?P<sentry_secret>[^=]+)$', raw_headers)
        return m.groupdict()

    def decode_body(self):
        return json.loads(zlib.decompress(self.body).decode('utf-8'))


class ElasticsearchTransport:
    def __init__(self, host, use_ssl=False):
        self._host = host
        self._use_ssl = use_ssl

    def send(self, message):
        postfix_encoded_data(message.body)
        dated_index = message.body['project'].format(datetime.datetime.now())
        http_auth = '{}:{}'.format(message.headers['sentry_key'],
                                   message.headers['sentry_secret'])
        connection = elasticsearch.Elasticsearch(hosts=[self._host],
                                                 http_auth=http_auth,
                                                 use_ssl=self._use_ssl)
        connection.index(body=message.body, index=dated_index,
                         doc_type='raven-log')
