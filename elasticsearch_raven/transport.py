import base64
import datetime
import json
import re
import zlib

import elasticsearch
from elasticsearch_raven.postfix import postfix_encoded_data


class Message:
    def __init__(self, headers, body):
        self._body = body
        self._headers = headers

    @classmethod
    def from_message(cls, message):
        return cls(message.headers, message.body)

    @property
    def body(self):
        return self._body

    @property
    def headers(self):
        return self._headers


class SentryMessage(Message):
    def __init__(self, headers, body):
        super().__init__(headers, body)
        self._compressed = True

    @classmethod
    def create_from_udp(cls, data):
        byte_headers, data = data.split(b'\n\n')
        headers = cls.parse_headers(str(byte_headers.decode('utf-8')))
        data = base64.b64decode(data)
        return cls(headers, data)

    @staticmethod
    def parse_headers(unparsed_headers):
        m = re.search(r'sentry_key=(?P<sentry_key>[^=]+), sentry_secret='
                      r'(?P<sentry_secret>[^=]+)$', unparsed_headers)
        return m.groupdict()


    @classmethod
    def create_from_http(cls, unparsed_headers, data):
        headers = cls.parse_headers(unparsed_headers)
        data = base64.b64decode(data)
        return cls(headers, data)

    @property
    def body(self):
        if self._compressed:
            self._body = json.loads(
                zlib.decompress(self._body).decode('utf-8'))
            self._compressed = False
        return self._body


class ElasticsearchMessage(Message):
    def __init__(self, headers, body):
        super().__init__(headers, body)
        postfix_encoded_data(self._body)


class ElasticsearchTransport:
    def __init__(self, host, use_ssl=False):
        self._host = host
        self._use_ssl = use_ssl

    def send(self, message):
        message = ElasticsearchMessage.from_message(message)
        dated_index = message.body['project'].format(datetime.datetime.now())
        http_auth = '{}:{}'.format(message.headers['sentry_key'],
                                   message.headers['sentry_secret'])
        connection = elasticsearch.Elasticsearch(hosts=[self._host],
                                                 http_auth=http_auth,
                                                 use_ssl=self._use_ssl)
        connection.index(body=message.body, index=dated_index,
                         doc_type='raven-log')
