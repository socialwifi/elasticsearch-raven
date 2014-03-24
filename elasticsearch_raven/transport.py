import base64
import datetime
import json
import zlib

import elasticsearch
from elasticsearch_raven.postfix import postfix_encoded_data


class Message:
    def __init__(self, body):
        self._body = body

    @classmethod
    def from_message(cls, message):
        return cls(message.headers, message.body)

    @property
    def body(self):
        return self ._body


class SentryMessage(Message):
    def __init__(self, body):
        super().__init__(body)
        self._compressed = True

    @classmethod
    def create_from_udp(cls, data):
        headers, data = data.split(b'\n\n')
        data = base64.b64decode(data)
        return cls(data)

    @classmethod
    def create_from_http(cls, data):
        data = base64.b64decode(data)
        return cls(data)

    @property
    def body(self):
        if self._compressed:
            self._body = json.loads(
                zlib.decompress(self._body).decode('utf-8'))
            self._compressed = False
        return self._body


class ElasticsearchMessage(Message):
    def __init__(self, body):
        super().__init__(body)
        postfix_encoded_data(self._body)


class ElasticsearchTransport:
    def __init__(self, host):
        self._connection = elasticsearch.Elasticsearch(hosts=[host])

    def send(self, message):
        message = ElasticsearchMessage.from_message(message)
        dated_index = message.body['project'].format(datetime.datetime.now())
        self._connection.index(body=message.body, index=dated_index,
                               doc_type='raven-log')
