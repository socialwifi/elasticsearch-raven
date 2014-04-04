import base64
import collections
import datetime
import json
import logging
import re
import time
import zlib

import elasticsearch

from elasticsearch_raven import exceptions
from elasticsearch_raven.postfix import postfix_encoded_data


class SentryMessage(collections.namedtuple('SentryMessage',
                                           ['headers', 'body'])):
    @classmethod
    def create_from_udp(cls, data):
        try:
            byte_headers, data = data.split(b'\n\n')
        except ValueError:
            raise exceptions.DamagedSentryMessageError
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
        match = re.search(r'sentry_key=(?P<sentry_key>[^, =]+), sentry_secret='
                          r'(?P<sentry_secret>[^, =]+)$', raw_headers)
        if match:
            return match.groupdict()
        else:
            raise exceptions.BadSentryMessageHeaderError

    def decode_body(self):
        try:
            return json.loads(zlib.decompress(self.body).decode('utf-8'))
        except (zlib.error, ValueError):
            raise exceptions.DamagedSentryMessageBodyError


class ElasticsearchTransport:
    def __init__(self, host, use_ssl=False):
        self._host = host
        self._use_ssl = use_ssl

    def send(self, message):
        message_body = message.decode_body()
        postfix_encoded_data(message_body)
        dated_index = message_body['project'].format(datetime.datetime.now())
        http_auth = '{}:{}'.format(message.headers['sentry_key'],
                                   message.headers['sentry_secret'])
        connection = elasticsearch.Elasticsearch(hosts=[self._host],
                                                 http_auth=http_auth,
                                                 use_ssl=self._use_ssl)
        for retry in retry_loop(15 * 60, delay=1, back_off=1.5):
            try:
                with ErrorLevelLoggingManager('elasticsearch'):
                    connection.index(body=message_body, index=dated_index,
                                     doc_type='raven-log')
            except elasticsearch.exceptions.ConnectionError as e:
                retry(e)


def retry_loop(timeout, delay, back_off=1.0):
    start_time = time.time()
    exceptions = []

    def retry(exception):
        exceptions.append(exception)

    while True:
        yield retry
        if not exceptions:
            return
        if time.time() - start_time > timeout:
            break
        else:
            exceptions.clear()
        time.sleep(delay)
        delay *= back_off

    raise exceptions[0]


class ErrorLevelLoggingManager:
    def __init__(self, logger_name):
        self._logger = logging.getLogger(logger_name)

    def __enter__(self):
        self._level = self._logger.level
        self._logger.setLevel(logging.ERROR)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._logger.setLevel(self._level)
