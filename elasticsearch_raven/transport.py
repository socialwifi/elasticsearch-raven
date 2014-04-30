import base64
import collections
import contextlib
import datetime
import hashlib
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


class LogTransport:
    def __init__(self, host, use_ssl=False):
        self._host = host
        self._use_ssl = use_ssl

    def send(self, message):
        message_body = message.decode_body()
        postfix_encoded_data(message_body)
        message_id = self._get_id(message_body)
        http_auth = self._get_http_auth(message)
        index = message_body['project'].format(datetime.datetime.now())
        self._send(message_body, index, http_auth, message_id)

    @staticmethod
    def _get_id(message_body):
        message_json = json.dumps(
            message_body, indent=None, ensure_ascii=True,
            separators=(',', ':'), sort_keys=True)
        sha1 = hashlib.sha1()
        sha1.update(message_json.encode('ascii'))
        return sha1.hexdigest()

    @staticmethod
    def _get_http_auth(message):
        return '{}:{}'.format(message.headers['sentry_key'],
                              message.headers['sentry_secret'])

    def _send(self, body, index, http_auth, message_id):
        connection = elasticsearch.Elasticsearch(
            hosts=[self._host], http_auth=http_auth, use_ssl=self._use_ssl)
        for retry in retry_loop(15 * 60, delay=1, back_off=1.5):
            try:
                with logger_level_to_error('elasticsearch'):
                    connection.index(body=body, index=index, id=message_id,
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


@contextlib.contextmanager
def logger_level_to_error(logger_name):
    logger = logging.getLogger(logger_name)
    level = logger.level
    logger.setLevel(logging.ERROR)
    yield
    logger.setLevel(level)
