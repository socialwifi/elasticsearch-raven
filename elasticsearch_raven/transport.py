import base64
import collections
import contextlib
import datetime
import hashlib
import itertools
import json
import logging
import re
import sys
import time
import zlib

import elasticsearch

from elasticsearch_raven import configuration
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
    DOCUMENT_TYPE = 'raven-log'

    def __init__(self, host, use_ssl=False):
        self._host = host
        self._use_ssl = use_ssl

    def send_message(self, message):
        message_body = message.decode_body()
        postfix_encoded_data(message_body)
        message_id = hash_dict(message_body)
        http_auth = self._get_http_auth(message)
        index = message_body['project'].format(datetime.datetime.now())
        self.send(message_body, index, message_id, http_auth)

    @staticmethod
    def _get_http_auth(message):
        return '{}:{}'.format(message.headers['sentry_key'],
                              message.headers['sentry_secret'])

    def send(self, body, index, message_id, http_auth=None):
        connection = self._connect(http_auth)
        for retry in retry_loop(15 * 60, delay=1, back_off=1.5):
            try:
                with logger_level_to_error('elasticsearch'):
                    connection.index(body=body, index=index,
                                     id=message_id,
                                     doc_type=self.DOCUMENT_TYPE)
            except elasticsearch.exceptions.ConnectionError as e:
                retry(e)

    def _connect(self, http_auth):
        return elasticsearch.Elasticsearch(hosts=[self._host],
                                           http_auth=http_auth,
                                           use_ssl=self._use_ssl)

    def search(self, http_auth=None, segment_size=1000, **kwargs):
        connection = self._connect(http_auth)
        for offset in itertools.count(step=segment_size):
            response = connection.search(doc_type=self.DOCUMENT_TYPE,
                                         size=segment_size, from_=offset,
                                         **kwargs)
            hits = response['hits']['hits']
            for hit in hits:
                yield hit
            if len(hits) < segment_size:
                break

    def delete(self, index, record_id, http_auth=None):
        connection = self._connect(http_auth)
        connection.delete(index, self.DOCUMENT_TYPE, record_id)


def hash_dict(dictionary):
    message_json = json.dumps(
        dictionary, indent=None, ensure_ascii=True, separators=None,
        sort_keys=True)
    sha1 = hashlib.sha1()
    sha1.update(message_json.encode('ascii'))
    return sha1.hexdigest()


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


def update_ids():
    log_transport = LogTransport(configuration['host'],
                                 configuration['use_ssl'])
    all_count, modified_count = 0, 0
    for log in log_transport.search():
        all_count += 1
        log_id = hash_dict(log['_source'])
        if log['_id'] != log_id:
            modified_count += 1
            log_transport.send(log['_source'], log['_index'], log_id,
                               configuration['error_http_auth'])
            log_transport.delete(log['_index'], log['_id'])
    sys.stdout.write('Logs: {}\nModified: {}\n'.format(all_count,
                                                       modified_count))
