import base64
import collections
import datetime
import itertools
import json
from pprint import pprint
from urllib.parse import urlparse
from wsgiref.simple_server import make_server
import zlib

import elasticsearch


def application(environ, start_response):
    response_body = 'Hello World'
    status = '200 OK'
    response_headers = [('Content-Type', 'text/plain'),
                        ('Content-Length', str(len(response_body)))]

    transport = ElasticsearchTransport(
        'elasticsearch://127.0.0.1:9200/django-log-{0:%Y.%m.%d}/1')
    transport.send(environ['wsgi.input'].read())

    start_response(status, response_headers)
    return [response_body.encode('utf-8')]


class ElasticsearchTransport:
    def __init__(self, dsn):
        path, index, project = dsn.rsplit('/', 2)
        self._index = index
        parsed_url = urlparse(path)
        self.connection = elasticsearch.Elasticsearch(
            hosts=[parsed_url.netloc])

    def send(self, data):
        real_data = self.encode_data(data)
        self.postfix_encoded_data(real_data)
        pprint(real_data.keys())
        index = self._index.format(datetime.date.today())
        self.connection.index(body=real_data, index=index,
                              doc_type='raven-log')

    def encode_data(self, data):
        return json.loads(
            zlib.decompress(base64.b64decode(data)).decode('utf-8'))

    def postfix_encoded_data(self, encoded_data):
        fields_to_postfix = ['extra']
        fields_to_postfix.extend([key for key in encoded_data.keys()
                                  if key.startswith('sentry.')])
        for field in fields_to_postfix:
            if field in encoded_data:
                _, encoded_data[field] = next(postfix_types(
                    ('', encoded_data[field])))


def postfix_types(row):
    name, data = row
    if data is None:
        return postfix_none(name, data)
    type_postfix = postfixes.get(type(data), postfix_other)
    return type_postfix(name, data)


def postfix_dict(name, data):
    if name.endswith(">"):
        name = name + "<dict>"
    postfix_items = list(map(postfix_types, data.items()))
    yield name, dict(itertools.chain(*postfix_items))


def postfix_str(name, data):
    yield name + '<string>', data


def postfix_list(name, data):
    for k, v in _split_list_by_type(data).items():
        yield name + k, v


def postfix_none(name, data):
    yield name, None


def postfix_other(name, data):
    yield ('%s<%s>' % (name, type(data).__name__)), data


postfixes = {
    dict: postfix_dict,
    str: postfix_str,
    list: postfix_list,
}


def _split_list_by_type(data):
    result = collections.defaultdict(list)
    for element in data:
        for type, value in postfix_types(('', element)):
            result[type].append(value)
    if result:
        return result
    else:
        return {'': []}

if __name__ == '__main__':
    httpd = make_server('', 8052, application)
    httpd.serve_forever()
