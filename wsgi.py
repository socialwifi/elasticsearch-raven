import base64
import datetime
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
        fields_to_postfix.extend([key for key in encoded_data.keys() if key.startswith('sentry.')])
        for field in fields_to_postfix:
            if field in encoded_data:
                _, encoded_data[field] = next(self.postfix_types(
                    ('', encoded_data[field])))

    def postfix_types(self, row):
        name, data = row
        if isinstance(data, dict):
            if name.endswith(">"):
                name = name + "<dict>"  # to prevent name colisions
            yield name, dict(self._flatten(
                map(self.postfix_types, data.items())))
        elif isinstance(data, str):
            yield name + "<string>", data
        elif isinstance(data, list):
            for k, v in self._split_list_by_type(data).items():
                yield (name + k, v)
        elif data is None:
            yield name, None
        else:
            yield ('%s<%s>' % (name, type(data).__name__)), data

    def _split_list_by_type(self, data):
        typed_list = list(self._flatten([self.postfix_types(('', element))
                                         for element in data]))
        if not typed_list:
            return {'': []}
        else:
            result = {}
            for type, value in typed_list:
                result.setdefault(type, []).append(value)
            return result

    def _flatten(self, l):
        for element in l:
            yield from element

if __name__ == '__main__':
    httpd = make_server('', 8052, application)
    httpd.serve_forever()
