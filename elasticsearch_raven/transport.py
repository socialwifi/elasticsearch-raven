import datetime
import json
import zlib

import elasticsearch
from elasticsearch_raven.postfix import postfix_encoded_data


def decode(data):
    return json.loads(zlib.decompress(data).decode('utf-8'))


class ElasticsearchTransport:

    def __init__(self, host):
        self.connection = elasticsearch.Elasticsearch(hosts=[host])

    def send(self, data):
        postfix_encoded_data(data)
        index = data['project'].format(datetime.date.today())
        self.connection.index(body=data, index=index,
                              doc_type='raven-log')

