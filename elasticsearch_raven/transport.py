import collections
import datetime
import itertools
import json
import zlib

import elasticsearch


def decode(data):
    return json.loads(zlib.decompress(data).decode('utf-8'))


class ElasticsearchTransport:

    def __init__(self, host):
        self.connection = elasticsearch.Elasticsearch(hosts=[host])

    def send(self, data):
        self.postfix_encoded_data(data)
        index = data['project'].format(datetime.date.today())
        self.connection.index(body=data, index=index,
                              doc_type='raven-log')

    def postfix_encoded_data(self, encoded_data):
        field_names_to_postfix = ['extra']
        sentry_fields = self.keys_starting_with(encoded_data, 'sentry.')
        field_names_to_postfix.extend(sentry_fields)

        fields_to_postfix = filter(lambda x: x in field_names_to_postfix,
                                   encoded_data)

        for field in fields_to_postfix:
            _, encoded_data[field] = next(postfix_types(
                ('', encoded_data[field])))

    @staticmethod
    def keys_starting_with(dictionary, word):
        return (key for key in dictionary.keys() if key.startswith(word))


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
