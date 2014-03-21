from unittest import TestCase
from elasticsearch_raven.transport import ElasticsearchTransport


class ElasticsearchTransportTypePostfixTest(TestCase):
    def setUp(self):
        url = 'elasticsearch:localhost/a/1'
        self.transport = ElasticsearchTransport(url)

    def test_no_extra(self):
        encoded_data = {'x': 1}
        self.transport.postfix_encoded_data(encoded_data)
        self.assertDictEqual({'x': 1}, encoded_data)

    def test_empty_extra(self):
        encoded_data = {'x': 1, 'extra': {}}
        self.transport.postfix_encoded_data(encoded_data)
        self.assertDictEqual({'x': 1, 'extra': {}}, encoded_data)

    def test_string(self):
        encoded_data = {'extra': {'x': 'test'}}
        self.transport.postfix_encoded_data(encoded_data)
        self.assertDictEqual({'extra': {'x<string>': 'test'}}, encoded_data)

    def test_none(self):
        encoded_data = {'extra': {'x': None}}
        self.transport.postfix_encoded_data(encoded_data)
        self.assertDictEqual({'extra': {'x': None}}, encoded_data)

    def test_empty_dict(self):
        encoded_data = {'extra': {'x': {}}}
        self.transport.postfix_encoded_data(encoded_data)
        self.assertDictEqual({'extra': {'x': {}}}, encoded_data)

    def test_dict(self):
        encoded_data = {'extra': {'x': {'y': 'test'}}}
        self.transport.postfix_encoded_data(encoded_data)
        self.assertDictEqual({'extra': {'x': {'y<string>': 'test'}}},
                             encoded_data)

    def test_empty_list(self):
        encoded_data = {'extra': {'x': []}}
        self.transport.postfix_encoded_data(encoded_data)
        self.assertDictEqual({'extra': {'x': []}},
                             encoded_data)

    def test_list(self):
        encoded_data = {'extra': {'x': ['string']}}
        self.transport.postfix_encoded_data(encoded_data)
        self.assertDictEqual({'extra': {'x<string>': ['string']}},
                             encoded_data)

    def test_list_with_none(self):
        encoded_data = {'extra': {'x': ['string', None]}}
        self.transport.postfix_encoded_data(encoded_data)
        self.assertDictEqual(
            {'extra': {'x<string>': ['string'],
                       'x': [None]}},
            encoded_data)

    def test_list_with_multiple_types(self):
        encoded_data = {'extra': {'x': ['string', None, {'a': 3}]}}
        self.transport.postfix_encoded_data(encoded_data)
        self.assertDictEqual(
            {'extra': {'x<string>': ['string'],
                       'x': [None, {'a<int>': 3}]}},
            encoded_data)

    def test_list_order(self):
        encoded_data = {'extra': {'x': ['string', {'a': 3},
                                        'string2', {'a': 'test'}]}}
        self.transport.postfix_encoded_data(encoded_data)
        self.assertDictEqual(
            {'extra': {'x<string>': ['string', 'string2'],
                       'x': [{'a<int>': 3}, {'a<string>': 'test'}]}},
            encoded_data)

    def test_nested_list(self):
        encoded_data = {'extra': {'x': [['string']]}}
        self.transport.postfix_encoded_data(encoded_data)
        self.assertDictEqual(
            {'extra': {'x<string>': [['string']]}},
            encoded_data)

    def test_multiple_type_nested_list(self):
        encoded_data = {'extra': {'x': [1, ['string', 2]]}}
        self.transport.postfix_encoded_data(encoded_data)
        self.assertDictEqual(
            {'extra': {'x<string>': [['string']],
                       'x<int>': [1, [2]]}},
            encoded_data)

    def test_sentry_message(self):
        key = 'sentry.interfaces.Message'
        encoded_data = {key: {'message': 'MESSAGE', 'params': 'PARAMS'}}
        self.transport.postfix_encoded_data(encoded_data)
        self.assertDictEqual({key: {'message<string>': 'MESSAGE',
                                    'params<string>': 'PARAMS'}}, encoded_data)

    def test_sentry_message_with_dict(self):
        key = 'sentry.interfaces.Message'
        encoded_data = {key: {'message': 'MESSAGE', 'params': {'p1': "P1"}}}
        self.transport.postfix_encoded_data(encoded_data)
        self.assertDictEqual({key: {'message<string>': 'MESSAGE',
                                    'params': {'p1<string>': "P1"}}},
                             encoded_data)
