from unittest import TestCase
from elasticsearch_raven import postfix


class ElasticsearchTransportTypePostfixTest(TestCase):
    def test_no_extra(self):
        encoded_data = {'x': 1}
        postfix.postfix_encoded_data(encoded_data)
        self.assertDictEqual({'x': 1}, encoded_data)

    def test_empty_extra(self):
        encoded_data = {'x': 1, 'extra': {}}
        postfix.postfix_encoded_data(encoded_data)
        self.assertDictEqual({'x': 1, 'extra': {}}, encoded_data)

    def test_string(self):
        encoded_data = {'extra': {'x': 'test'}}
        postfix.postfix_encoded_data(encoded_data)
        self.assertDictEqual({'extra': {'x<string>': 'test'}}, encoded_data)

    def test_none(self):
        encoded_data = {'extra': {'x': None}}
        postfix.postfix_encoded_data(encoded_data)
        self.assertDictEqual({'extra': {'x': None}}, encoded_data)

    def test_empty_dict(self):
        encoded_data = {'extra': {'x': {}}}
        postfix.postfix_encoded_data(encoded_data)
        self.assertDictEqual({'extra': {'x': {}}}, encoded_data)

    def test_dict(self):
        encoded_data = {'extra': {'x': {'y': 'test'}}}
        postfix.postfix_encoded_data(encoded_data)
        self.assertDictEqual({'extra': {'x': {'y<string>': 'test'}}},
                             encoded_data)

    def test_empty_list(self):
        encoded_data = {'extra': {'x': []}}
        postfix.postfix_encoded_data(encoded_data)
        self.assertDictEqual({'extra': {'x': []}},
                             encoded_data)

    def test_list(self):
        encoded_data = {'extra': {'x': ['string']}}
        postfix.postfix_encoded_data(encoded_data)
        self.assertDictEqual({'extra': {'x<string>': ['string']}},
                             encoded_data)

    def test_list_with_none(self):
        encoded_data = {'extra': {'x': ['string', None]}}
        postfix.postfix_encoded_data(encoded_data)
        self.assertDictEqual(
            {'extra': {'x<string>': ['string'],
                       'x': [None]}},
            encoded_data)

    def test_list_with_multiple_types(self):
        encoded_data = {'extra': {'x': ['string', None, {'a': 3}]}}
        postfix.postfix_encoded_data(encoded_data)
        self.assertDictEqual(
            {'extra': {'x<string>': ['string'],
                       'x': [None, {'a<int>': 3}]}},
            encoded_data)

    def test_list_order(self):
        encoded_data = {'extra': {'x': ['string', {'a': 3},
                                        'string2', {'a': 'test'}]}}
        postfix.postfix_encoded_data(encoded_data)
        self.assertDictEqual(
            {'extra': {'x<string>': ['string', 'string2'],
                       'x': [{'a<int>': 3}, {'a<string>': 'test'}]}},
            encoded_data)

    def test_nested_list(self):
        encoded_data = {'extra': {'x': [['string']]}}
        postfix.postfix_encoded_data(encoded_data)
        self.assertDictEqual(
            {'extra': {'x<string>': [['string']]}},
            encoded_data)

    def test_multiple_type_nested_list(self):
        encoded_data = {'extra': {'x': [1, ['string', 2]]}}
        postfix.postfix_encoded_data(encoded_data)
        self.assertDictEqual(
            {'extra': {'x<string>': [['string']],
                       'x<int>': [1, [2]]}},
            encoded_data)

    def test_sentry_message(self):
        key = 'sentry.interfaces.Message'
        encoded_data = {key: {'message': 'MESSAGE', 'params': 'PARAMS'}}
        postfix.postfix_encoded_data(encoded_data)
        self.assertDictEqual({key: {'message<string>': 'MESSAGE',
                                    'params<string>': 'PARAMS'}}, encoded_data)

    def test_sentry_message_with_dict(self):
        key = 'sentry.interfaces.Message'
        encoded_data = {key: {'message': 'MESSAGE', 'params': {'p1': "P1"}}}
        postfix.postfix_encoded_data(encoded_data)
        self.assertDictEqual({key: {'message<string>': 'MESSAGE',
                                    'params': {'p1<string>': "P1"}}},
                             encoded_data)


class PostfixNoneTest(TestCase):
    def test_generator(self):
        self.args = 'test_name', None
        self.result = 'test_name', None
        result = postfix.postfix_none(*self.args)
        self.assertEqual(self.result, next(result))
        self.assertRaises(StopIteration, next, result)


class PostfixOtherTest(TestCase):
    def test_generator(self):
        args = 'test_name', type('TestObject', (), {})
        self.result = 'test_name<type>', args[1]
        result = postfix.postfix_other(*args)
        self.assertEqual(self.result, next(result))
        self.assertRaises(StopIteration, next, result)


class PostfixDictTest(TestCase):
    def test_generator(self):
        args = 'test_name', {}
        self.result = 'test_name', {}
        result = postfix.postfix_dict(*args)
        self.assertEqual(self.result, next(result))
        self.assertRaises(StopIteration, next, result)

    def test_with_items(self):
        args = 'test_name', {'a': 1, 'b': 1.0}
        result = postfix.postfix_dict(*args)
        self.assertEqual(('test_name', {'a<int>': 1, 'b<float>': 1.0}),
                         next(result))


class PostfixListTest(TestCase):
    def test_generator(self):
        args = 'test_name', []
        self.result = 'test_name', []
        result = postfix.postfix_list(*args)
        self.assertEqual(self.result, next(result))
        self.assertRaises(StopIteration, next, result)

    def test_with_items(self):
        args = 'test_name', [1, 'a', 2]
        result = dict(postfix.postfix_list(*args))
        self.assertEqual([1, 2], result['test_name<int>'])
        self.assertEqual(['a'], result['test_name<string>'])
