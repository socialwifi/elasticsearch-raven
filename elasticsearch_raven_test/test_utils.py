from unittest import TestCase
from elasticsearch_raven.utils import get_index


class GetIndexTest(TestCase):
    def test_empty(self):
        arg = {}
        self.assertRaises(AttributeError, get_index, arg)

    def test_empty_path(self):
        arg = {'PATH_INFO': ''}
        self.assertRaises(AttributeError, get_index, arg)

    def test_bad_path(self):
        arg = {'PATH_INFO': '/bad_path/'}
        self.assertRaises(AttributeError, get_index, arg)

    def test_good_path(self):
        arg = {'PATH_INFO': '/api/abc123/store/'}
        result = get_index(arg)
        self.assertEqual('abc123', result)

    def test_good_subpath(self):
        arg = {'PATH_INFO': '/api/abc123/d4/store/'}
        result = get_index(arg)
        self.assertEqual('abc123/d4', result)
