import unittest
import os
import json

from unittest.mock import Mock, patch

from pipeline.loaders import Datapusher

HERE = os.path.abspath(os.path.dirname(__file__))

class TestDatapusher(unittest.TestCase):
    def setUp(self):
        super(TestDatapusher, self).setUp()
        self.data_pusher = Datapusher(
                server='testing',
                settings_file=os.path.join(HERE, 'test_settings.json')
        )

    def test_datapusher_init(self):
        self.assertIsNotNone(self.data_pusher)

    def test_datapusher_exception_no_settings(self):
        with self.assertRaises(KeyError):
            Datapusher({})

    @patch('requests.post')
    def test_resource_exists(self, post):
        mock_post = Mock()
        mock_post.content.side_effect = [
            json.dumps({'result': {'resource': []}}),
            json.dumps({'result': {'resource': [{'name': 'NOT EXIST'}]}}),
            json.dumps({'result': {'resource': [{'name': 'exists'}]}}),
        ]
        post.return_value = mock_post

        self.assertFalse(self.data_pusher.resource_exists(None, 'exists'))
        self.assertFalse(self.data_pusher.resource_exists(None, 'exists'))
        self.assertTrue(self.data_pusher.resource_exists(None, 'exists'))

    def test_create_resource(self):
        self.assertTrue(False)

    def test_create_datastore(self):
        self.assertTrue(False)

    def test_delete_datastore(self):
        self.assertTrue(False)

    def test_upsert(self):
        self.assertTrue(False)

    def test_update_metadata(self):
        self.assertTrue(False)

    def test_resource_search(self):
        self.assertTrue(False)
