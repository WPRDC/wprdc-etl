import unittest
import os
import json

from unittest.mock import Mock, patch, PropertyMock

import pipeline as pl
from pipeline.loaders import CKANLoader
from pipeline.exceptions import CKANException

HERE = os.path.abspath(os.path.dirname(__file__))


class TestCKANDatastoreBase(unittest.TestCase):
    def setUp(self):
        self.pipeline = pl.Pipeline(
            'test', 'Test',
            log_status=False
        )
        settings_file = os.path.join(HERE, '../mock/first_test_settings.json')
        with open(settings_file) as f:
            self.ckan_config = json.load(f)['loader']['ckan']
        pass


class TestCKANDatastore(TestCKANDatastoreBase):
    def setUp(self):
        patcher = patch('requests.post')
        mock_post = patcher.start()
        mock_post.json.side_effect = [
            {'id': {'someNumber': []}},
        ]
        super(TestCKANDatastore, self).setUp()
        self.ckan_loader = CKANLoader(**self.ckan_config)
        patcher.stop()

    def test_datapusher_init(self):
        self.assertIsNotNone(self.ckan_loader)
        self.assertEquals(self.ckan_loader.ckan_url, 'localhost:9000/api/3/')
        self.assertEquals(self.ckan_loader.dump_url, 'localhost:9000/datastore/dump/')

    @patch('requests.post')
    def test_get_resource_id(self, post):
        mock_post = Mock()
        mock_post.json.side_effect = [
            {'result': {'resources': []}},
            {'result': {'resources': [{'name': 'NOT EXIST', 'id': 'who cares'}]}},
            {'result': {'resources': [{'name': 'exists', 'id': 'anID'}]}},
        ]
        post.return_value = mock_post

        self.assertIsNone(self.ckan_loader.get_resource_id(None, 'anything'))
        self.assertIsNone(self.ckan_loader.get_resource_id(None, 'exists'))
        self.assertEqual(self.ckan_loader.get_resource_id(None, 'exists'), 'anID')

    @patch('requests.post')
    def test_resource_exists(self, post):
        mock_post = Mock()
        mock_post.json.side_effect = [
            {'result': {'resources': []}},
            {'result': {'resources': [{'name': 'NOT EXIST', 'id': 'who cares'}]}},
            {'result': {'resources': [{'name': 'exists', 'id': 'anID'}]}},
        ]
        post.return_value = mock_post

        self.assertFalse(self.ckan_loader.resource_exists(None, 'exists'))
        self.assertFalse(self.ckan_loader.resource_exists(None, 'exists'))
        self.assertTrue(self.ckan_loader.resource_exists(None, 'exists'))

    @patch('requests.post')
    def test_create_resource(self, post):
        mock_post = Mock()
        mock_post.json.side_effect = [
            {'success': True, 'result': {'id': 1}},
            {'error': {'__type': ['not successful']}}
        ]
        post.return_value = mock_post

        self.assertEquals(self.ckan_loader.create_resource(None, None), 1)
        with self.assertRaises(CKANException):
            self.ckan_loader.create_resource(None, None)

    @patch('requests.post')
    def test_create_datastore(self, post):
        mock_post = Mock()
        mock_post.json.side_effect = [
            {'success': True, 'result': {'resource_id': 1}},
            {'error': {'name': ['not successful']}}
        ]
        post.return_value = mock_post

        self.assertEquals(self.ckan_loader.create_datastore(None, []), 1)
        with self.assertRaises(CKANException):
            self.ckan_loader.create_datastore(None, [])

    @patch('requests.post')
    def test_generate_datastore(self, post):
        mock_post = Mock()
        mock_post.json.side_effect = [
            {'success': True, 'result': {'id': 1}},
            {'success': True, 'result': {'resource_id': 1}},
        ]
        post.return_value = mock_post

        self.assertEquals(self.ckan_loader.generate_datastore([], False, False), 1)
        self.assertEquals(self.ckan_loader.generate_datastore([], True, False), 1)

    @patch('requests.post')
    def test_delete_datastore(self, post):
        type(post.return_value).status_code = PropertyMock(return_value=204)
        self.assertEquals(self.ckan_loader.delete_datastore(None), 204)

    @patch('requests.post')
    def test_upsert(self, post):
        type(post.return_value).status_code = PropertyMock(return_value=200)
        self.assertEquals(self.ckan_loader.upsert(None, None), 200)

    @patch('requests.post')
    def test_update_metadata(self, post):
        type(post.return_value).status_code = PropertyMock(return_value=200)
        self.assertEquals(self.ckan_loader.update_metadata(None), 200)


class TestCKANDatastoreLoader(TestCKANDatastoreBase):
    def setUp(self):
        patcher = patch('requests.post')
        mock_post = patcher.start()
        mock_post.json.side_effect = [
            {'id': {'someNumber': []}},
            {'id': {'someNumber': []}}
        ]
        super(TestCKANDatastoreLoader, self).setUp()
        self.insert_loader = pl.CKANDatastoreLoader(
            **self.ckan_config, fields=[],
            method='insert'
        )

        self.upsert_loader = pl.CKANDatastoreLoader(
            **self.ckan_config,
            method='upsert',
            fields=[
                {
                    "type": "text",
                    "id": "words"
                },
                {
                    "type": "numeric",
                    "id": "numbers"
                }
            ],
            key_fields=['words']
        )
        self.error_codes = [409, 500]
        patcher.stop()

    @patch('requests.post')
    def test_datastore_loader_no_fields(self, post):
        mock_post = Mock()
        mock_post.json.side_effect = [
            {'id': {'someNumber': []}}
        ]
        with self.assertRaises(RuntimeError):
            pl.CKANDatastoreLoader(**self.ckan_config)

    @patch('requests.post')
    def test_datastore_load__insert_successful(self, post):
        mock_post = Mock()
        mock_post.json.side_effect = [
            {'success': True, 'result': {'id': 1}},
            {'success': True, 'result': {'resource_id': 1}},
        ]
        post.return_value = mock_post
        self.insert_loader.load([])

    @patch('requests.post')
    def test_datastore_load_insert_failed(self, post):
        mock_post = Mock()
        mock_post.json.side_effect = [
            {'success': True, 'result': {'id': 1}},
            {'success': True, 'result': {'resource_id': 1}},
            {'success': False, 'result': {'help': 'really doesn\'t matter'}},
            {'success': False, 'result': {'help': 'some help text'}},
        ]
        post.return_value = mock_post

        for error in self.error_codes:
            type(post.return_value).status_code = PropertyMock(return_value=error)

            with self.assertRaises(RuntimeError):
                self.insert_loader.load([])

    @patch('requests.post')
    def test_datastore_load__upsert_successful(self, post):
        mock_post = Mock()
        mock_post.json.side_effect = [
            {'success': True, 'result': {'id': 1}},
            {'success': True, 'result': {'resource_id': 1}},
        ]
        post.return_value = mock_post
        self.upsert_loader.load([])

    @patch('requests.post')
    def test_datastore_load_upsert_failed(self, post):
        mock_post = Mock()
        mock_post.json.side_effect = [
            {'success': True, 'result': {'id': 1}},
            {'success': True, 'result': {'resource_id': 1}},
            {'success': False, 'result': {'help': 'really doesn\'t matter'}},
            {'success': False, 'result': {'help': 'some help text'}},
        ]
        post.return_value = mock_post

        for error in self.error_codes:
            type(post.return_value).status_code = PropertyMock(return_value=error)

            with self.assertRaises(RuntimeError):
                self.upsert_loader.load([])

    @patch('requests.post')
    def test_datastore_load_insert_update_metadata_failed(self, post):
        mock_post = Mock()
        mock_post.json.side_effect = [
            {'success': True, 'result': {'id': 1}},
            {'success': True, 'result': {'resource_id': 1}},
        ]
        post.return_value = mock_post

        for error in self.error_codes:
            type(post.return_value).status_code = PropertyMock(side_effect=[200, 500])
            with self.assertRaises(RuntimeError):
                self.insert_loader.load([])

    @patch('requests.post')
    def test_datastore_load_upsert_update_metadata_failed(self, post):
        mock_post = Mock()
        mock_post.json.side_effect = [
            {'success': True, 'result': {'id': 1}},
            {'success': True, 'result': {'resource_id': 1}},
        ]
        post.return_value = mock_post

        for error in self.error_codes:
            type(post.return_value).status_code = PropertyMock(return_value=error)
            with self.assertRaises(RuntimeError):
                self.upsert_loader.load([])