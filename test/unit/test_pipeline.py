import unittest

import os
import pipeline as pl
from test.base import TestLoader, TestBase, TestSchema

HERE = os.path.abspath(os.path.dirname(__file__))

class TestPipeline(unittest.TestCase):
    def setUp(self):
        self.pipeline = pl.Pipeline(
            'test', 'Test',
            settings_file=os.path.join(HERE, '../mock/first_test_settings.json'),
            log_status=False
        )

    def test_get_config(self):
        config = self.pipeline.get_config()
        self.assertEquals(config['loader']['ckan']['ckan_api_key'], 'FUN FUN FUN')
        self.assertEquals(config['loader']['ckan']['ckan_root_url'], 'localhost:9000/')
        self.assertEquals(config['general']['statusdb'], ':memory:')

    def test_invalid_config(self):
        with self.assertRaises(pl.InvalidConfigException):
            pl.Pipeline(
                'test', 'Test',
                settings_file=os.path.join(HERE, 'first_test_settings.json')
            )

    def test_no_config(self):
        with self.assertRaises(pl.InvalidConfigException):
            pl.Pipeline(
                'test', 'Test',
                settings_file=os.path.join(HERE, 'NOT-A-VALID-PATH')
            )

    def test_build_config_piece_no_piece(self):
        self.assertDictEqual(
            self.pipeline.parse_config_piece('general', None),
            {'statusdb': ':memory:'}
        )

    def test_build_config_piece_one_level(self):
        self.assertDictEqual(
            self.pipeline.parse_config_piece('loader', 'ckan'),
            {
                'ckan_api_key': 'FUN FUN FUN',
                'ckan_root_url': 'localhost:9000/',
                'ckan_organization': {}
            }
        )

    def test_build_config_piece_nested_config(self):
        self.assertDictEqual(
            self.pipeline.parse_config_piece('connector', 'nested.two_levels'),
            {'key': 'value'}
        )

    def test_build_config_invalid_piece(self):
        with self.assertRaises(pl.InvalidConfigException):
            self.pipeline.parse_config_piece('loader', 'lol.nope.nuh.uh')

    def test_misconfigured_pipeline(self):
        with self.assertRaises(RuntimeError):
            pl.Pipeline('test', 'Test', log_status=False) \
                .run()
        with self.assertRaises(RuntimeError):
            pl.Pipeline('test', 'Test', log_status=False) \
                .extract(pl.CSVExtractor, None).run()
        with self.assertRaises(RuntimeError):
            pl.Pipeline('test', 'Test', log_status=False) \
                .extract(pl.CSVExtractor, None).run()
        with self.assertRaises(RuntimeError):
            pl.Pipeline('test', 'Test', log_status=False) \
                .extract(pl.CSVExtractor, None).schema(pl.BaseSchema).run()
        with self.assertRaises(RuntimeError):
            pl.Pipeline('test', 'Test', log_status=False) \
                .schema(pl.BaseSchema).load(TestLoader).run()

    def test_extractor_args(self):
        self.pipeline.extract(pl.CSVExtractor, None, 1, firstline_headers=False)
        self.assertIn(1, self.pipeline.extractor_args)
        self.assertIn('firstline_headers', self.pipeline.extractor_kwargs)

class TestStatusLogging(TestBase):
    def test_checksum_duplicate_prevention(self):
        pipeline = pl.Pipeline(
            'fatal_od_pipeline', 'Fatal OD Pipeline',
            settings_file=self.settings_file,
            log_status=True, conn=self.conn
        ) \
            .connect(pl.FileConnector, os.path.join(HERE, '../mock/simple_mock.csv')) \
            .extract(pl.CSVExtractor, firstline_headers=True) \
            .schema(TestSchema) \
            .load(self.Loader)

        pipeline.run()

        status = self.cur.execute('select * from status').fetchall()
        self.assertEquals(len(status), 1)

        with self.assertRaises(pl.DuplicateFileException):
            pipeline.run()

        status = self.cur.execute('select * from status').fetchall()
        self.assertEquals(len(status), 1)
