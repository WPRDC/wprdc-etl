import os
import unittest

import pipeline as pl

from test.jobs.base import TestLoader

HERE = os.path.abspath(os.path.dirname(__file__))

class TestPipeline(unittest.TestCase):
    def setUp(self):
        self.pipeline = pl.Pipeline(
            'test', 'Test', server='testing',
            settings_file=os.path.join(HERE, '../mock/test_settings.json'),
            log_status=False
        )

    def test_get_config(self):
        config = self.pipeline.get_config()
        self.assertEquals(config['api_key'], 'FUN FUN FUN')
        self.assertEquals(config['root_url'], 'localhost:9000/')
        self.assertEquals(config['organizations'], {})

    def test_reset_config(self):
        self.pipeline.set_config_from_file(
            'second_testing', os.path.join(HERE, '../mock/test_settings.json')
        )
        config = self.pipeline.get_config()
        self.assertEquals(config['api_key'], 'EVEN MORE FUN',)
        self.assertEquals(config['root_url'], 'localhost:9001/',)
        self.assertEquals(config['organizations'], {})

    def test_invalid_config(self):
        with self.assertRaises(pl.InvalidConfigException):
            pl.Pipeline(
                'test', 'Test', server='NO SERVER',
                settings_file=os.path.join(HERE, 'test_settings.json')
            )

    def test_no_config(self):
        with self.assertRaises(pl.InvalidConfigException):
            pl.Pipeline(
                'test', 'Test', server='NO SERVER',
                settings_file=os.path.join(HERE, 'NOT-A-VALID-PATH')
            )

    def test_misconfigured_pipeline(self):
        with self.assertRaises(RuntimeError):
            pl.Pipeline('test', 'Test', log_status=False) \
                .run()
        with self.assertRaises(RuntimeError):
            pl.Pipeline('test', 'Test', log_status=False) \
                .extract(pl.FileExtractor, None).run()
        with self.assertRaises(RuntimeError):
            pl.Pipeline('test', 'Test', log_status=False) \
                .extract(pl.FileExtractor, None).run()
        with self.assertRaises(RuntimeError):
            pl.Pipeline('test', 'Test', log_status=False) \
                .extract(pl.FileExtractor, None).schema(pl.BaseSchema).run()
        with self.assertRaises(RuntimeError):
            pl.Pipeline('test', 'Test', log_status=False) \
                .schema(pl.BaseSchema).load(TestLoader).run()

    def test_extractor_args(self):
        self.pipeline.extract(pl.FileExtractor, None, 1, firstline_headers=False)
        self.assertIsNone(self.pipeline.target)
        self.assertIn(1, self.pipeline.extractor_args)
        self.assertIn('firstline_headers', self.pipeline.extractor_kwargs)
