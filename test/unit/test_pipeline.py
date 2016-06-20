import unittest

import os
import pipeline as pl
from test.base import TestLoader, TestBase, TestSchema

HERE = os.path.abspath(os.path.dirname(__file__))

class TestPipeline(unittest.TestCase):
    def setUp(self):
        self.pipeline = pl.Pipeline(
            'test', 'Test',
            log_status=False
        )

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
            conn=self.conn, log_status=True
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

    def test_missing_connection_name(self):
        pipeline = pl.Pipeline(
            'fatal_od_pipeline', 'Fatal OD Pipeline',
            log_status=True
        ) \
            .connect(pl.FileConnector, os.path.join(HERE, '../mock/simple_mock.csv')) \
            .extract(pl.CSVExtractor, firstline_headers=True) \
            .schema(TestSchema) \
            .load(self.Loader)

        with self.assertRaises(pl.MissingDatabaseName):
            pipeline.run()

    def test_connection_creation(self):
        pipeline = pl.Pipeline(
            'fatal_od_pipeline', 'Fatal OD Pipeline',
            conn_name=":memory:",log_status=True
        ) \
            .connect(pl.FileConnector, os.path.join(HERE, '../mock/simple_mock.csv')) \
            .extract(pl.CSVExtractor, firstline_headers=True) \
            .schema(TestSchema) \
            .load(self.Loader)