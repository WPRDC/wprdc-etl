import os
import csv
import unittest

import pipeline as pl

HERE = os.path.abspath(os.path.dirname(__file__))

class TestCSVExtractor(unittest.TestCase):
    def setUp(self):
        self.path = os.path.join(HERE, '../mock/simple_mock.csv')
        self.tsv_path = os.path.join(HERE, '../mock/simple_tsv_mock.tsv')
        self.conn = pl.LocalFileConnector()
        self.extractor = pl.CSVExtractor(self.conn.connect(self.path))

    def tearDown(self):
        self.conn.close()

    def test_initialization(self):
        self.assertListEqual(self.extractor.headers, ['One', 'Two words'])
        self.assertListEqual(self.extractor.schema_headers, ['one', 'two_words'])
        self.assertEquals(self.extractor.delimiter, ',')

    def test_headers_change(self):
        self.assertListEqual(self.extractor.schema_headers, ['one', 'two_words'])
        self.extractor.set_headers(['new', 'new_two'])
        self.assertListEqual(self.extractor.schema_headers, ['new', 'new_two'])

    def test_no_headers_error(self):
        with self.assertRaises(RuntimeError):
            pl.CSVExtractor(
                self.conn.connect(self.path),
                firstline_headers=False
            )

    def test_extract_line(self):
        f = self.extractor.process_connection()

        self.assertEquals(
            self.extractor.handle_line(next(f)),
            {'one': '1', 'two_words': '2'}
        )

    def test_extract_custom_delimiter(self):
        extractor = pl.CSVExtractor(
            self.conn.connect(self.tsv_path), delimiter='\t'
        )

        self.assertEquals(extractor.delimiter, '\t')
        f = extractor.process_connection()

        self.assertEquals(
            extractor.handle_line(next(f)),
            {'one': '10', 'two_words': '20'}
        )
