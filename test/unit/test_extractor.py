import os
import unittest

from io import TextIOBase

import pipeline as pl

HERE = os.path.abspath(os.path.dirname(__file__))

class TestFileExtractor(unittest.TestCase):
    def setUp(self):
        self.path = os.path.join(HERE, '../mock/simple_mock.csv')
        self.extractor = pl.FileExtractor(self.path)

    def test_target(self):
        self.assertEquals(self.path, self.extractor.target)

    def test_extract(self):
        f = self.extractor.extract()
        self.assertIsInstance(f, TextIOBase)
        self.assertFalse(f.closed)
        self.extractor.cleanup(f)
        self.assertTrue(f.closed)

    def test_bad_filepath(self):
        with self.assertRaises(IOError):
            pl.FileExtractor('no-file-exists-here.badext').extract()

class TestCSVExtractor(unittest.TestCase):
    def setUp(self):
        self.path = os.path.join(HERE, '../mock/simple_mock.csv')
        self.tsv_path = os.path.join(HERE, '../mock/simple_tsv_mock.tsv')
        self.extractor = pl.CSVExtractor(self.path)

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
            pl.CSVExtractor(self.path, firstline_headers=False)

    def test_extract_line(self):
        f = self.extractor.extract()

        with self.assertRaises(pl.IsHeaderException):
            self.extractor.handle_line(f.readline()),

        self.assertEquals(
            self.extractor.handle_line(f.readline()),
            {'one': '1', 'two_words': '2'}
        )
        self.extractor.cleanup(f)

    def test_extract_custom_delimiter(self):
        extractor = pl.CSVExtractor(self.tsv_path, delimiter='\t')
        self.assertEquals(extractor.delimiter, '\t')
        f = extractor.extract()

        with self.assertRaises(pl.IsHeaderException):
            extractor.handle_line(f.readline()),

        self.assertEquals(
            extractor.handle_line(f.readline()),
            {'one': '10', 'two_words': '20'}
        )
        extractor.cleanup(f)
