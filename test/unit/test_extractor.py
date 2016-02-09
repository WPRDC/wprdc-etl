import os
import csv
import xlrd
import unittest

import pipeline as pl

HERE = os.path.abspath(os.path.dirname(__file__))

class TestCSVExtractor(unittest.TestCase):
    def setUp(self):
        self.path = os.path.join(HERE, '../mock/simple_mock.csv')
        self.tsv_path = os.path.join(HERE, '../mock/simple_tsv_mock.tsv')
        self.conn = pl.FileConnector('')
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

    def test_raises_headers_exception(self):
        with self.assertRaises(pl.IsHeaderException):
            self.extractor.handle_line(['One', 'Two words'])

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


class TestExcellExtractor(unittest.TestCase):
    def setUp(self):
        self.path = os.path.join(HERE, '../mock/excel_mock.xlsx')
        self.conn = pl.FileConnector('')
        self.extractor = pl.ExcelExtractor(self.conn.connect(self.path))

    def tearDown(self):
        self.conn.close()

    def test_initialization(self):
        self.assertListEqual(self.extractor.headers, ['One', 'Two', 'Three Things'])
        self.assertListEqual(self.extractor.schema_headers, ['one', 'two', 'three_things'])
        self.assertTrue(isinstance(self.extractor.workbook, xlrd.Book))
        self.assertTrue(isinstance(self.extractor.sheet, xlrd.Sheet))

    def test_headers_change(self):
        self.assertListEqual(self.extractor.schema_headers, ['one', 'two', 'three_things'])
        self.extractor.set_headers(['new', 'new_two'])
        self.assertListEqual(self.extractor.schema_headers, ['new', 'new_two'])

    def test_raises_headers_exception(self):
        with self.assertRaises(pl.IsHeaderException):
            self.extractor.handle_line(['One', 'Two', 'Three Things'])

    def test_no_headers_error(self):
        with self.assertRaises(RuntimeError):
            pl.ExcelExtractor(
                self.conn.connect(self.path),
                firstline_headers=False
            )

    def test_extract_line(self):
        f = self.extractor.process_connection()

        self.assertEquals(
            self.extractor.handle_line(next(f)),
            {'one': '1', 'two': 'a', 'three_things': 'ccc'}
        )
