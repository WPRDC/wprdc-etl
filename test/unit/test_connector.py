import os
import unittest

from io import TextIOBase

import pipeline as pl

HERE = os.path.abspath(os.path.dirname(__file__))

class TestCSVConnector(unittest.TestCase):
    def setUp(self):
        self.path = os.path.join(HERE, '../mock/simple_mock.csv')
        self.connector = pl.LocalFileConnector()

    def test_connect(self):
        f = self.connector.connect(self.path)
        self.assertIsInstance(f, TextIOBase)
        self.assertFalse(f.closed)

    def test_close(self):
        f = self.connector.connect(self.path)
        self.assertFalse(f.closed)
        self.connector.close()
        self.assertTrue(f.closed)
