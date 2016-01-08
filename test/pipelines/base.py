import os
import json
import unittest
import sqlite3

HERE = os.path.abspath(os.path.dirname(__file__))

from pipeline.loaders import Loader

class TestLoader(Loader):
    def load(self, data):
        pass

class TestBase(unittest.TestCase):
    def setUp(self):
        self.default_server = 'testing'
        self.settings_file = os.path.join(HERE, '../mock/test_settings.json')
        self.Loader = TestLoader

        with open(self.settings_file) as f:
            db = json.loads(f.read())[self.default_server]['statusdb']

        self.conn = sqlite3.connect(db)
        self.cur = self.conn.cursor()
        self.cur.execute(
            '''
            CREATE TABLE IF NOT EXISTS
            status (
                name TEXT NOT NULL,
                display_name TEXT,
                last_ran INTEGER,
                start_time INTEGER NOT NULL,
                status TEXT,
                num_lines INTEGER,
                PRIMARY KEY (display_name, start_time)
            )
            '''
        )

    def tearDown(self):
        self.conn.close()
