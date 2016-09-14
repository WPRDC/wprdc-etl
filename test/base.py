import os
import json
import unittest
import sqlite3
from marshmallow import fields

HERE = os.path.abspath(os.path.dirname(__file__))

from pipeline.loaders import Loader
from pipeline.extractors import Extractor
from pipeline.connectors import Connector
from pipeline.schema import BaseSchema

class TestSchema(BaseSchema):
    death_date = fields.DateTime(format='%m/%d/%Y')
    death_time = fields.DateTime(format='%I:%M %p')
    death_date_and_time = fields.DateTime(dump_only=True)
    manner_of_death = fields.String()
    age = fields.Integer()
    sex = fields.String()
    race = fields.String()
    case_dispo = fields.String()
    combined_od1 = fields.String(allow_none=True)
    combined_od2 = fields.String(allow_none=True)
    combined_od3 = fields.String(allow_none=True)
    combined_od4 = fields.String(allow_none=True)
    combined_od5 = fields.String(allow_none=True)
    combined_od6 = fields.String(allow_none=True)
    combined_od7 = fields.String(allow_none=True)
    incident_zip = fields.Integer()
    decedent_zip = fields.Integer()
    case_year = fields.Integer()

class TestLoader(Loader):
    def load(self, data):
        pass

class TestConnector(Connector):
    def connect(self, target):
        return []

    def checksum_contents(self, target):
        return ''

    def close(self):
        return True

class TestExtractor(Extractor):
    def process_connection(self):
        return iter([])

    def handle_line(self, line):
        return []

    def set_headers(self):
        pass

    def extract(self):
        return []

class TestBase(unittest.TestCase):
    def setUp(self):
        self.settings_file = os.path.join(HERE, 'mock/first_test_settings.json')
        self.Connector = TestConnector
        self.Loader = TestLoader

        with open(self.settings_file) as f:
            db = json.loads(f.read())['general']['statusdb']

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
                input_checksum TEXT,
                status TEXT,
                num_lines INTEGER,
                PRIMARY KEY (display_name, start_time)
            )
            '''
        )

    def tearDown(self):
        self.conn.close()
