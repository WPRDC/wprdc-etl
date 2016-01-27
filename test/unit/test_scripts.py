import os
import sqlite3

from unittest import TestCase

from click.testing import CliRunner

import pipeline as pl
from pipeline.scripts import create_db, run_job

from test.jobs.base import TestLoader, TestExtractor, TestConnector

HERE = os.path.abspath(os.path.dirname(__file__))
SETTINGS_FILE = os.path.join(HERE, '../mock/test_settings.json')

class TestCreateDBScript(TestCase):
    def setUp(self):
        self.runner = CliRunner()

    def test_create_database(self):
        with self.runner.isolated_filesystem():
            with open('test_settings.json', 'w') as f:
                f.write('''{"cli_testing": {"statusdb": "test.db"}}''')

            result = self.runner.invoke(create_db, ['test_settings.json', '--server', 'cli_testing'])

            self.assertEquals(result.exit_code, 0)
            self.assertTrue('test.db' in os.listdir())

            os.unlink(os.path.join(os.getcwd(), 'test.db'))

    def test_create_and_drop_database(self):
        with self.runner.isolated_filesystem():

            conn = sqlite3.connect(os.path.join(os.getcwd(), 'test.db'))
            cur = conn.cursor()
            cur.execute('create table status (name TEXT NOT NULL)')
            cur.execute("insert into status (name) values ('test')")
            conn.close()

            self.assertTrue('test.db' in os.listdir())

            with open('test_settings.json', 'w') as f:
                f.write('''{"cli_testing": {"statusdb": "test.db"}}''')

            result = self.runner.invoke(create_db, ['test_settings.json', '--server', 'cli_testing', '--drop'])

            self.assertEquals(result.exit_code, 0)
            self.assertTrue('test.db' in os.listdir())

            conn = sqlite3.connect(os.path.join(os.getcwd(), 'test.db'))
            cur = conn.cursor()
            cur.execute("select count(*) from status")
            status_count = cur.fetchall()[0][0]
            conn.close()

            self.assertEquals(status_count, 0)
            os.unlink(os.path.join(os.getcwd(), 'test.db'))

    def test_bad_server_name(self):
        with self.runner.isolated_filesystem():
            with open('test_settings.json', 'w') as f:
                f.write('''{"cli_testing": {"statusdb": "test.db"}}''')
            result = self.runner.invoke(create_db, ['test_settings.json', '--server', 'DOES NOT EXIST'])
            self.assertNotEquals(result.exit_code, 0)
            self.assertTrue('invalid choice: DOES NOT EXIST' in result.output)

    def test_bad_config_path(self):
        with self.runner.isolated_filesystem():
            result = self.runner.invoke(create_db, ['./no_file_here.txt'])
            self.assertNotEquals(result.exit_code, 0)
            self.assertTrue('"./no_file_here.txt" does not exist' in result.output)

    def test_malformed_config(self):
        with self.runner.isolated_filesystem():
            with open('test_settings.json', 'w') as f:
                f.write('''This isn't valid JSON doc!''')
            result = self.runner.invoke(create_db, ['test_settings.json', '--server', 'DOES NOT EXIST'])
            self.assertNotEquals(result.exit_code, 0)
            self.assertTrue('invalid JSON in settings file' in result.output)

    def test_no_statusdb_in_config(self):
        with self.runner.isolated_filesystem():
            with open('test_settings.json', 'w') as f:
                f.write('''{"cli_testing": {"lolnostatusdb": "test.db"}}''')
            result = self.runner.invoke(create_db, ['test_settings.json', '--server', 'cli_testing'])
            self.assertNotEquals(result.exit_code, 0)
            self.assertTrue('CONFIG must contain a location for a statusdb' in result.output)

test_pipeline = pl.Pipeline(
    'test', 'Test',
    server='testing',
    settings_file=SETTINGS_FILE,
    log_status=False
).connect(TestConnector, None) \
    .extract(TestExtractor) \
    .schema(pl.BaseSchema) \
    .load(TestLoader)

class Junk(object):
    pass

junk = Junk()

class TestRunJobScript(TestCase):
    def setUp(self):
        self.runner = CliRunner()

    def test_run_job_successfully(self):
        result = self.runner.invoke(run_job, ['test.unit.test_scripts:test_pipeline'])
        self.assertEquals(result.exit_code, 0)

    def test_run_job_no_job(self):
        result = self.runner.invoke(run_job, ['nope.does.not:exist'])
        self.assertNotEquals(result.exit_code, 0)
        self.assertTrue('A Pipeline could not be found' in result.output)

    def test_run_job_no_pipline(self):
        result = self.runner.invoke(run_job, ['test.unit.test_scripts:junk'])
        self.assertNotEquals(result.exit_code, 0)
        self.assertTrue('A Pipeline could not be found' in result.output)

    def test_run_job_custom_settings(self):
        result = self.runner.invoke(
            run_job, [
                'test.unit.test_scripts:test_pipeline',
                '--config', SETTINGS_FILE,
                '--server', 'second_testing']
        )
        self.assertEquals(result.exit_code, 0)
