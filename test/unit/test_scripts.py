import sqlite3
from unittest import TestCase

import os
import pipeline as pl
from click.testing import CliRunner
from pipeline.scripts import create_db, run_job
from test.base import TestLoader, TestExtractor, TestConnector

HERE = os.path.abspath(os.path.dirname(__file__))


class TestCreateDBScript(TestCase):
    def setUp(self):
        self.runner = CliRunner()

    def test_create_database(self):
        with self.runner.isolated_filesystem():
            result = self.runner.invoke(create_db, ['test.db'])

            self.assertEquals(result.exit_code, 0)
            self.assertTrue('test.db' in os.listdir())

            os.unlink(os.path.join(os.getcwd(), 'test.db'))

    def test_create_and_drop_database(self):
        with self.runner.isolated_filesystem():
            conn = sqlite3.connect(os.path.join(os.getcwd(), 'test.db'))
            cur = conn.cursor()
            cur.execute('CREATE TABLE status (name TEXT NOT NULL)')
            cur.execute("INSERT INTO status (name) VALUES ('test')")
            conn.close()

            self.assertTrue('test.db' in os.listdir())

            result = self.runner.invoke(create_db, ['test.db', '--drop'])

            self.assertEquals(result.exit_code, 0)
            self.assertTrue('test.db' in os.listdir())

            conn = sqlite3.connect(os.path.join(os.getcwd(), 'test.db'))
            cur = conn.cursor()
            cur.execute("SELECT count(*) FROM status")
            status_count = cur.fetchall()[0][0]
            conn.close()

            self.assertEquals(status_count, 0)
            os.unlink(os.path.join(os.getcwd(), 'test.db'))

    def test_missing_path_argument(self):
        with self.runner.isolated_filesystem():
            result = self.runner.invoke(create_db, ['--drop'])

            self.assertNotEquals(result.exit_code, 0)
            self.assertTrue('Missing argument' in result.output)

test_pipeline = pl.Pipeline(
    'test', 'Test',
    log_status=False
).connect(TestConnector, None) \
    .extract(TestExtractor) \
    .schema(pl.BaseSchema) \
    .load(TestLoader)

not_working = pl.Pipeline('nope', 'It does not work')


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

    def test_run_job_error_in_pipeline(self):
        result = self.runner.invoke(run_job, ['test.unit.test_scripts:not_working'])
        self.assertNotEquals(result.exit_code, 0)
        self.assertTrue('Something went wrong in the pipeline' in result.output)

    def test_run_job_custom_settings(self):
        result = self.runner.invoke(
            run_job, [
                'test.unit.test_scripts:test_pipeline',
            ]
        )
        self.assertEquals(result.exit_code, 0)
