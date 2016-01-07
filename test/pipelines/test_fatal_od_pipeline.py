import os

from jobs.fatal_od import FatalODSchema
import pipeline as pl

from .base import TestBase, HERE

class TestODPipeline(TestBase):
    def test_od_pipeline(self):
        pl.Pipeline(
            'fatal_od_pipeline', 'Fatal OD Pipeline',
            server=self.default_server,
            settings_file=self.settings_file,
            conn=self.conn
        ) \
            .extract(pl.CSVExtractor, firstline_headers=True) \
            .schema(FatalODSchema) \
            .load(self.Loader) \
            .run(os.path.join(HERE, '../mock/fatal_od_mock.csv'))
        status = self.cur.execute('select * from status').fetchall()
        self.assertEquals(len(status), 1)
        self.assertEquals(status[0][-2], 'success')
        self.assertEquals(status[0][-1], 1)
