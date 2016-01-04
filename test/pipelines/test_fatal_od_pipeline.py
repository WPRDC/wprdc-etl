import os
from jobs.fatal_od import fatal_od_pipeline

from unittest import TestCase

HERE = os.path.abspath(os.path.dirname(__file__))

class TestODPipeline(TestCase):
    def test_od_pipeline(self):
        fatal_od_pipeline.run(os.path.join(HERE, 'fatal_od_mock.csv'))
