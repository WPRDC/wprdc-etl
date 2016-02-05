.. WPRDC ETL Pipeline documentation master file, created by
   sphinx-quickstart on Wed Jan 13 22:53:01 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

WPRDC Pipeline
==============

The WPRDC pipeline is a python library that allows users to quickly build pipelines. Schema and data validation are handled by the a custom implementation of a Marshmallow :py:class:`~marshmallow.Schema`.

.. note::
    This project is in a **pre-alpha** stage, meaning that its API can and will likely change fairly dramatically in pre-release and release versions.

Example:

.. code-block:: python

    from marshmallow import fields
    import pipeline as pl

    class MySchema(BaseSchema):
        some_field = fields.Integer()
        some_date = fields.DateTime(format='%Y-%m-%d')

    my_pipeline = pl.Pipeline('my_pipeline', 'An Example Pipeline') \
        .connect(pl.FileConnector, 'path/to/my.csv')
        .extract(pl.CSVExtractor, firstline_headers=True) \
        .schema(MySchema) \
        .load(pl.Loader)

This pipeline connects to a file located a 'path/to/my.csv', extracts data from it, validates it according to the rules of ``MySchema``, and loads it into a ``LoadTarget``. The pipeline can be kicked off by calling ``my_pipeline.run()``, or scheduled via command-line.

As the job runs, its status is automatically recorded in a local sqlite database.

To schedule a job via command-line, a built-in ``run_job`` is included. Let's say that ``my_pipeline`` was stored in a file called ``jobs.py``. It could be kicked off from the command line using the following:

.. code-block:: bash

    run_job jobs:my_pipeline

This command can be scheduled via cron to run at specific intervals.

Guide:
------

.. toctree::
   :maxdepth: 1

   getting_started
   writing_pipelines
   monitoring
   api
   changelog
