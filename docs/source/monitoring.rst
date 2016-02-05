Monitoring and Reporting
========================

As pipelines run, they generate status information and store it. The :py:class:`~pipeline.status.Status` object contains the specification of the exact information stored, but there are a few key fields that are important to consider for other parts of the pipeline.

Automatic monitoring requires the creation of a Sqlite database as mentioned in the :doc:`getting_started` section. However, for testing and possibly other purposes, it is possible to either disable logging (by setting the ``log_status`` flag to ``False``), or to send logging information to a different Sqlite database than the one specified in configuration (by setting ``conn`` to some other :py:class:`sqlite3.Connection` object).

Note:
    If you pass your own Sqlite connection, you are responsible for making sure that it is closed after the pipeline runs and has the ``status`` table configured with the proper schema. Using the connection created via the configuration will ensure everything is correct.

status
++++++

Status is a string that can be one of three things: ``new`` for pipelines that have just started to run, ``success`` for pipelines that have successfully completed, and ``error`` for pipelines that have unsuccessfully run. In the event that a pipeline has an ``error`` status, the Exception that was raised to break the pipeline will be attached to the string.

num_lines
+++++++++

This field holds the length of the ``data`` object that is eventually loaded into the final ``Loader`` destination. This can be helpful for tracking dataset growth over time, and possibly forecasting if different ETL pre-processing would be needed in order to keep up with the growth of the dataset.

input_checksum
++++++++++++++

One of the goals of the Pipeline is to avoid re-processing the same input data twice. In order to do this, a checksum of a file's contents is created when the pipeline loads. This checksum is an md5 hash of the file's contents, read in 8192-byte chunks (see :py:meth:`~pipeline.connectors.FileConnector.checksum_contents` for an example).

When a given pipeline is run again, it checks against the status table to see if a pipeline with the same name has an identical checksum. If it does, it raises a custom ``DuplicateFileException`` and halts.

Note:
    ``DuplicateFileException`` is thrown before a new Status object is created to represent a new pipeline. If you are seeing long gaps where you think new pipelines should be running, make sure that your source data is being updated properly.
