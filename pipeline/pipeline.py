import os
import json
import sqlite3
import time

from pipeline.exceptions import (
    IsHeaderException, DuplicateFileException, MissingDatabaseName
)
from pipeline.status import Status


HERE = os.path.abspath(os.path.dirname(__file__))
PARENT = os.path.join(HERE, '..')


class Pipeline(object):
    '''Main pipeline class

    The pipeline class binds together extractors, schema,
    and loaders and runs everything together. Almost all
    Pipeline methods return the pipeline object, allowing
    for methods to be chained together.
    '''

    def __init__(
        self, name, display_name, log_status=False,
        conn=None, conn_name=None
    ):
        '''
        Arguments:
            name: pipeline's name, passed to
                :py:class:`~purchasing.status.Status`
            display_name: display name, passed to
                :py:class:`~purchasing.status.Status`

        Keyword Arguments:
            settings_file: filepath to the configuration file
            log_status: boolean for whether or not to log the status
                of the pipeline. useful to turn off for testing
            conn: optionally passed sqlite3 connection object. if no
                connection is passed
        '''
        self.data = []
        self._connector, self._extractor, self._schema, self._loader = \
            None, None, None, None
        self.name = name
        self.display_name = display_name

        self.log_status = log_status
        self.conn_name = conn_name

        if conn:
            self.conn = conn
            self.passed_conn = True
        else:
            self.passed_conn = False


    def connect(self, connector, target, *args, **kwargs):
        self._connector = connector
        self.target = target
        self.connector_args = list(args)
        self.connector_kwargs = dict(**kwargs)
        return self

    def extract(self, extractor, *args, **kwargs):
        '''Set the extractor class and related arguments

        Arguments:
            extractor: Extractor class, see :ref:`built-in-extractors`
            target: location of the extraction target (file, url, etc.)
        '''
        self._extractor = extractor
        self.extractor_args = list(args)
        self.extractor_kwargs = dict(**kwargs)
        return self

    def schema(self, schema):
        '''Set the schema class

        Arguments:
            schema: Schema class

        Returns:
            modified Pipeline object
        '''
        self._schema = schema
        return self

    def load(self, loader, *args, **kwargs):
        '''Sets the loader class

        Arguments:
            loader: Loader class. See :ref:`built-in-loaders`

        Returns:
            modified Pipeline object
        '''
        self._loader = loader
        self.loader_args = list(args)
        self.loader_kwargs = dict(**kwargs)
        return self

    def load_line(self, data):
        '''Load a line into the pipeline's data or throw an error

        Arguments:
            data: A parsed line from an extractor's handle_line
                method
        '''
        loaded = self.__schema.load(data)
        if loaded.errors:
            raise RuntimeError('There were errors in the input data: {} (passed data: {})'.format(
                loaded.errors.__str__(), data
            ))
        else:
            self.data.append(self.__schema.dump(loaded.data).data)

    def enforce_full_pipeline(self):
        '''Ensure that a pipeline has an extractor, schema, and loader

        Raises:
            RuntimeError: if an extractor, schema, and loader are not
                all specified
        '''
        if not all([
            self._connector, self._extractor,
            self._schema, self._loader,
        ]):
            raise RuntimeError(
                'You must specify connect, extract, schema, and load steps!'
            )

    def get_last_run_checksum(self):
        if self.log_status:
            result = self.conn.execute('''
		        SELECT input_checksum, max(last_ran)
                FROM status
                WHERE name = ?
                AND display_name = ?
                GROUP BY input_checksum
            ''', (self.name, self.display_name)).fetchone()
            if result:
                return result[0]
        return None

    def pre_run(self):
        '''Method to be run immediately before the pipeline runs

        Enforces that a pipeline is complete and, connects to the statusdb

        Returns:
            A unix timestamp of the pipeline's start time.
        '''
        start_time = time.time()

        self.enforce_full_pipeline()

        if self.log_status and not self.passed_conn:
            if self.conn_name:
                self.conn = sqlite3.Connection(self.conn_name)
            else:
                raise MissingDatabaseName

        return start_time

    def run(self):
        '''Main pipeline run method

        One of the main features is that the connector, extractor,
        schema, and loader are all instantiated here as opposed to
        when they are declared on pipeline instantiation. This delays
        opening connections until the last possible moment.

        The run method works essentially as follow:

        1. Run the ``pre_run`` method, which gives us the pipeline
           start time, ensures that our pipeline has all of the
           required component pieces, and connects to the status db.
        2. Boot up a new connection object, and get the checksum
           of the connected iterable.
        3. Check to make sure that the incoming checksum is different
           from the previous run's input_checksum
        4. Instantiate our schema
        5. Iterate through the iterable returned from the connector's
           connect method, handling each element with the extractor's
           ``handle_line`` method before passing it to the the
           ``load_line`` method to attach each row to the pipeline's
           data.
        6. After iteration, clean up the connector
        7. Instantiate the loader and load the data
        8. Finally, update the status to successful run and close
           down and clean up the pipeline.
        '''
        try:
            start_time = self.pre_run()

            # instantiate a new connection based on the
            # passed connector class
            _connector = self._connector(
                *(self.connector_args), **(self.connector_kwargs)
            )

            # connect and retreive source data
            connection = _connector.connect(self.target)

            input_checksum = _connector.checksum_contents(self.target)
            if input_checksum == self.get_last_run_checksum():
                raise DuplicateFileException

            if self.log_status:
                self.status = Status(
                    self.conn, self.name, self.display_name, None,
                    start_time, 'new', None, None, None
                )

            # log the status
            if self.log_status:
                self.status.write()

            # instantiate a new extrator instance based on
            # the passed extract class
            _extractor = self._extractor(
                connection, *(self.extractor_args), **(self.extractor_kwargs)
            )

            # instantiate our schema
            self.__schema = self._schema()

            # build the data
            raw = _extractor.process_connection()

            try:
                for line in raw:
                    try:
                        data = _extractor.handle_line(line)
                        self.load_line(data)
                    except IsHeaderException:
                        continue
            finally:
                _connector.close()

            # load the data
            _loader = self._loader(
                *(self.loader_args), **(self.loader_kwargs)
            )
            _loader.load(self.data)

            if self.log_status:
                self.status.update(status='success', input_checksum=input_checksum)

        except Exception as e:
            if self.log_status and hasattr(self, 'status'):
                self.status.update(status='error: {}'.format(str(e)))
            raise

        finally:
            if self.log_status and hasattr(self, 'status'):
                self.status.update(
                    num_lines=len(self.data),
                    last_ran=time.time()
                )
            self.close()

        return self

    def close(self):
        '''Close any open database connections.
        '''
        if not self.passed_conn and hasattr(self, 'conn'):
            self.conn.close()
        self.__schema = None