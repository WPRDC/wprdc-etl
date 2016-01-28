import os
import json
import sqlite3
import time

from pipeline.exceptions import IsHeaderException, InvalidConfigException
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
        self, name, display_name, server='staging',
        settings_file=None, log_status=True, conn=None
    ):
        '''
        Arguments:
            name: pipeline's name, passed to
                :py:class:`~purchasing.status.Status`
            display_name: display name, passed to
                :py:class:`~purchasing.status.Status`

        Keyword Arguments:
            server: name of the server to use in the configuration,
                defaults to "staging"
            settings_file: filepath to the configuration file
            log_status: boolean for whether or not to log the status
                of the pipeline. useful to turn off for testing
            conn: optionally passed sqlite3 connection object. if no
                connection is passed, one will be instantiated when the
                pipeline's ``run`` method is called
        '''
        self.data = []
        self._connector, self._extractor, self._schema, self._loader = \
            None, None, None, None
        self.name = name
        self.display_name = display_name

        settings_file = settings_file if settings_file else \
            os.path.join(PARENT, 'settings.json')
        self.set_config_from_file(server, settings_file)
        self.log_status = log_status

        if conn:
            self.conn = conn
            self.passed_conn = True
        else:
            self.passed_conn = False

    def get_config(self):
        return self.config

    def set_config_from_file(self, server, file):
        '''Sets the pipeline's configuration from file

        Arguments:
            server: String that represents
            file: Location of the configuration to load

        Raises:
            InvalidConfigException: if configuration is found or
            the found configuration is not valid json
        '''
        try:
            with open(file) as f:
                raw_config = json.loads(f.read())
                self.config = raw_config[server]
        except (KeyError, IOError, FileNotFoundError):
            raise InvalidConfigException(
                'No config file found, or config not properly formatted'
            )

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
            self.data.append(loaded.data)

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
                'You must specify connet, extract, schema, and load steps!'
            )

    def pre_run(self):
        '''Method to be run immediately before the pipeline runs

        Establishes a connection to the monitoring database on the
        pipeline and builds the initial :py:class:`~pipeline.status.Status`
        object.
        '''
        start_time = time.time()

        if not self.passed_conn:
            self.conn = sqlite3.Connection(self.config['statusdb'])

        if self.log_status:
            self.status = Status(
                self.conn, self.name, self.display_name, None,
                start_time, 'new', None, None,
            )
            self.status.write()

    def run(self):
        '''Main pipeline run method

        One of the main features is that the extractor, schema, and
        loader are all instantiated here as opposed to when they
        are declared on pipeline instantiation. This delays opening
        connections until the last possible moment.

        The run method works essentially as follow:

        1. Ensure that we have all pieces of the pipeline
        2. Instantiate the extractor, passing it whatever args
           and kwargs needed
        3. Run the extract method, which should return an iterable
           of different data methods
        4. Instantiate our schema and attach it to the pipeline
        5. Iterate through the extracted raw data, using the extractor's
           ``handle_line`` to extract the data from each line, and
           then run the ``load_line`` method to attach each row to the
           pipeline's ``data`` attribute. At the end of the iteration,
           call the extractor's ``cleanup`` attribute.
        6. Instantiate the loader and then load the data
        7. Finally, at the end of the run, run a final log, and run the
           close method to shut down the pipeline
        '''
        self.pre_run()

        try:
            self.enforce_full_pipeline()
            # instantiate a new connection based on the
            # passed connector class
            _connector = self._connector(
                *(self.connector_args), **(self.connector_kwargs)
            )
            connection = _connector.connect(self.target)

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
            self._loader(self.config, *(self.loader_args), **(self.loader_kwargs)).load(self.data)
            if self.log_status:
                self.status.update(status='success')
        except Exception as e:
            if self.log_status:
                self.status.update(status='error: {}'.format(str(e)))
            raise
        finally:
            if self.log_status:
                self.status.update(
                    num_lines=len(self.data),
                    last_ran=time.time()
                )
            self.close()
        return self

    def close(self):
        '''Close any open database connections.
        '''
        if not self.passed_conn:
            self.conn.close()
        self.__schema = None
