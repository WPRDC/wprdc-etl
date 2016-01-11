import os
import json
import sqlite3
import time

from pipeline.exceptions import IsHeaderException, InvalidConfigException

HERE = os.path.abspath(os.path.dirname(__file__))
PARENT = os.path.join(HERE, '..')

class Status(object):
    def __init__(
        self, conn, name, display_name, last_ran, start_time,
        status, frequency, num_lines
    ):
        self.conn = conn
        self.name = name
        self.display_name = display_name
        self.last_ran = last_ran
        self.start_time = start_time
        self.status = status
        self.num_lines = num_lines

    def update(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)
        self.write()

    def write(self):
        cur = self.conn.cursor()
        cur.execute(
            '''
            INSERT OR REPLACE INTO status (
                name, display_name, last_ran, start_time,
                status, num_lines
            ) VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                self.name, self.display_name, self.last_ran,
                self.start_time, self.status, self.num_lines
            )
        )
        self.conn.commit()

class Pipeline(object):
    def __init__(
        self, name, display_name, server="staging",
        settings_file=None, log_status=True, conn=None
    ):
        self.data = []
        self._extractor, self._schema, self._loader = None, None, None
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
        try:
            with open(file) as f:
                raw_config = json.loads(f.read())
                self.config = raw_config[server]
        except (KeyError, IOError, FileNotFoundError):
            raise InvalidConfigException(
                'No config file found, or config not properly formatted'
            )

    def extract(self, extractor, target, *args, **kwargs):
        self._extractor = extractor
        self.target = target
        self.extractor_args = list(args)
        self.extractor_kwargs = dict(**kwargs)
        return self

    def schema(self, schema):
        self._schema = schema
        return self

    def load(self, loader):
        self._loader = loader
        return self

    def load_line(self, data):
        '''Load a line into the pipeline's data or throw an error
        '''
        loaded = self._schema().load(data)
        if loaded.errors:
            raise RuntimeError('There were errors in the input data: {} (passed data: {})'.format(
                loaded.errors.__str__(), data
            ))
        else:
            self.data.append(loaded.data)

    def enforce_full_pipeline(self):
        if not all([self._extractor, self._schema, self._loader]):
            raise RuntimeError(
                'You must specify extract, schema, and load steps!'
            )

    def before_run(self):
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
        self.before_run()
        try:
            self.enforce_full_pipeline()
            # instantiate a new extrator instance based on the passed extract class
            _extractor = self._extractor(
                self.target, *(self.extractor_args), **(self.extractor_kwargs)
            )
            # instantiate a new schema instance based on the passed schema class
            raw = _extractor.extract()

            # build the data
            try:
                for line in raw:
                    try:
                        data = _extractor.handle_line(line)
                        self.load_line(data)
                    except IsHeaderException:
                        continue
            finally:
                _extractor.cleanup(raw)

            # load the data
            self._loader(self.config).load(self.data)
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
        if not self.passed_conn:
            self.conn.close()
