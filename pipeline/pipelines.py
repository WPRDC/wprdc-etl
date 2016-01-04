import os
import json

from .exceptions import IsHeaderException, InvalidConfigException

HERE = os.path.abspath(os.path.dirname(__file__))
PARENT = os.path.join(HERE, '..')

class Pipeline(object):
    def __init__(self, server="staging", settings_file=None):
        self.data = []
        self._extractor, self._schema, self._loader = None, None, None
        settings_file = settings_file if settings_file else \
            os.path.join(PARENT, 'settings.json')
        self.set_config_from_file(server, settings_file)

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

    def extract(self, extractor, *args, **kwargs):
        self._extractor = extractor
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
            raise RuntimeError('There were errors in the input data: {}'.format(
                loaded.errors.__str__()
            ))
        else:
            self.data.append(loaded.data)

    def enforce_full_pipeline(self):
        if not all([self._extractor, self._schema, self._loader]):
            raise RuntimeError('You must specify extract, schema, and load steps!')

    def _run(self):
        self.enforce_full_pipeline()
        # instantiate a new extrator instance based on the passed extract class
        _extractor = self._extractor(
            self.target, *(self.extractor_args), **(self.extractor_kwargs)
        )
        # instantiate a new schema instance based on the passed schema class
        raw = self._extractor(self.target).extract()

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

    def schedule(self, extract_target):
        self.target = extract_target
        return self

    def run(self, extract_target):
        self.target = extract_target
        self._run()
        return self
