class Pipeline(object):
    def __init__(self):
        self.data = []

    def extract(self, extractor, *args, **kwargs):
        self.extractor = extractor
        self.extractor_args = list(*args)
        self.extractor_kwargs = dict(**kwargs)
        return self

    def schema(self, schema):
        self.schema = schema
        return self

    def load(self, loader):
        self.loader = loader
        return self

    def load_line(self, data):
        '''Load a line into the pipeline's data or throw an error
        '''
        loaded = self.schema().load(data)
        if loaded.errors:
            raise RuntimeError('There were errors in the input data: {}'.format(
                loaded.errors.__str__()
            ))
        else:
            self.data.append(loaded.data)

    def _run(self):
        # instantiate a new extrator instance based on the passed extract class
        _extractor = self.extractor(
            self.target, *(self.extractor_args), **(self.extractor_kwargs)
        )
        # instantiate a new schema instance based on the passed schema class
        raw = self.extractor(self.target).extract()

        # build the data
        try:
            for line in raw:
                data = _extractor.handle_line(line)
                if data:
                    self.load_line(data)
                else:
                    continue
        finally:
            _extractor.cleanup(raw)

        # load the data
        self.loader().load(self.data)

    def schedule(self, extract_target):
        self.target = extract_target
        return self

    def run(self, extract_target):
        self.target = extract_target
        self._run()
        return self
