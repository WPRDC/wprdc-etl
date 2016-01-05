from .exceptions import IsHeaderException

class Extractor(object):
    def __init__(self, target, *args, **kwargs):
        self.target = target

    def extract(self):
        '''Return a generator. Must be implemented in subclasses
        '''
        raise NotImplementedError

    def handle_line(self, line):
        '''Handle each line or row in a particular file.
        '''
        raise NotImplementedError

    def cleanup(self, *args, **kwargs):
        '''Perform whatever cleanup is necessary to the extractor.
        '''
        raise NotImplementedError

    def set_headers(self):
        '''Sets the column names or headers. Required for serialization
        '''
        raise NotImplementedError

class FileExtractor(Extractor):
    def extract(self):
        try:
            f = open(self.target, 'r')
            return f
        except IOError as e:
            raise e

    def cleanup(self, f):
        '''Closes a file object if it isn't closed already
        '''
        f.close()
        return

class CSVExtractor(FileExtractor):
    def __init__(self, target, *args, **kwargs):
        super(CSVExtractor, self).__init__(target)
        self.firstline_headers = kwargs.get('firstline_headers', True)
        self.headers = kwargs.get('headers', None)

        self.set_headers()

    def handle_line(self, line):
        parsed = line.strip('\n').strip('\r\n').split(',')
        if parsed == self.headers:
            raise IsHeaderException('Headers found in data!')
        return dict(zip(self.schema_headers, parsed))

    def set_headers(self, headers=None):
        if headers:
            self.headers = headers
            self.schema_headers = self.headers
            return
        elif self.firstline_headers:
            with open(self.target) as f:
                self.headers = f.readline().strip('\n').strip('\r\n').split(',')
                self.schema_headers = [
                    i.lower().replace(' ', '_') for i in
                    self.headers
                ]
                return
        else:
            raise RuntimeError('No headers were passed or detected.')

class SFTPExtractor(Extractor):
    pass
