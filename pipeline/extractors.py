import csv
from pipeline.exceptions import IsHeaderException

class Extractor(object):
    def __init__(self, target, *args, **kwargs):
        '''Extractor base class

        Attributes:
            target: location of object to be extracted from
        '''
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
    '''Base class for file-based extraction
    '''
    def extract(self):
        '''Open the file object

        Returns:
            f: a ``file`` object

        Raises:
            IOError: When there are problems opening the file
        '''
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
        '''FileExtractor subclass for csv or character-delimited files
        '''
        super(CSVExtractor, self).__init__(target)
        self.firstline_headers = kwargs.get('firstline_headers', True)
        self.headers = kwargs.get('headers', None)
        self.delimiter = kwargs.get('delimiter', ',')

        self.set_headers()

    def extract(self):
        '''Extract method for csv file

        Because we are using the csv module, we need to have access
        to the file itself on the class, so we store it here.

        Returns:
            csv.reader object
        '''
        f = open(self.target, 'r')
        self.__file = f
        reader = csv.reader(f, delimiter=self.delimiter)
        return reader

    def cleanup(self, f):
        '''Cleanup method

        Closes the file that was opened and set in the ``extract`` method
        '''
        self.__file.close()
        return

    def handle_line(self, line):
        '''Handle a file
        '''
        if line == self.headers:
            raise IsHeaderException('Headers found in data!')
        return dict(zip(self.schema_headers, line))

    def create_schema_headers(self, headers):
        '''Maps headers to schema headers

        Arguments:
            headers: A list of headers

        Returns:
            a list of formatted schema headers. By default,
                the passed headers are lowercased and have their
                spaces replaced with underscores
        '''
        return [
            i.lower().replace(' ', '_') for i in
            headers
        ]

    def set_headers(self, headers=None):
        '''Sets headers from file or passed headers

        This method sets two attributes on the class: the headers
        attribute and the schema_headers attribute. schema_headers
        must align with the schema attributes for the pipeline.

        If no headers are passed, then we check the first line of the file.
        The headers attribute is set from the first line, and schema
        headers are by default created by lowercasing and replacing
        spaces with underscores. Custom header -> schema header mappings can
        be created by subclassing the CSVExtractor and overwriting the
        create_schema_headers method.

        Keyword Arguments:
            headers: Optional headers that can be passed to be used
                as the headers and schema headers

        Raises:
            RuntimeError: if self.headers is not passed and the
            firstline_headers kwarg is not set.
        '''
        if headers:
            self.headers = headers
            self.schema_headers = self.headers
            return
        elif self.firstline_headers:
            with open(self.target) as f:
                reader = csv.reader(f, delimiter=self.delimiter)
                self.headers = next(reader)
                self.schema_headers = self.create_schema_headers(self.headers)
                return
        else:
            raise RuntimeError('No headers were passed or detected.')

class SFTPExtractor(Extractor):
    pass
