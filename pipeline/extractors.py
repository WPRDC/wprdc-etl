import csv
from pipeline.exceptions import IsHeaderException

class Extractor(object):
    def __init__(self, connection):
        self.connection = connection

    def process_connection(self):
        '''Do any additional processing on a connection to prepare it for reading
        '''
        raise NotImplementedError

    def handle_line(self, line):
        '''Handle each line or row in a particular file.
        '''
        raise NotImplementedError

    def set_headers(self):
        '''Sets the column names or headers. Required for serialization
        '''
        raise NotImplementedError

class CSVExtractor(Extractor):
    def __init__(self, connection, *args, **kwargs):
        '''FileExtractor subclass for csv or character-delimited files
        '''
        super(CSVExtractor, self).__init__(connection)
        self.firstline_headers = kwargs.get('firstline_headers', True)
        self.headers = kwargs.get('headers', None)
        self.delimiter = kwargs.get('delimiter', ',')

        self.set_headers()

    def process_connection(self):
        reader = csv.reader(self.connection, delimiter=self.delimiter)
        return reader

    def handle_line(self, line):
        '''Handle a line in a file
        '''
        if line == self.headers:
            raise IsHeaderException
        return dict(zip(self.schema_headers, [i if i != '' else None for i in line]))

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
            reader = csv.reader(self.connection, delimiter=self.delimiter)
            self.headers = next(reader)
            self.schema_headers = self.create_schema_headers(self.headers)
        else:
            raise RuntimeError('No headers were passed or detected.')
