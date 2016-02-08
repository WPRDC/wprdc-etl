import csv
from pipeline.exceptions import IsHeaderException
import xlrd


class Extractor(object):
    def __init__(self, connection):
        self.connection = connection
        self.checksum = None
        self.data = []

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


class TableExtractor(Extractor):
    def __init__(self, connection, *args, **kwargs):
        super(TableExtractor, self).__init__(connection)
        self.headers = kwargs.get('headers', None)
        self.delimiter = kwargs.get('delimiter', ',')
        self.firstline_headers = kwargs.get('firstline_headers', True)

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

    def handle_line(self, line):
        '''Replace empty strings with None types.
        '''
        if line == self.headers:
            raise IsHeaderException
        return dict(zip(self.schema_headers, [i if i != '' else None for i in line]))

class CSVExtractor(TableExtractor):
    def __init__(self, connection, *args, **kwargs):
        '''FileExtractor subclass for csv or character-delimited files
        '''
        super(CSVExtractor, self).__init__(connection, *args, **kwargs)
        self.delimiter = kwargs.get('delimiter', ',')
        self.set_headers()

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

    def process_connection(self):
        reader = csv.reader(self.connection, delimiter=self.delimiter)
        return reader



    def extract(self):
        raw = self.process_connection()
        for line in raw:
            try:
                self.data.append(self.handle_line(line))
            except IsHeaderException:
                continue
        return self.data


class ExcelExtractor(TableExtractor):
    def __init__(self, connection, *args, **kwargs):
        super(ExcelExtractor, self).__init__(connection, *args, **kwargs)
        self.firstline_headers = kwargs.get('firstline_headers', True)
        self.sheet_index = kwargs.get('sheet_index', 0)
        self.column_count = kwargs.get('column_count', 0)
        self.sheet = None

    # TODO: very similar to CSVExtractor - see if it can be moved to TableExtractor
    def set_headers(self, headers=None):
        if headers:
            self.headers = headers
            self.schema_headers = self.headers
            return
        elif self.firstline_headers:
            self.sheet = xlrd.open_workbook(file_contents=self.connection) \
                .sheet_by_index(self.sheet_index)
            self.headers = self.read_line(0)
            self.schema_headers = self.create_schema_headers(self.headers)
        else:
            raise RuntimeError

    def process_connection(self):
        if self.sheet:
            return self.sheet
        self.sheet = xlrd.open_workbook(file_contents=self.connection). \
            sheet_by_index(self.sheet_index)

    def extract(self):
        if self.sheet is None:
            self.sheet = self.process_connection()
        for i in self.sheet.nrows():
            self.data.append(self.handle_line(self.read_line(i))) # fixme: this is ugly

    def read_line(self, row, columns=0):
        if not columns:
            columns = self.column_count
        return [self.sheet.cell(row, column).value for column in range(columns)]
