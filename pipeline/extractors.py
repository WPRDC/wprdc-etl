import csv
import datetime
import io
from collections import OrderedDict
from pipeline.exceptions import IsHeaderException
from xlrd import open_workbook, xldate_as_tuple, XL_CELL_DATE


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
    '''Abstract Extractor subclass for extracting data in a tabular format
    '''

    def __init__(self, connection, *args, **kwargs):
        super(TableExtractor, self).__init__(connection)
        self.headers = kwargs.get('headers', None)
        self.delimiter = kwargs.get('delimiter', ',')
        self.firstline_headers = kwargs.get('firstline_headers', True)

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
            reader = self.process_connection()
            self.headers = next(reader)
            self.schema_headers = self.create_schema_headers(self.headers)
        else:
            raise RuntimeError('No headers were passed or detected.')

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
            i.lower().rstrip().replace(' ', '_').replace('-', '_') for i in
            headers
            ]

    def handle_line(self, line):
        '''Replace empty strings with None types.
        '''
        if line == self.headers:
            raise IsHeaderException
        return OrderedDict(zip(self.schema_headers, [i if i != '' else None for i in line]))


class CSVExtractor(TableExtractor):
    def __init__(self, connection, *args, **kwargs):
        '''TableExtractor subclass for csv or character-delimited files
        '''
        super(CSVExtractor, self).__init__(connection, *args, **kwargs)
        self.delimiter = kwargs.get('delimiter', ',')
        self.set_headers()

    def process_connection(self):
        reader = csv.reader(self.connection, delimiter=self.delimiter)
        return reader


class ExcelExtractor(TableExtractor):
    '''TableExtractor subclass for Microsft Excel spreadsheet files (xls, xlsx)
    '''

    def __init__(self, connection, *args, **kwargs):
        super(ExcelExtractor, self).__init__(connection, *args, **kwargs)
        self.firstline_headers = kwargs.get('firstline_headers', True)
        self.sheet_index = kwargs.get('sheet_index', 0)
        self.datemode = None
        self.set_headers()

    def process_connection(self):
        data = []
        self.connection.seek(0)
        contents = self.connection.read()
        workbook = open_workbook(file_contents=contents)
        sheet = workbook.sheet_by_index(self.sheet_index)
        self.datemode = workbook.datemode
        for i in range(sheet.nrows):
            data.append(self._read_line(sheet, i))

        return iter(data)

    def _read_line(self, sheet, row):
        '''Helper function to read line from Excel files and handle representations of
            different data types
        '''
        line = []
        for col in range(sheet.ncols):
            cell = sheet.cell(row, col)
            if cell.ctype == XL_CELL_DATE:
                xldate_tuple = xldate_as_tuple(cell.value, self.datemode)
                if xldate_tuple[0]:
                    date = datetime.datetime(*xldate_as_tuple(cell.value, self.datemode))
                    dt = date.strftime('%m/%d/%Y')  # todo: return datetime and handle the formatting elsewhere
                else:
                    time = datetime.time(*xldate_tuple[3:])
                    dt = time.strftime('%H:%M:%S')
                line.append(dt)
            else:
                line.append(cell.value)
        return line
