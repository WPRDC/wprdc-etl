import io
import os
import hashlib
import requests
import urllib
import paramiko
import ftplib

from io import TextIOWrapper

from pipeline.exceptions import HTTPConnectorError

SFTP_MAX_FILE_SIZE = 500000 #KiB

class Connector(object):
    '''Base connector class.

    Subclasses must implement ``connect``, ``checksum_contents``,
    and ``close`` methods.
    '''
    def __init__(self, *args, **kwargs):
        self.encoding = kwargs.get('encoding', 'utf-8')
        self.checksum = None

    def connect(self, target):
        '''Base connect method

        Should return an object that can be iterated through via
        the :py:func:`next` builtin method.
        '''
        raise NotImplementedError

    def checksum_contents(self, target):
        '''Should return an md5 hash of the contents of the conn object
        '''
        raise NotImplementedError

    def close(self):
        '''Teardown any open connections (like to a file, for example)
        '''
        raise NotImplementedError

class FileConnector(Connector):
    '''Base connector for file objects.
    '''
    def connect(self, target):
        '''Connect to a file

        Runs :py:func:`open` on the passed ``target``, sets the
        result on the class as ``_file``, and returns it.

        Arguments:
            target: a valid filepath

        Returns:
            A `file-object`_
        '''
        if self.encoding:
            self._file = open(target, 'r', encoding=self.encoding)
        else:
            self._file = open(target, 'rb', encoding=self.encoding)
        return self._file

    def checksum_contents(self, target, blocksize=8192):
        '''Open a file and get a md5 hash of its contents

        Arguments:
            target: a valid filepath

        Keyword Arguments:
            blocksize: the size of the block to read at a time
                in the file. Defaults to 8192.

        Returns:
            A hexidecimal representation of a file's contents.
        '''
        _file = self._file if self._file else self.connect(target)
        m = hashlib.md5()
        for chunk in iter(lambda: _file.read(blocksize, ), b''):
            if not chunk:
                break
            m.update(chunk.encode(self.encoding) if self.encoding else chunk)
        self._file.seek(0)
        return m.hexdigest()

    def close(self):
        '''Closes the connected file if it is not closed already
        '''
        if not self._file.closed:
            self._file.close()
        return

class RemoteFileConnector(FileConnector):
    '''Connector for a file located at a remote (HTTP-accessible) resource

    This class should be used to connect to a file available over
    HTTP. For example, if there is a CSV that is streamed from a
    web server, this is the correct connector to use.
    '''
    def connect(self, target):
        '''Connect to a remote target

        Arguments:
            target: Remote URL

        Returns:
            :py:class:`io.TextIOWrapper` around the opened URL.
        '''
        self._file = TextIOWrapper(urllib.request.urlopen(target), encoding=self.encoding)
        return self._file

class HTTPConnector(Connector):
    ''' Connect to remote file via HTTP
    '''
    def connect(self, target):
        response = requests.get(target)
        if response.status_code > 299:
            raise HTTPConnectorError(
                'Request could not be processed. Status Code: ' +
                str(response.status_code)
            )

        if 'application/json' in response.headers['content-type']:
            return response.json()

        return response.text

    def close(self):
        return True

class SFTPConnector(FileConnector):
    ''' Connect to remote file via SFTP
    '''
    def __init__(self, *args, **kwargs):
        super(SFTPConnector, self).__init__(*args, **kwargs)
        self.host = kwargs.get('host', None)
        self.username = kwargs.get('username', '')
        self.password = kwargs.get('password', '')
        self.port = kwargs.get('port', 22)
        self.root_dir = kwargs.get('root_dir', '').rstrip('/') + '/'
        self.conn, self.transport, self._file = None, None, None

    def connect(self, target):
        try:
            self.transport = paramiko.Transport((self.host, self.port))
            self.transport.connect(
                username=self.username, password=self.password
            )
            self.conn = paramiko.SFTPClient.from_transport(self.transport)
            size = self.conn.stat(self.root_dir + target).st_size
            if self.conn.stat(self.root_dir + target).st_size > SFTP_MAX_FILE_SIZE:
                # For large files, copy to local folder first
                # prevents re-downloading data for checksum and extraction
                self.conn.get(self.root_dir + target, os.path.basename(target))
                return super(SFTPConnector, self).connect(os.path.basename(target))
            else:
                self._file = io.BytesIO(self.conn.open(self.root_dir + target, 'r').read())

            if self.encoding:
                self._file = io.TextIOWrapper(self._file, self.encoding)

        except IOError as e:
            raise e

        return self._file

    def close(self):
        self.conn.close()
        self.transport.close()
        if not self._file.closed:
            self._file.close()


class FTPConnector(FileConnector):
    ''' Connect to remote file via SFTP
    '''
    def __init__(self, *args, **kwargs):
        super(FTPConnector, self).__init__(*args, **kwargs)
        self.host = kwargs.get('host', None)
        self.username = kwargs.get('username', '')
        self.password = kwargs.get('password', '')
        self.passive = kwargs.get('passive', False)
        self.ftp = None
        self.file_text = ''

    def connect(self, target):
        try:
            self.ftp = ftplib.FTP(self.host)
            self.ftp.login(self.username, self.password)
            self.ftp.set_pasv(self.passive)
            self.ftp.retrlines('RETR ' + target, self.add_to_file)
            if 'latin-sig' in self.encoding:
                b = self.file_text.encode('latin-1')
                self.file_text = b.decode('utf-8-sig')
            self._file = io.StringIO(self.file_text)

        except IOError as e:
            raise e

        return self._file

    def close(self):
        try:
            self.ftp.quit()
        except:
            pass
        if not self._file.closed:
            self._file.close()

    def add_to_file(self, line):
        self.file_text += line + "\n"
