import hashlib
import requests
import urllib

from io import TextIOWrapper

from pipeline.exceptions import HTTPConnectorError

class Connector(object):
    def __init__(self, *args, **kwargs):
        self.encoding = kwargs.get('encoding', 'utf-8')
        self.checksum = None

    def connect(self):
        raise NotImplementedError

    def checksum_contents(self):
        '''Get an md5 hash of the contents of the conn object
        '''
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

class FileConnector(Connector):
    def connect(self, target):
        self._file = open(target, 'r', encoding=self.encoding)
        return self._file

    def checksum_contents(self, blocksize=8192):
        m = hashlib.md5()
        for chunk in iter(lambda: self._file.read(blocksize, ), b''):
            if not chunk:
                break
            m.update(chunk.encode(self.encoding))
        self.checksum = m.hexdigest()
        self._file.seek(0)
        return self.checksum

    def close(self):
        if not self._file.closed:
            self._file.close()
        return

class RemoteFileConnector(FileConnector):
    def connect(self, target):
        self._file = TextIOWrapper(urllib.request.urlopen(target))
        return self._file

class HTTPConnector(Connector):
    ''' Connect to remote file via HTTP
    '''
    def connect(self, target):
        response = requests.get(target)
        if response.status_code > 299:
            raise HTTPConnectorError('Request could not be processed. Status Code: ' +str(response.status_code))

        if 'application/json' in response.headers['content-type']:
            return response.json()

        return response.text

    def close(self):
        return True

class SFTPConnector(Connector):
    ''' Connect to remote file via SFTP
    '''
    def __init__(self, *args, **kwargs):
        super(SFTPConnector, self).__init__(*args, **kwargs)
        self.host = kwargs.get('host', None)
        self.username = kwargs.get('username', '')
        self.password = kwargs.get('password', '')
        self.port = kwargs.get('port', 22)
        self.dir = kwargs.get('dir','')
        self.conn = None
