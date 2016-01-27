import requests
import urllib

from io import TextIOWrapper

from pipeline.exceptions import HTTPConnectorError

class Connector(object):
    def __init__(self, *args, **kwargs):
        self.encoding = kwargs.get('encoding', 'utf-8')

    def connect(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

class LocalFileConnector(Connector):
    def connect(self, target):
        self.__file = open(target, 'r', encoding=self.encoding)
        return self.__file

    def close(self):
        if not self.__file.closed:
            self.__file.close()
        return

class RemoteFileConnector(Connector):
    def connect(self, target):
        self.__file = TextIOWrapper(urllib.request.urlopen(target))
        return self.__file

    def close(self):
        if not self.__file.closed:
            self.__file.close()
        return

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
