import requests
import urllib
from pipeline.exceptions import HTTPConnectorError



class Connector(object):
    def connect(self):
        raise NotImplementedError
    def close(self):
        raise NotImplementedError

class LocalFileConnector(Connector):
    def __init__(self, *args, **kwargs):
        self.encoding = kwargs.get('encoding', 'utf-8')

    def connect(self, target):
        try:
            self.__file = open(target, 'r', encoding=self.encoding)
            return self.__file
        except IOError as e:
            raise e

    def close(self):
        if not self.__file.closed:
            self.__file.close()
        return


class RemoteFileConnector(Connector):
    def __init__(self, *args, **kwargs):
        self.encoding = kwargs.get('encoding','utf-8')

    def connect(self, target):
        try:
            self.__file = urllib.request.urlopen(target)
            import pdb; pdb.set_trace()
        except Exception as e:
            raise e

        return self.__file

    def close(self):
        if not self.__file.closed:
            self.__file.close()
        return


class HTTPConnector(Connector):
    ''' Connect to remote file via HTTP
    '''
    def __init__(self, *args, **kwargs):
        self.encoding = kwargs.get('encoding', 'utf-8')

    def connect(self, target):
        try:
            response = requests.get(target)
        except Exception as e:
            raise

        if response.status_code > 299:
            raise HTTPConnectorError('Request could not be processed. Status Code: ' +str(response.status_code))

        if 'application/json' in response.headers['content-type']:
            return response.json()

        return response.text

    def close(self):
        pass


class SFTPConnector(Connector):
    ''' Connect to remote file via SFTP
    '''
    def __init__(self, *args, **kwargs):
        self.encoding = kwargs.get('encoding', 'utf-8')
        self.host = kwargs.get('host', None)
        self.username = kwargs.get('username', '')
        self.password = kwargs.get('password', '')
        self.port = kwargs.get('port', 22)
        self.dir = kwargs.get('dir','')
        self.conn = None





