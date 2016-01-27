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
