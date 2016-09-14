import os
import io
import unittest

from io import TextIOBase, TextIOWrapper, StringIO

import pipeline as pl
from pipeline.connectors import Connector

from unittest.mock import patch, PropertyMock, Mock

HERE = os.path.abspath(os.path.dirname(__file__))

class TestConnector(unittest.TestCase):
    def test_encoding(self):
        self.assertEquals(Connector('').encoding, 'utf-8')

class TestCSVConnector(unittest.TestCase):
    def setUp(self):
        self.path = os.path.join(HERE, '../mock/simple_mock.csv')
        self.connector = pl.FileConnector('')

    def test_connect(self):
        f = self.connector.connect(self.path)
        self.assertIsInstance(f, TextIOBase)
        self.assertFalse(f.closed)

    def test_close(self):
        f = self.connector.connect(self.path)
        self.assertFalse(f.closed)
        self.connector.close()
        self.assertTrue(f.closed)

class TestRemoteFileConnector(unittest.TestCase):
    def setUp(self):
        self.connector = pl.RemoteFileConnector('')

    @patch('urllib.request.urlopen', return_value=StringIO())
    def test_remote_connection(self, urlopen):
        fileobj = self.connector.connect('')
        self.assertIsInstance(fileobj, TextIOWrapper)
        self.assertFalse(fileobj.closed)

    @patch('urllib.request.urlopen', return_value=StringIO())
    def test_remote_close(self, urlopen):
        fileobj = self.connector.connect('')
        self.assertFalse(fileobj.closed)
        self.connector.close()
        self.assertTrue(fileobj.closed)

class TestHTTPConnector(unittest.TestCase):
    def setUp(self):
        self.connector = pl.HTTPConnector('')

    @patch('requests.get')
    def test_error_when_bad_status_code(self, get):
        type(get.return_value).status_code = PropertyMock(return_value=500)
        with self.assertRaises(pl.HTTPConnectorError):
            self.connector.connect(None)

    @patch('requests.get')
    def test_returns_json_when_json_content_type(self, get):
        get.return_value = Mock(json=lambda: {"json": True})
        type(get.return_value).headers = PropertyMock(return_value={'content-type': 'application/json'})
        type(get.return_value).status_code = PropertyMock(return_value=200)
        self.assertEquals(
            self.connector.connect(None),
            {"json": True}
        )

    @patch('requests.get')
    def test_returns_text(self, get):
        get.return_value = Mock(text='woohoo!')
        type(get.return_value).status_code = PropertyMock(return_value=200)
        type(get.return_value).headers = PropertyMock(return_value={'content-type': 'text'})
        self.assertEquals(
            self.connector.connect(None),
            'woohoo!'
        )

    def test_http_connector_close(self):
        self.assertTrue(self.connector.close())

class TestSFTPConnector(unittest.TestCase):
    def setUp(self):
        self.connector = pl.SFTPConnector(**{
            'username': 'hello',
            'password': 'world',
            'host': 'afunhost@localhost'
        })

    def test_successful_init(self):
        self.assertEquals(self.connector.encoding, 'utf-8')
        self.assertEquals(self.connector.host, 'afunhost@localhost')
        self.assertEquals(self.connector.username, 'hello')
        self.assertEquals(self.connector.password, 'world')
        self.assertEquals(self.connector.port, 22)
        self.assertEquals(self.connector.root_dir, '/')
        self.assertEquals(self.connector.conn, None)

    @patch('pipeline.connectors.paramiko.SFTPClient')
    @patch('pipeline.connectors.paramiko.Transport')
    @patch('pipeline.connectors.paramiko.SFTPFile')
    def test_connector(self, SFTPFile, Transport, SFTPClient):
        self.assertTrue(self.connector.conn is None)
        self.assertTrue(self.connector.transport is None)
        self.assertTrue(self.connector._file is None)

        SFTPFile.open.return_value = io.BytesIO(initial_bytes=None)
        SFTPClient.from_transport.return_value = SFTPFile
        SFTPClient.from_transport().stat.return_value = Mock(st_size=5)
        self.connector.connect('myfile.txt')

        self.assertTrue(self.connector.transport is not None)
        self.assertTrue(Transport.called)
        Transport.assert_called_with(('afunhost@localhost', 22))

        self.assertTrue(self.connector.conn is not None)
        self.assertTrue(SFTPClient.from_transport.called)
        SFTPClient.from_transport.assert_called_with(self.connector.transport)

        self.assertTrue(self.connector._file is not None)
        self.assertTrue(SFTPClient.from_transport().open.called)
        SFTPClient.from_transport().open.assert_called_with(
            '/myfile.txt', 'r'
        )

    @patch('pipeline.connectors.paramiko.SFTPClient')
    @patch('pipeline.connectors.paramiko.Transport')
    def test_bad_sftp(self, Transport, SFTPClient):
        SFTPClient.from_transport().open.side_effect = IOError()
        SFTPClient.from_transport().stat.return_value = Mock(st_size=5)
        with self.assertRaises(IOError):
            self.connector.connect('')

    @patch('pipeline.connectors.paramiko.SFTPClient')
    @patch('pipeline.connectors.paramiko.Transport')
    @patch('pipeline.connectors.paramiko.SFTPFile')
    def test_connector_closes_connections(self, SFTPFile, Transport, SFTPClient):
        SFTPFile.open.return_value = io.BytesIO(initial_bytes=None)
        SFTPClient.from_transport.return_value = SFTPFile
        SFTPClient.from_transport().stat.return_value = Mock(st_size=5)
        self.connector.connect('myfile.txt')
        self.connector._file = StringIO()
        self.assertFalse(self.connector._file.closed)
        self.connector.close()
        self.assertTrue(self.connector.conn.close.called)
        self.assertTrue(self.connector.transport.close.called)
        self.assertTrue(self.connector._file.closed)
