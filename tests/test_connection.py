import logging
from unittest import TestCase

from mock import patch, Mock

from seq_dbutils import Connection

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


class ConnectionTestClass(TestCase):

    def setUp(self):
        self.user = 'me'
        self.pwd = 'mypassword'
        self.host = 'myhost'
        self.db = 'mydb'
        self.connection = Connection(self.user, self.pwd, self.host, self.db)
        self.connector_type = 'mysqlconnector'

    @patch('logging.info')
    @patch('sqlalchemy.create_engine')
    def test_create_sql_engine_ok(self, mock_create, mock_info):
        self.connection.create_sql_engine()
        mock_info.assert_called_once()
        mock_info.assert_called_with(f'Connecting to {self.db} on host {self.host}')
        mock_create.assert_called_once()
        mock_create.assert_called_with(f'mysql+{self.connector_type}://{self.user}:{self.pwd}@{self.host}/{self.db}',
                                       echo=False)

    @patch('sys.exit')
    @patch('logging.error')
    @patch('sqlalchemy.create_engine')
    def test_create_sql_engine_fail(self, mock_create, mock_error, mock_exit):
        mock_create.side_effect = Mock(side_effect=Exception('Test exception'))
        self.connection.create_sql_engine()
        mock_error.assert_called_with('Test exception')
        mock_exit.assert_called_once()
