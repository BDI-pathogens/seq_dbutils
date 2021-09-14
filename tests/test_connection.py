import logging
from unittest import TestCase

import seq_dbutils
from mock import patch, Mock

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


class ConnectionTestClass(TestCase):

    @patch('logging.info')
    @patch('sqlalchemy.create_engine')
    def test_create_sql_engine_ok(self, mock_create, mock_info):
        user = 'me'
        pwd = 'password'
        host = 'myhost'
        db = 'mydb'
        connection = seq_dbutils.Connection(user, pwd, host, db)
        connection.create_sql_engine()
        mock_info.assert_called_with(f'Connecting to {db} on host {host}')
        mock_create.assert_called_once()

    @patch('sys.exit')
    @patch('logging.info')
    @patch('logging.error')
    @patch('sqlalchemy.create_engine')
    def test_create_sql_engine_fail(self, mock_create, mock_error, mock_info, mock_exit):
        user = 'me'
        pwd = 'password'
        host = 'myhost'
        db = 'mydb'
        mock_create.side_effect = Mock(side_effect=Exception('Test exception'))
        connection = seq_dbutils.Connection(user, pwd, host, db)
        connection.create_sql_engine()
        mock_error.assert_called_with('Test exception')
