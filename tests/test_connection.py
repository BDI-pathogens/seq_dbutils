import logging
from unittest import TestCase

import seq_dbutils
from mock import patch, Mock

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s:%(levelname)s:%(message)s")


class ConnectionTestClass(TestCase):

    @staticmethod
    @patch('logging.info')
    @patch('sqlalchemy.orm.sessionmaker')
    def test_commit_changes_false(mock_session, mock_info):
        mock_instance = mock_session()
        seq_dbutils.Connection.commit_changes(mock_instance, False)
        mock_info.assert_called_with('Changes NOT committed')

    @staticmethod
    @patch('logging.info')
    @patch('sqlalchemy.orm.sessionmaker')
    def test_commit_changes_true(mock_session, mock_info):
        mock_instance = mock_session()
        seq_dbutils.Connection.commit_changes(mock_instance, True)
        mock_info.assert_called_with('Changes committed')

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
