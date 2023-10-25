import logging
from unittest import TestCase

from mock import patch
from mock_alchemy.mocking import AlchemyMagicMock

from seq_dbutils import Database

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


class DatabaseTestClass(TestCase):

    @staticmethod
    @patch('logging.info')
    def test_log_and_execute_sql(mock_info):
        sql = 'SELECT * FROM test;'
        mock_session = AlchemyMagicMock()
        Database(mock_session).log_and_execute_sql(sql)
        mock_info.assert_called_with(sql)
        mock_session.execute.assert_called_with(sql)
        mock_session.execute.assert_called_once()

    @staticmethod
    @patch('logging.info')
    def test_commit_changes_false(mock_info):
        mock_instance = AlchemyMagicMock()
        Database(mock_instance).commit_changes(False)
        mock_info.assert_called_with('Changes NOT committed')
        mock_instance.commit.assert_not_called()

    @staticmethod
    @patch('logging.info')
    def test_commit_changes_true(mock_info):
        mock_instance = AlchemyMagicMock()
        Database(mock_instance).commit_changes(True)
        mock_info.assert_called_with('Changes committed')
        mock_instance.commit.assert_called_once()
