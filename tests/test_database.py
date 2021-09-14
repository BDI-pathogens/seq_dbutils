import logging
from unittest import TestCase

import seq_dbutils
from mock import patch, Mock

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


class DatabaseTestClass(TestCase):

    @staticmethod
    @patch('logging.info')
    @patch('sqlalchemy.orm.sessionmaker')
    def test_commit_changes_false(mock_session, mock_info):
        mock_instance = mock_session()
        seq_dbutils.Database.commit_changes(mock_instance, False)
        mock_info.assert_called_with('Changes NOT committed')

    @staticmethod
    @patch('logging.info')
    @patch('sqlalchemy.orm.sessionmaker')
    def test_commit_changes_true(mock_session, mock_info):
        mock_instance = mock_session()
        seq_dbutils.Database.commit_changes(mock_instance, True)
        mock_info.assert_called_with('Changes committed')
