import logging
from unittest import TestCase

import pandas as pd
from mock import patch, Mock

import seq_dbutils

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

    @staticmethod
    @patch('pandas.read_sql')
    @patch('sqlalchemy.engine.Engine')
    def test_get_db_table_col_list(mock_engine, mock_sql):
        seq_dbutils.Database.get_db_table_col_list(mock_engine, 'Test')
        mock_sql.assert_called_with(f'SHOW COLUMNS FROM Test;', mock_engine)

    @staticmethod
    @patch('logging.info')
    @patch('seq_dbutils.Database.get_db_table_col_list')
    @patch('sqlalchemy.engine.Engine')
    def test_create_db_table_dataframe(mock_engine, mock_get, mock_info):
        df = pd.DataFrame()
        seq_dbutils.Database.create_db_table_dataframe(df, mock_engine, 'Test')
        mock_get.assert_called_once()
        mock_info.assert_called_with("'Test' rows to load: 0")
