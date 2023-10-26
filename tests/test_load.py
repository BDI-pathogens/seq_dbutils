from unittest import TestCase

import pandas as pd
from mock import patch, Mock
from sqlalchemy import Column, String, Float
from sqlalchemy.orm import declarative_base

from seq_dbutils import Load

BASE = declarative_base()


class MockTable(BASE):
    __tablename__ = 'Mock'

    mock_id = Column(String(45), primary_key=True)
    some_data = Column(Float(precision=1), nullable=True)
    mysql_engine = 'InnoDB'
    mysql_charset = 'utf8'


class LoadTestClass(TestCase):

    @patch('sqlalchemy.orm.sessionmaker')
    def setUp(self, mock_session):
        self.mock_instance = mock_session()
        self.df_data = pd.DataFrame(data={'id1': ['a', 'b', 'c'],
                                'id2': ['d', 'b', 'f'],
                                'id3': ['g', 'h', 'i']},
                          columns=['id1', 'id2', 'id3'])

    @patch('logging.info')
    def test_bulk_insert_df_table_empty(self, mock_info):
        df = pd.DataFrame()
        Load(df, self.mock_instance, MockTable).bulk_insert_df_table()
        mock_info.assert_called_with('Skipping bulk insert for table \'Mock\' and empty dataframe')

    def test_bulk_insert_df_table_ok(self):
        Load(self.df_data, self.mock_instance, MockTable).bulk_insert_df_table()
        self.mock_instance.bulk_insert_mappings.assert_called_once()

    @patch('sys.exit')
    def test_bulk_insert_df_table_fail(self, mock_exit):
        self.mock_instance.bulk_insert_mappings = Mock(side_effect=Exception())
        self.mock_instance.rollback = Mock()
        Load(self.df_data, self.mock_instance, MockTable).bulk_insert_df_table()
        self.mock_instance.rollback.assert_called_once()
        mock_exit.assert_called_once()

    @patch('logging.info')
    def test_bulk_update_df_table_empty(self, mock_info):
        df = pd.DataFrame()
        Load(df, self.mock_instance, MockTable).bulk_update_df_table()
        mock_info.assert_called_with('Skipping bulk update for table \'Mock\' and empty dataframe')

    def test_bulk_update_df_table_ok(self):
        Load(self.df_data, self.mock_instance, MockTable).bulk_update_df_table()
        self.mock_instance.bulk_update_mappings.assert_called_once()

    @patch('sys.exit')
    def test_bulk_update_df_table_fail(self, mock_exit):
        self.mock_instance.bulk_update_mappings = Mock(side_effect=Exception())
        self.mock_instance.rollback = Mock()
        Load(self.df_data, self.mock_instance, MockTable).bulk_update_df_table()
        self.mock_instance.rollback.assert_called_once()
        mock_exit.assert_called_once()
