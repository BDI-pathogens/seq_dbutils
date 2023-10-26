import logging
from unittest import TestCase

import pandas as pd
from mock import patch, Mock
from sqlalchemy import Column, String, Float
from sqlalchemy.orm import declarative_base

from seq_dbutils import Load

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

BASE = declarative_base()


class MockLoad(Load, BASE):
    __tablename__ = 'Mock'

    mock_id = Column(String(45), primary_key=True)
    some_data = Column(Float(precision=1), nullable=True)
    mysql_engine = 'InnoDB'
    mysql_charset = 'utf8'


class LoadTestClass(TestCase):

    @staticmethod
    @patch('logging.info')
    @patch('sqlalchemy.orm.sessionmaker')
    def test_bulk_insert_df_table_empty(mock_session, mock_info):
        df = pd.DataFrame()
        mock_instance = mock_session()
        Load(df, mock_instance, MockLoad).bulk_insert_df_table()
        mock_info.assert_called_with(
            f'Skipping bulk insert for table \'{MockLoad.__tablename__}\' and empty dataframe')

    @staticmethod
    @patch('logging.info')
    @patch('sqlalchemy.orm.sessionmaker')
    def test_bulk_insert_df_table_ok(mock_session, mock_info):
        df = pd.DataFrame(data={'id1': ['a', 'b', 'c'],
                                'id2': ['d', 'b', 'f'],
                                'id3': ['g', 'h', 'i']},
                          columns=['id1', 'id2', 'id3'])
        mock_instance = mock_session()
        Load(df, mock_instance, MockLoad).bulk_insert_df_table()
        mock_info.assert_called_with(f'Bulk inserting into table \'{MockLoad.__tablename__}\'. Number of rows: 3')
        mock_instance.bulk_insert_mappings.assert_called_once()

    @staticmethod
    @patch('sys.exit')
    @patch('logging.error')
    @patch('sqlalchemy.orm.sessionmaker')
    def test_bulk_insert_df_table_fail(mock_session, mock_error, mock_exit):
        df = pd.DataFrame(data={'id1': ['a', 'b', 'c'],
                                'id2': ['d', 'b', 'f'],
                                'id3': ['g', 'h', 'i']},
                          columns=['id1', 'id2', 'id3'])
        mock_instance = mock_session()
        mock_instance.bulk_insert_mappings = Mock(side_effect=Exception('Test exception'))
        mock_instance.rollback = Mock()
        Load(df, mock_instance, MockLoad).bulk_insert_df_table()
        mock_error.assert_called_with('Test exception')
        mock_instance.rollback.assert_called_once()
        mock_exit.assert_called_once()

    @staticmethod
    @patch('logging.info')
    @patch('sqlalchemy.orm.sessionmaker')
    def test_bulk_update_df_table_empty(mock_session, mock_info):
        df = pd.DataFrame()
        mock_instance = mock_session()
        Load(df, mock_instance, MockLoad).bulk_update_df_table()
        mock_info.assert_called_with(
            f'Skipping bulk update for table \'{MockLoad.__tablename__}\' and empty dataframe')

    @staticmethod
    @patch('logging.info')
    @patch('sqlalchemy.orm.sessionmaker')
    def test_bulk_update_df_table_ok(mock_session, mock_info):
        df = pd.DataFrame(data={'id1': ['a', 'b', 'c'],
                                'id2': ['d', 'b', 'f'],
                                'id3': ['g', 'h', 'i']},
                          columns=['id1', 'id2', 'id3'])
        mock_instance = mock_session()
        Load(df, mock_instance, MockLoad).bulk_update_df_table()
        mock_info.assert_called_with(f'Bulk updating table \'{MockLoad.__tablename__}\'. Number of rows: 3')
        mock_instance.bulk_update_mappings.assert_called_once()

    @staticmethod
    @patch('sys.exit')
    @patch('logging.error')
    @patch('sqlalchemy.orm.sessionmaker')
    def test_bulk_update_df_table_fail(mock_session, mock_error, mock_exit):
        df = pd.DataFrame(data={'id1': ['a', 'b', 'c'],
                                'id2': ['d', 'b', 'f'],
                                'id3': ['g', 'h', 'i']},
                          columns=['id1', 'id2', 'id3'])
        mock_instance = mock_session()
        mock_instance.bulk_update_mappings = Mock(side_effect=Exception('Test exception'))
        mock_instance.rollback = Mock()
        Load(df, mock_instance, MockLoad).bulk_update_df_table()
        mock_error.assert_called_with('Test exception')
        mock_instance.rollback.assert_called_once()
        mock_exit.assert_called_once()
