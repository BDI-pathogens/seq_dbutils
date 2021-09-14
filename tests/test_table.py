import logging
from os.path import abspath, dirname, join
from unittest import TestCase

import pandas as pd
from mock import patch, Mock, MagicMock
from sqlalchemy import Column, String, Float
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import declarative_base

import seq_dbutils

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

THIS_DIR = dirname(abspath(__file__))
DATA_DIR = join(THIS_DIR, 'data')

BASE = declarative_base()


class MockTable(seq_dbutils.Table, BASE):
    __tablename__ = 'Mock'

    mock_id = Column(String(45), primary_key=True)
    some_data = Column(Float(precision=1), nullable=True)
    mysql_engine = 'InnoDB'
    mysql_charset = 'utf8'


class TableTestClass(TestCase):

    @staticmethod
    @patch('logging.info')
    @patch('sqlalchemy.orm.sessionmaker')
    def test_bulk_insert_df_table_empty(mock_session, mock_info):
        df = pd.DataFrame()
        mock_instance = mock_session()
        seq_dbutils.Table(df, mock_instance, MockTable).bulk_insert_df_table()
        mock_info.assert_called_with(
            f'Skipping bulk insert for table \'{MockTable.__tablename__}\' and empty dataframe')

    @staticmethod
    @patch('logging.info')
    @patch('sqlalchemy.orm.sessionmaker')
    def test_bulk_insert_df_table_ok(mock_session, mock_info):
        df = pd.DataFrame(data={'id1': ['a', 'b', 'c'],
                                'id2': ['d', 'b', 'f'],
                                'id3': ['g', 'h', 'i']},
                          columns=['id1', 'id2', 'id3'])
        mock_instance = mock_session()
        seq_dbutils.Table(df, mock_instance, MockTable).bulk_insert_df_table()
        mock_info.assert_called_with(f'Bulk inserting into table \'{MockTable.__tablename__}\'')

    @staticmethod
    @patch('sys.exit')
    @patch('logging.info')
    @patch('logging.error')
    @patch('sqlalchemy.orm.sessionmaker')
    def test_bulk_insert_df_table_fail(mock_session, mock_error, mock_info, mock_exit):
        df = pd.DataFrame(data={'id1': ['a', 'b', 'c'],
                                'id2': ['d', 'b', 'f'],
                                'id3': ['g', 'h', 'i']},
                          columns=['id1', 'id2', 'id3'])
        mock_instance = mock_session()
        mock_instance.bulk_insert_mappings = Mock(side_effect=Exception('Test exception'))
        mock_instance.rollback = Mock()
        seq_dbutils.Table(df, mock_instance, MockTable).bulk_insert_df_table()
        mock_error.assert_called_with('Test exception')
        mock_instance.rollback.assert_called_once()

    @staticmethod
    @patch('logging.info')
    @patch('sqlalchemy.orm.sessionmaker')
    def test_bulk_update_df_table_empty(mock_session, mock_info):
        df = pd.DataFrame()
        mock_instance = mock_session()
        seq_dbutils.Table(df, mock_instance, MockTable).bulk_update_df_table()
        mock_info.assert_called_with(
            f'Skipping bulk update for table \'{MockTable.__tablename__}\' and empty dataframe')

    @staticmethod
    @patch('logging.info')
    @patch('sqlalchemy.orm.sessionmaker')
    def test_bulk_update_df_table_ok(mock_session, mock_info):
        df = pd.DataFrame(data={'id1': ['a', 'b', 'c'],
                                'id2': ['d', 'b', 'f'],
                                'id3': ['g', 'h', 'i']},
                          columns=['id1', 'id2', 'id3'])
        mock_instance = mock_session()
        seq_dbutils.Table(df, mock_instance, MockTable).bulk_update_df_table()
        mock_info.assert_called_with(f'Bulk updating table \'{MockTable.__tablename__}\'')

    @staticmethod
    @patch('sys.exit')
    @patch('logging.info')
    @patch('logging.error')
    @patch('sqlalchemy.orm.sessionmaker')
    def test_bulk_update_df_table_fail(mock_session, mock_error, mock_info, mock_exit):
        df = pd.DataFrame(data={'id1': ['a', 'b', 'c'],
                                'id2': ['d', 'b', 'f'],
                                'id3': ['g', 'h', 'i']},
                          columns=['id1', 'id2', 'id3'])
        mock_instance = mock_session()
        mock_instance.bulk_update_mappings = Mock(side_effect=Exception('Test exception'))
        mock_instance.rollback = Mock()
        seq_dbutils.Table(df, mock_instance, MockTable).bulk_update_df_table()
        mock_error.assert_called_with('Test exception')
        mock_instance.rollback.assert_called_once()
