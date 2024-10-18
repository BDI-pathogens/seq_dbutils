import pandas as pd
import pytest
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


@pytest.fixture()
def instance():
    with patch('sqlalchemy.orm.sessionmaker') as mock_session:
        return mock_session()



@pytest.fixture(scope='session')
def dataframe():
    df_data = pd.DataFrame(data={'id1': ['a', 'b', 'c'],
                                 'id2': ['d', 'b', 'f'],
                                 'id3': ['g', 'h', 'i']},
                           columns=['id1', 'id2', 'id3'])
    return df_data


def test_bulk_insert_df_table_empty(instance):
    df = pd.DataFrame()
    with patch('logging.info') as mock_info:
        Load(df, instance, MockTable).bulk_insert_df_table()
        mock_info.assert_called_with('Skipping bulk insert for table \'Mock\' and empty dataframe')


def test_bulk_insert_df_table_ok(instance, dataframe):
    with patch('logging.info'):
        Load(dataframe, instance, MockTable).bulk_insert_df_table()
        instance.bulk_insert_mappings.assert_called_once()


def test_bulk_insert_df_table_fail(instance, dataframe):
    with patch('logging.info'):
        with patch('logging.error'):
            with patch('sys.exit') as mock_exit:
                instance.bulk_insert_mappings = Mock(side_effect=Exception())
                instance.rollback = Mock()
                Load(dataframe, instance, MockTable).bulk_insert_df_table()
                instance.rollback.assert_called_once()
                mock_exit.assert_called_once()


def test_bulk_update_df_table_empty(instance):
    with patch('logging.info') as mock_info:
        df = pd.DataFrame()
        Load(df, instance, MockTable).bulk_update_df_table()
        mock_info.assert_called_with('Skipping bulk update for table \'Mock\' and empty dataframe')


def test_bulk_update_df_table_ok(instance, dataframe):
    with patch('logging.info'):
        Load(dataframe, instance, MockTable).bulk_update_df_table()
        instance.bulk_update_mappings.assert_called_once()


def test_bulk_update_df_table_fail(instance, dataframe):
    with patch('logging.info'):
        with patch('logging.error'):
            with patch('sys.exit') as mock_exit:
                instance.bulk_update_mappings = Mock(side_effect=Exception())
                instance.rollback = Mock()
                Load(dataframe, instance, MockTable).bulk_update_df_table()
                instance.rollback.assert_called_once()
                mock_exit.assert_called_once()
