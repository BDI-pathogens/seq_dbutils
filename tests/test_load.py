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


@pytest.fixture(scope='session')
@patch('sqlalchemy.orm.sessionmaker')
def session_fixture(mock_session):
    return mock_session()


@pytest.fixture(scope='session')
def dataframe_fixture():
    df_data = pd.DataFrame(data={'id1': ['a', 'b', 'c'],
                                 'id2': ['d', 'b', 'f'],
                                 'id3': ['g', 'h', 'i']},
                           columns=['id1', 'id2', 'id3'])
    return df_data


def test_bulk_insert_df_table_empty(session_fixture):
    df = pd.DataFrame()
    with patch('logging.info') as mock_info:
        Load(df, session_fixture, MockTable).bulk_insert_df_table()
        mock_info.assert_called_with('Skipping bulk insert for table \'Mock\' and empty dataframe')


def test_bulk_insert_df_table_ok(session_fixture, dataframe_fixture):
    with patch('logging.info'):
        Load(dataframe_fixture, session_fixture, MockTable).bulk_insert_df_table()
        session_fixture.bulk_insert_mappings.assert_called_once()


def test_bulk_insert_df_table_fail(session_fixture, dataframe_fixture):
    with patch('logging.info'):
        with patch('logging.error'):
            with patch('sys.exit') as mock_exit:
                session_fixture.bulk_insert_mappings = Mock(side_effect=Exception())
                session_fixture.rollback = Mock()
                Load(dataframe_fixture, session_fixture, MockTable).bulk_insert_df_table()
                session_fixture.rollback.assert_called_once()
                mock_exit.assert_called_once()


def test_bulk_update_df_table_empty(session_fixture):
    with patch('logging.info') as mock_info:
        df = pd.DataFrame()
        Load(df, session_fixture, MockTable).bulk_update_df_table()
        mock_info.assert_called_with('Skipping bulk update for table \'Mock\' and empty dataframe')


def test_bulk_update_df_table_ok(session_fixture, dataframe_fixture):
    with patch('logging.info'):
        Load(dataframe_fixture, session_fixture, MockTable).bulk_update_df_table()
        session_fixture.bulk_update_mappings.assert_called_once()


def test_bulk_update_df_table_fail(session_fixture, dataframe_fixture):
    with patch('logging.info'):
        with patch('logging.error'):
            with patch('sys.exit') as mock_exit:
                session_fixture.bulk_update_mappings = Mock(side_effect=Exception())
                session_fixture.rollback = Mock()
                Load(dataframe_fixture, session_fixture, MockTable).bulk_update_df_table()
                session_fixture.rollback.assert_called_once()
                mock_exit.assert_called_once()
