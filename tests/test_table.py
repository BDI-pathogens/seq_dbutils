import pytest
from mock import patch
from mock_alchemy.mocking import AlchemyMagicMock
from sqlalchemy import Column, String, Float
from sqlalchemy.engine import Engine
from sqlalchemy.orm import declarative_base

from seq_dbutils import Table

BASE = declarative_base()


class MockTable(Table, BASE):
    __tablename__ = 'Mock'

    mock_id = Column(String(45), primary_key=True)
    some_data = Column(Float(precision=1), nullable=True)
    mysql_engine = 'InnoDB'
    mysql_charset = 'utf8'


@pytest.fixture(scope='session')
def engine_fixture():
    return AlchemyMagicMock(spec=Engine)


@pytest.fixture(scope='session')
def table_fixture(engine_fixture):
    mock_engine = engine_fixture
    return Table(mock_engine, MockTable)


def test_drop_table(engine_fixture, table_fixture):
    with patch('logging.info'):
        with patch('sqlalchemy.schema.Table.drop') as mock_drop:
            table_fixture.drop_table()
            mock_drop.assert_called_once_with(engine_fixture)


def test_create_table(engine_fixture, table_fixture):
    with patch('logging.info'):
        with patch('sqlalchemy.schema.Table.create') as mock_create:
            table_fixture.create_table()
            mock_create.assert_called_once_with(engine_fixture)
