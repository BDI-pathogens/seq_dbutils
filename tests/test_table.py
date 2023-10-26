from unittest import TestCase

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


class TableTestClass(TestCase):

    def setUp(self):
        self.mock_engine = AlchemyMagicMock(spec=Engine)
        self.table = Table(self.mock_engine, MockTable)

    @patch('logging.info')
    @patch('sqlalchemy.schema.Table.drop')
    def test_drop_table(self, mock_drop, mock_info):
        self.table.drop_table()
        mock_drop.assert_called_once_with(self.mock_engine, check_first=True)

    @patch('logging.info')
    @patch('sqlalchemy.schema.Table.create')
    def test_create_table(self, mock_create, mock_info):
        self.table.create_table()
        mock_create.assert_called_once_with(self.mock_engine, check_first=True)

