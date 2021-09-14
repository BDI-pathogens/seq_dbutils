import logging
from os.path import abspath, dirname, join
from unittest import TestCase

import seq_dbutils
from mock import patch

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

THIS_DIR = dirname(abspath(__file__))
DATA_DIR = join(THIS_DIR, 'data')


class ViewTestClass(TestCase):

    @patch('logging.info')
    @patch('sqlalchemy.orm.sessionmaker')
    def test_drop_view_if_exists(self, mock_session, mock_info):
        mock_instance = mock_session()
        view_name = 'test_view'
        view_filepath = join(DATA_DIR, f'{view_name}.sql')
        view = seq_dbutils.View(view_filepath, mock_instance)
        view.drop_view_if_exists()
        sql = f'DROP VIEW IF EXISTS {view_name};'
        mock_instance.execute.assert_called_with(sql)

    @patch('logging.info')
    @patch('sqlalchemy.orm.sessionmaker')
    def test_create_view_ok(self, mock_session, mock_info):
        mock_instance = mock_session()
        view_name = 'test_view'
        view_filepath = join(DATA_DIR, f'{view_name}.sql')
        view = seq_dbutils.View(view_filepath, mock_instance)
        view.create_view()
        sql = f'CREATE VIEW {view_name} AS \nSELECT * FROM Pt;'
        mock_instance.execute.assert_called_with(sql)

    @patch('logging.info')
    @patch('sqlalchemy.orm.sessionmaker')
    def test_drop_and_create_view_ok(self, mock_session, mock_info):
        mock_instance = mock_session()
        view_name = 'test_view'
        view_filepath = join(DATA_DIR, f'{view_name}.sql')
        view = seq_dbutils.View(view_filepath, mock_instance)
        view.drop_and_create_view()
        sql = f'CREATE VIEW {view_name} AS \nSELECT * FROM Pt;'
        mock_info.assert_called_with(sql)
