import logging
from os.path import abspath, dirname, join
from unittest import TestCase

from mock import patch

from seq_dbutils import View

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

DATA_DIR = join(dirname(abspath(__file__)), 'data')


class ViewTestClass(TestCase):

    @patch('logging.info')
    @patch('sqlalchemy.orm.sessionmaker')
    def test_drop_view_if_exists(self, mock_session, mock_info):
        mock_instance = mock_session()
        view_name = 'test_view'
        view_filepath = join(DATA_DIR, f'{view_name}.sql')
        view = View(view_filepath, mock_instance)
        view.drop_view_if_exists(mock_instance, view_name)
        sql = f'DROP VIEW IF EXISTS {view_name};'
        mock_info.assert_called_once()
        mock_info.assert_called_with(sql)
        mock_instance.execute.assert_called_once()
        mock_instance.execute.assert_called_with(sql)

    @patch('logging.info')
    @patch('sqlalchemy.orm.sessionmaker')
    def test_create_view(self, mock_session, mock_info):
        mock_instance = mock_session()
        view_name = 'test_view'
        view_filepath = join(DATA_DIR, f'{view_name}.sql')
        view = View(view_filepath, mock_instance)
        view.create_view()
        sql = f'CREATE VIEW {view_name} AS \nSELECT * FROM Pt;'
        mock_info.assert_called_once()
        mock_info.assert_called_with(sql)
        mock_instance.execute.assert_called_once()
        mock_instance.execute.assert_called_with(sql)

    @patch('logging.info')
    @patch('sqlalchemy.orm.sessionmaker')
    def test_drop_and_create_view(self, mock_session, mock_info):
        mock_instance = mock_session()
        view_name = 'test_view'
        view_filepath = join(DATA_DIR, f'{view_name}.sql')
        view = View(view_filepath, mock_instance)
        view.drop_and_create_view()
        self.assertEqual(mock_info.call_count, 2)
        self.assertEqual(mock_instance.execute.call_count, 2)
