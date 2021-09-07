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
        view_name = 'mock_view'
        sql = f'DROP VIEW IF EXISTS {view_name};'
        commit = True
        view = seq_dbutils.View(DATA_DIR, mock_instance, commit)
        view.drop_view_if_exists(view_name)
        mock_instance.execute.assert_called_with(sql)

    @patch('sys.exit')
    @patch('logging.error')
    @patch('sqlalchemy.orm.sessionmaker')
    def test_create_view_no_file(self, mock_session, mock_error, mock_exit):
        mock_instance = mock_session()
        view_name = 'mock_view'
        commit = True
        view_fp = join(DATA_DIR, view_name + '.sql')
        view = seq_dbutils.View(DATA_DIR, mock_instance, commit)
        view.create_view(view_name)
        mock_error.assert_called_with(f'Unable to find file: {view_fp}')

    @patch('logging.info')
    @patch('sqlalchemy.orm.sessionmaker')
    def test_create_view_ok(self, mock_session, mock_info):
        mock_instance = mock_session()
        view_name = 'test_create_view_ok'
        commit = True
        view = seq_dbutils.View(DATA_DIR, mock_instance, commit)
        view.create_view(view_name)
        sql = f'CREATE VIEW {view_name} AS \nSELECT * FROM Pt;'
        mock_instance.execute.assert_called_with(sql)

    @patch('logging.info')
    @patch('sqlalchemy.orm.sessionmaker')
    def test_drop_and_create_view_ok(self, mock_session, mock_info):
        mock_instance = mock_session()
        view_name = 'test_create_view_ok'
        commit = True
        view = seq_dbutils.View(DATA_DIR, mock_instance, commit)
        view.drop_and_create_view(view_name)
        mock_info.assert_called_with('Changes committed')
