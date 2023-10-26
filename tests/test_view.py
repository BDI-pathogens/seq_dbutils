from os.path import abspath, dirname, join
from unittest import TestCase

from mock import patch

from seq_dbutils import View

DATA_DIR = join(dirname(abspath(__file__)), 'data')


class ViewTestClass(TestCase):

    @patch('sqlalchemy.orm.sessionmaker')
    def setUp(self, mock_session):
        self.mock_instance = mock_session()
        self.view_name = 'test_view'
        self.view_filepath = join(DATA_DIR, f'{self.view_name}.sql')
        self.view = View(self.view_filepath, self.mock_instance)

    @patch('logging.info')
    def test_drop_view_if_exists(self, mock_info):
        self.view.drop_view_if_exists(self.mock_instance, self.view_name)
        sql = f'DROP VIEW IF EXISTS {self.view_name};'
        self.mock_instance.execute.assert_called_once_with(sql)

    @patch('logging.info')
    def test_create_view(self, mock_info):
        self.view.create_view()
        sql = f'CREATE VIEW {self.view_name} AS \nSELECT * FROM Pt;'
        self.mock_instance.execute.assert_called_once_with(sql)

    @patch('logging.info')
    def test_drop_and_create_view(self, mock_info):
        self.view.drop_and_create_view()
        self.assertEqual(self.mock_instance.execute.call_count, 2)
