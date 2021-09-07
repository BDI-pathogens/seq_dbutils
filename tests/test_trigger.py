import logging
from os.path import abspath, dirname, join
from unittest import TestCase

import seq_dbutils
from mock import patch

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

THIS_DIR = dirname(abspath(__file__))
DATA_DIR = join(THIS_DIR, 'data')


class TriggerTestClass(TestCase):

    @patch('logging.info')
    @patch('sqlalchemy.orm.sessionmaker')
    def test_drop_trigger_if_exists(self, mock_session, mock_info):
        mock_instance = mock_session()
        trigger_name = 'mock_trigger'
        sql = "DROP TRIGGER IF EXISTS {};".format(trigger_name)
        commit = True
        trigger = seq_dbutils.Trigger(DATA_DIR, mock_instance, commit)
        trigger.drop_trigger_if_exists(trigger_name)
        mock_instance.execute.assert_called_with(sql)

    @patch('sys.exit')
    @patch('logging.error')
    @patch('sqlalchemy.orm.sessionmaker')
    def test_create_trigger_no_file(self, mock_session, mock_error, mock_exit):
        mock_instance = mock_session()
        file_name = 'mock_trigger.sql'
        commit = True
        trigger_fp = join(DATA_DIR, file_name)
        trigger = seq_dbutils.Trigger(DATA_DIR, mock_instance, commit)
        trigger.create_trigger(file_name)
        mock_error.assert_called_with(f'Unable to find file: {trigger_fp}')

    @patch('logging.info')
    @patch('sqlalchemy.orm.sessionmaker')
    def test_create_trigger_ok(self, mock_session, mock_info):
        mock_instance = mock_session()
        file_name = 'test_create_trigger_ok.sql'
        commit = True
        trigger = seq_dbutils.Trigger(DATA_DIR, mock_instance, commit)
        trigger.create_trigger(file_name)
        sql = """CREATE TRIGGER test_create_trigger_ok
BEFORE UPDATE ON Pt
  FOR EACH ROW SET NEW.modified = CURRENT_TIMESTAMP;"""
        mock_instance.execute.assert_called_with(sql)

    @patch('logging.info')
    @patch('sqlalchemy.orm.sessionmaker')
    def test_drop_and_create_trigger_ok(self, mock_session, mock_info):
        mock_instance = mock_session()
        file_name = 'test_create_trigger_ok.sql'
        commit = True
        trigger = seq_dbutils.Trigger(DATA_DIR, mock_instance, commit)
        trigger.drop_and_create_trigger(file_name)
        mock_info.assert_called_with('Changes committed')
