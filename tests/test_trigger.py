import logging
from os.path import abspath, dirname, join
from unittest import TestCase

from mock import patch

from seq_dbutils import Trigger

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

DATA_DIR = join(dirname(abspath(__file__)), 'data')


class TriggerTestClass(TestCase):

    @patch('logging.info')
    @patch('sqlalchemy.orm.sessionmaker')
    def test_drop_trigger_if_exists(self, mock_session, mock_info):
        mock_instance = mock_session()
        trigger_name = 'test_trigger'
        trigger_filepath = join(DATA_DIR, f'{trigger_name}.sql')
        trigger = Trigger(trigger_filepath, mock_instance)
        trigger.drop_trigger_if_exists()
        sql = f"DROP TRIGGER IF EXISTS {trigger_name};"
        mock_info.assert_called_once()
        mock_info.assert_called_with(sql)
        mock_instance.execute.assert_called_once()
        mock_instance.execute.assert_called_with(sql)

    @patch('logging.info')
    @patch('sqlalchemy.orm.sessionmaker')
    def test_create_trigger(self, mock_session, mock_info):
        mock_instance = mock_session()
        trigger_name = 'test_trigger'
        trigger_filepath = join(DATA_DIR, f'{trigger_name}.sql')
        trigger = Trigger(trigger_filepath, mock_instance)
        trigger.create_trigger()
        sql = f"""CREATE TRIGGER {trigger_name}
BEFORE UPDATE ON Pt
  FOR EACH ROW SET NEW.modified = CURRENT_TIMESTAMP;"""
        mock_info.assert_called_once()
        mock_info.assert_called_with(sql)
        mock_instance.execute.assert_called_once()
        mock_instance.execute.assert_called_with(sql)

    @patch('logging.info')
    @patch('sqlalchemy.orm.sessionmaker')
    def test_drop_and_create_trigger(self, mock_session, mock_info):
        mock_instance = mock_session()
        trigger_name = 'test_trigger'
        trigger_filepath = join(DATA_DIR, f'{trigger_name}.sql')
        trigger = Trigger(trigger_filepath, mock_instance)
        trigger.drop_and_create_trigger()
        self.assertEqual(mock_info.call_count, 2)
        self.assertEqual(mock_instance.execute.call_count, 2)
