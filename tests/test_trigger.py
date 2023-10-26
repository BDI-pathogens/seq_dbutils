from os.path import abspath, dirname, join
from unittest import TestCase

from mock import patch

from seq_dbutils import Trigger

DATA_DIR = join(dirname(abspath(__file__)), 'data')


class TriggerTestClass(TestCase):

    @patch('sqlalchemy.orm.sessionmaker')
    def setUp(self, mock_session):
        self.mock_instance = mock_session()
        self.trigger_name = 'test_trigger'
        self.trigger_filepath = join(DATA_DIR, f'{self.trigger_name}.sql')
        self.trigger = Trigger(self.trigger_filepath, self.mock_instance)

    @patch('logging.info')
    def test_drop_trigger_if_exists(self, mock_info):
        self.trigger.drop_trigger_if_exists()
        sql = f"DROP TRIGGER IF EXISTS {self.trigger_name};"
        self.mock_instance.execute.assert_called_once_with(sql)

    @patch('logging.info')
    def test_create_trigger(self, mock_info):
        self.trigger.create_trigger()
        sql = f"""CREATE TRIGGER {self.trigger_name}
BEFORE UPDATE ON Pt
  FOR EACH ROW SET NEW.modified = CURRENT_TIMESTAMP;"""
        self.mock_instance.execute.assert_called_once_with(sql)

    @patch('logging.info')
    def test_drop_and_create_trigger(self, mock_info):
        self.trigger.drop_and_create_trigger()
        self.assertEqual(self.mock_instance.execute.call_count, 2)
