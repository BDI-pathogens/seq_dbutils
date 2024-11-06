from os.path import abspath, dirname, join

import pytest
from mock import patch

from seq_dbutils import Trigger

DATA_DIR = join(dirname(abspath(__file__)), 'data')


@pytest.fixture()
def instance():
    with patch('sqlalchemy.orm.sessionmaker') as mock_session:
        return mock_session()


@pytest.fixture()
def trigger(instance):
    trigger_filepath = join(DATA_DIR, 'test_trigger.sql')
    return Trigger(trigger_filepath, instance)


def test_drop_trigger_if_exists(instance, trigger):
    with patch('logging.info'):
        trigger.drop_trigger_if_exists()
        instance.execute.assert_called_once()


def test_create_trigger(instance, trigger):
    with patch('logging.info'):
        trigger.create_trigger()
        instance.execute.assert_called_once()


def test_drop_and_create_trigger(instance, trigger):
    with patch('logging.info'):
        trigger.drop_and_create_trigger()
        assert instance.execute.call_count == 2
