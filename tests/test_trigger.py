from os.path import abspath, dirname, join

import pytest
from mock import patch

from seq_dbutils import Trigger

DATA_DIR = join(dirname(abspath(__file__)), 'data')


@pytest.fixture(scope='function')
def instance_fixture():
    with patch('sqlalchemy.orm.sessionmaker') as mock_session:
        return mock_session()


@pytest.fixture(scope='function')
def trigger_fixture(instance_fixture):
    trigger_filepath = join(DATA_DIR, 'test_trigger.sql')
    return Trigger(trigger_filepath, instance_fixture)


def test_drop_trigger_if_exists(instance_fixture, trigger_fixture):
    with patch('logging.info'):
        trigger_fixture.drop_trigger_if_exists()
        instance_fixture.execute.assert_called_once()


def test_create_trigger(instance_fixture, trigger_fixture):
    with patch('logging.info'):
        trigger_fixture.create_trigger()
        instance_fixture.execute.assert_called_once()


def test_drop_and_create_trigger(instance_fixture, trigger_fixture):
    with patch('logging.info'):
        trigger_fixture.drop_and_create_trigger()
        assert instance_fixture.execute.call_count == 2
