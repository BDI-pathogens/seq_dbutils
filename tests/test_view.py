from os.path import abspath, dirname, join

import pytest
from mock import patch

from seq_dbutils import View

DATA_DIR = join(dirname(abspath(__file__)), 'data')


@pytest.fixture(scope='function')
def instance_fixture():
    with patch('sqlalchemy.orm.sessionmaker') as mock_session:
        return mock_session()


@pytest.fixture(scope='function')
def view_fixture(instance_fixture):
    view_filepath = join(DATA_DIR, 'test_view.sql')
    return View(view_filepath, instance_fixture)


def test_drop_view_if_exists(instance_fixture, view_fixture):
    with patch('logging.info'):
        view_fixture.drop_view_if_exists(instance_fixture, 'test_view')
        instance_fixture.execute.assert_called_once()


def test_create_view(instance_fixture, view_fixture):
    with patch('logging.info'):
        view_fixture.create_view()
        instance_fixture.execute.assert_called_once()


def test_drop_and_create_view(instance_fixture, view_fixture):
    with patch('logging.info'):
        view_fixture.drop_and_create_view()
        assert instance_fixture.execute.call_count == 2
