from os.path import abspath, dirname, join

import pytest
from mock import patch

from seq_dbutils import View

DATA_DIR = join(dirname(abspath(__file__)), 'data')


@pytest.fixture()
def instance():
    with patch('sqlalchemy.orm.sessionmaker') as mock_session:
        return mock_session()


@pytest.fixture()
def view(instance):
    view_filepath = join(DATA_DIR, 'test_view.sql')
    return View(view_filepath, instance)


def test_drop_view_if_exists(instance, view):
    with patch('logging.info'):
        view.drop_view_if_exists(instance, 'test_view')
        instance.execute.assert_called_once()


def test_create_view(instance, view):
    with patch('logging.info'):
        view.create_view()
        instance.execute.assert_called_once()


def test_drop_and_create_view(instance, view):
    with patch('logging.info'):
        view.drop_and_create_view()
        assert instance.execute.call_count == 2
