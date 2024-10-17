import pytest
from mock import patch
from mock_alchemy.mocking import AlchemyMagicMock

from seq_dbutils import Session


@pytest.fixture(scope='session')
def alchemy_fixture():
    return AlchemyMagicMock()


def test_log_and_execute_sql(alchemy_fixture):
    with patch('logging.info'):
        sql = 'SELECT * FROM test;'
        Session(alchemy_fixture).log_and_execute_sql(sql)
        alchemy_fixture.execute.assert_called_once()


def test_commit_changes_false(alchemy_fixture):
    with patch('logging.info'):
        Session(alchemy_fixture).commit_changes(False)
        alchemy_fixture.commit.assert_not_called()


def test_commit_changes_true(alchemy_fixture):
    with patch('logging.info'):
        Session(alchemy_fixture).commit_changes(True)
        alchemy_fixture.commit.assert_called_once()
