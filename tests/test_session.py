import pytest
from mock import patch
from mock_alchemy.mocking import AlchemyMagicMock

from seq_dbutils import Session


@pytest.fixture(scope='session')
def session():
    return AlchemyMagicMock()


def test_log_and_execute_sql(session):
    sql = 'SELECT * FROM test;'
    with patch('logging.info'):
        Session(session).log_and_execute_sql(sql)
        session.execute.assert_called_once()


def test_commit_changes_false(session):
    with patch('logging.info'):
        Session(session).commit_changes(False)
        session.commit.assert_not_called()


def test_commit_changes_true(session):
    with patch('logging.info'):
        Session(session).commit_changes(True)
        session.commit.assert_called_once()
