import pytest
from mock import patch

from seq_dbutils import Connection


@pytest.fixture(scope='session')
def connection_fixture():
    return Connection('me', 'mypassword', 'myhost', 'mydb')


def test_create_sql_engine_ok(connection_fixture):
    with patch('logging.info'):
        with patch('sqlalchemy.create_engine') as mock_create:
            connection_fixture.create_sql_engine()
            mock_create.assert_called_once_with('mysql+mysqlconnector://me:mypassword@myhost/mydb', echo=False)


def test_create_sql_engine_fail(connection_fixture):
    with patch('logging.info'):
        with patch('logging.error'):
            with patch('sys.exit') as mock_exit:
                with patch('sqlalchemy.create_engine', side_effect=Exception()):
                    connection_fixture.create_sql_engine()
                    mock_exit.assert_called_once()
