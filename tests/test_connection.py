import pytest
from mock import patch

from seq_dbutils import Connection


@pytest.fixture()
def connection():
    return Connection('me', 'mypassword', 'myhost', 'mydb')


def test_create_sql_engine_ok(connection):
    with patch('logging.info'):
        with patch('sqlalchemy.create_engine') as mock_create:
            connection.create_sql_engine()
            mock_create.assert_called_once_with('mysql+mysqlconnector://me:mypassword@myhost/mydb', echo=False)


def test_create_sql_engine_fail(connection):
    with patch('logging.info'):
        with patch('sqlalchemy.create_engine', side_effect=Exception()):
            with pytest.raises(Exception):
                connection.create_sql_engine()
