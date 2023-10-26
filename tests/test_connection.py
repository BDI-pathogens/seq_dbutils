from unittest import TestCase

from mock import patch, Mock

from seq_dbutils import Connection


class ConnectionTestClass(TestCase):

    def setUp(self):
        self.user = 'me'
        self.pwd = 'mypassword'
        self.host = 'myhost'
        self.db = 'mydb'
        self.connection = Connection(self.user, self.pwd, self.host, self.db)
        self.connector_type = 'mysqlconnector'

    @patch('sqlalchemy.create_engine')
    def test_create_sql_engine_ok(self, mock_create):
        self.connection.create_sql_engine()
        mock_create.assert_called_once_with(
            f'mysql+{self.connector_type}://{self.user}:{self.pwd}@{self.host}/{self.db}', echo=False)

    @patch('sys.exit')
    @patch('sqlalchemy.create_engine')
    def test_create_sql_engine_fail(self, mock_create, mock_exit):
        mock_create.side_effect = Mock(side_effect=Exception())
        self.connection.create_sql_engine()
        mock_exit.assert_called_once()
