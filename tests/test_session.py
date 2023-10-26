from unittest import TestCase

from mock_alchemy.mocking import AlchemyMagicMock

from seq_dbutils import Session


class DatabaseTestClass(TestCase):

    def setUp(self):
        self.mock_instance = AlchemyMagicMock()

    def test_log_and_execute_sql(self):
        sql = 'SELECT * FROM test;'
        Session(self.mock_instance).log_and_execute_sql(sql)
        self.mock_instance.execute.assert_called_once_with(sql)

    def test_commit_changes_false(self):
        Session(self.mock_instance).commit_changes(False)
        self.mock_instance.commit.assert_not_called()

    def test_commit_changes_true(self):
        Session(self.mock_instance).commit_changes(True)
        self.mock_instance.commit.assert_called_once()
