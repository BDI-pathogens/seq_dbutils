import datetime
from unittest import TestCase

import pandas as pd
from mock import patch
from mock_alchemy.mocking import AlchemyMagicMock
from sqlalchemy.engine import Engine

from seq_dbutils import DataFrameUtils


class DataFrameUtilsTestClass(TestCase):

    def test_apply_date_format_value_blank(self):
        result = DataFrameUtils.apply_date_format(None, '%Y-%m-%d')
        self.assertIsNone(result)

    @staticmethod
    def test_apply_date_format_ok():
        input_date = '2023-10-25'
        date_format = '%Y-%m-%d'
        result = DataFrameUtils.apply_date_format(input_date, date_format)
        expected = datetime.date(2023, 10, 25)
        assert result == expected

    @staticmethod
    @patch('sys.exit')
    def test_apply_date_format_error(mock_exit):
        input_date = 'xxxxxxxxxxxx'
        date_format = '%Y-%m-%d'
        DataFrameUtils.apply_date_format(input_date, date_format)
        mock_exit.assert_called_once()

    @staticmethod
    def test_apply_date_format_value_unconverted():
        input_date = '2023-10-25  00:00:00'
        date_format = '%Y-%m-%d'
        result = DataFrameUtils.apply_date_format(input_date, date_format)
        expected = datetime.date(2023, 10, 25)
        assert result == expected

    @staticmethod
    @patch('pandas.read_sql')
    def test_get_db_table_col_list(mock_sql):
        mock_engine = AlchemyMagicMock(spec=Engine)
        DataFrameUtils(mock_engine, 'Test').get_db_table_col_list()
        mock_sql.assert_called_once_with('SHOW COLUMNS FROM Test;', mock_engine)

    @staticmethod
    @patch('seq_dbutils.DataFrameUtils.get_db_table_col_list', return_value=['col1', 'col3'])
    def test_create_db_table_dataframe(mock_get):
        mock_engine = AlchemyMagicMock(spec=Engine)
        df = pd.DataFrame(data={
            'col1': ['a', 'b', None],
            'col2': ['some data', 'some more data', None],
            'col3': [None, None, None],
        }, columns=['col1', 'col2', 'col3'])
        df_result = DataFrameUtils(mock_engine, 'Test').create_db_table_dataframe(df)
        df_expected = pd.DataFrame(data={
            'col1': ['a', 'b'],
            'col3': [None, None],
        }, columns=['col1', 'col3'])
        mock_get.assert_called_once()
        pd.testing.assert_frame_equal(df_result, df_expected)
