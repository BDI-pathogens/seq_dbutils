import datetime
import logging
from os.path import abspath, dirname, join
from unittest import TestCase

import numpy as np
import pandas as pd
from mock import patch, call

import seq_dbutils

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

THIS_DIR = dirname(abspath(__file__))
DATA_DIR = join(THIS_DIR, 'data')


class DataFrameUtilsTestClass(TestCase):

    def test_apply_date_format_value_blank(self):
        result = seq_dbutils.DataFrameUtils.apply_date_format(None, '%Y-%m-%d')
        self.assertIsNone(result)

    @staticmethod
    def test_apply_date_format_ok():
        input_date = '2020-09-18'
        date_format = '%Y-%m-%d'
        result = seq_dbutils.DataFrameUtils.apply_date_format(input_date, date_format)
        expected = datetime.datetime.strptime(input_date, date_format).date()
        assert result == expected

    @staticmethod
    @patch('sys.exit')
    @patch('logging.error')
    def test_apply_date_format_error(mock_error, mock_exit):
        input_date = 'xxxxxxxxxxxx'
        date_format = '%Y-%m-%d'
        result = seq_dbutils.DataFrameUtils.apply_date_format(input_date, date_format)
        mock_error.assert_called_with("time data 'xxxxxxxxxxxx' does not match format '%Y-%m-%d'")
        assert result == input_date

    @staticmethod
    def test_apply_date_format_value_uncoverted():
        input_date = '2020-09-18  00:00:00'
        date_format = '%Y-%m-%d'
        result = seq_dbutils.DataFrameUtils.apply_date_format(input_date, date_format)
        expected = datetime.datetime.strptime('2020-09-18', date_format).date()
        assert result == expected

    @staticmethod
    def test_apply_date_format_dash():
        input_date = '-'
        date_format = '%Y-%m-%d'
        result = seq_dbutils.DataFrameUtils.apply_date_format(input_date, date_format)
        assert not result

    @staticmethod
    @patch('sys.exit')
    @patch('logging.error')
    def test_read_csv_with_header_mapping_no_file(mock_error, mock_exit):
        file = 'fake.csv'
        seq_dbutils.DataFrameUtils.read_csv_with_header_mapping(file, None)
        mock_error.assert_called_with(f'File {file} does not exist. Exiting...')

    @staticmethod
    @patch('logging.info')
    def test_read_csv_with_header_mapping_ok(mock_info):
        file = join(DATA_DIR, 'test_read_csv_with_header_mapping_ok.csv')
        name_mapping_dict = {'col2': 'rename_col2'}
        df_result = seq_dbutils.DataFrameUtils.read_csv_with_header_mapping(file,
                                                                            col_name_mapping_dict=name_mapping_dict)
        mock_info.assert_called_with(f'Read file {file} with lines: %s', 3)
        df_expected = pd.DataFrame(data={'col1': ['a', 'b', 'c'],
                                         'rename_col2': [1, 2, 3]},
                                   columns=['col1', 'rename_col2'])
        assert df_result.equals(df_expected)

    @staticmethod
    @patch('logging.info')
    def test_read_csv_with_header_mapping_no_dict(mock_info):
        file = join(DATA_DIR, 'test_read_csv_with_header_mapping_ok.csv')
        df_result = seq_dbutils.DataFrameUtils.read_csv_with_header_mapping(file)
        mock_info.assert_called_with(f'Read file {file} with lines: %s', 3)
        df_expected = pd.DataFrame(data={'col1': ['a', 'b', 'c'],
                                         'col2': [1, 2, 3]},
                                   columns=['col1', 'col2'])
        assert df_result.equals(df_expected)

    @staticmethod
    @patch('seq_dbutils.DataFrameUtils.apply_date_format')
    def test_format_date_cols(mock_apply):
        df = pd.DataFrame(data={'col1': ['a', 'b'],
                                'col2': ['2021-09-16', '2021-09-17']},
                          columns=['col1', 'col2'])
        date_col_list = ['col2']
        seq_dbutils.DataFrameUtils.format_date_cols(df, date_col_list)
        mock_apply.assert_has_calls([call('2021-09-16', '%Y-%m-%d'), call('2021-09-17', '%Y-%m-%d')])

    @staticmethod
    def test_format_date_cols_no_date_col():
        df = pd.DataFrame(data={'col1': ['a', 'b'],
                                'col2': ['2021-09-16', '2021-09-17']},
                          columns=['col1', 'col2'])
        date_col_list = ['col3']
        df_result = seq_dbutils.DataFrameUtils.format_date_cols(df, date_col_list)
        df_expected = pd.DataFrame(data={'col1': ['a', 'b'],
                                         'col2': ['2021-09-16', '2021-09-17']},
                                   columns=['col1', 'col2'])
        assert df_result.equals(df_expected)

    @staticmethod
    def test_add_dob_month_col():
        df = pd.DataFrame(data={'col1': ['a', 'b'],
                                'col2': [datetime.date(2021, 1, 16), datetime.date(2021, 9, 16)]},
                          columns=['col1', 'col2'])
        df_result = seq_dbutils.DataFrameUtils.add_dob_month_col(df, dob_date_col='col2')
        df_expected = pd.DataFrame(data={'col1': ['a', 'b'],
                                         'col2': [datetime.date(2021, 1, 16), datetime.date(2021, 9, 16)],
                                         'dob_month': [1, 9]},
                                   columns=['col1', 'col2', 'dob_month'])
        assert df_result.equals(df_expected)

    @staticmethod
    def test_add_dob_month_col_already_there():
        df = pd.DataFrame(data={'col1': ['a', 'b'],
                                'col2': [datetime.date(2021, 1, 16), datetime.date(2021, 9, 16)],
                                'dob_month': [1, 9]},
                          columns=['col1', 'col2', 'dob_month'])
        df_result = seq_dbutils.DataFrameUtils.add_dob_month_col(df, dob_date_col='col2')
        assert df_result.equals(df)

    @staticmethod
    def test_add_dob_year_col():
        df = pd.DataFrame(data={'col1': ['a', 'b'],
                                'col2': [datetime.date(2000, 1, 16), datetime.date(1975, 9, 16)]},
                          columns=['col1', 'col2'])
        df_result = seq_dbutils.DataFrameUtils.add_dob_year_col(df, dob_date_col='col2')
        df_expected = pd.DataFrame(data={'col1': ['a', 'b'],
                                         'col2': [datetime.date(2000, 1, 16), datetime.date(1975, 9, 16)],
                                         'dob_year': [2000, 1975]},
                                   columns=['col1', 'col2', 'dob_year'])
        assert df_result.equals(df_expected)

    @staticmethod
    def test_add_dob_year_col_already_there():
        df = pd.DataFrame(data={'col1': ['a', 'b'],
                                'col2': [datetime.date(2000, 1, 16), datetime.date(1975, 9, 16)],
                                'dob_year': [2000, 1975]},
                          columns=['col1', 'col2', 'dob_year'])
        df_result = seq_dbutils.DataFrameUtils.add_dob_year_col(df, dob_date_col='col2')
        assert df_result.equals(df)

    @staticmethod
    def test_replace_nan_with_none():
        df = pd.DataFrame(data={'col1': ['a', 'b', np.nan],
                                'col2': [1, np.nan, 3]},
                          columns=['col1', 'col2'])
        df_result = seq_dbutils.DataFrameUtils.replace_nan_with_none(df)
        df_expected = pd.DataFrame(data={'col1': ['a', 'b', None],
                                         'col2': [1, None, 3]},
                                   columns=['col1', 'col2'])
        df_expected['col2'] = df_expected['col2'].astype(object)
        assert df_result.equals(df_expected)
