import logging
import pandas as pd
import seq_dbutils
from datetime import datetime
from mock import patch
from os.path import abspath, dirname, join
from unittest import TestCase

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s:%(levelname)s:%(message)s")

THIS_DIR = dirname(abspath(__file__))
DATA_DIR = join(THIS_DIR, 'data')


class DataFrameUtilsTestClass(TestCase):

    @staticmethod
    def test_remove_rows_with_blank_col_subset_ok():
        df = pd.DataFrame(data={'id1': ['a', None, 'c'],
                                'id2': ['d', None, 'f'],
                                'id3': ['g', 'h', 'i']},
                          columns=['id1', 'id2', 'id3'])
        df_result = seq_dbutils.DataFrameUtils.remove_rows_with_blank_col_subset(df, ['id1', 'id2'])
        df_expected = pd.DataFrame(data={'id1': ['a', 'c'],
                                         'id2': ['d', 'f'],
                                         'id3': ['g', 'i']},
                                   columns=['id1', 'id2', 'id3'])
        assert df_result.equals(df_expected)

    @staticmethod
    def test_remove_rows_with_blank_col_subset_no_action():
        df = pd.DataFrame(data={'id1': ['a', 'b', 'c'],
                                'id2': ['d', 'b', 'f'],
                                'id3': ['g', 'h', 'i']},
                          columns=['id1', 'id2', 'id3'])
        df_result = seq_dbutils.DataFrameUtils.remove_rows_with_blank_col_subset(df, ['id1', 'id2'])
        assert df_result.equals(df)

    def test_apply_date_format_dash(self):
        result = seq_dbutils.DataFrameUtils.apply_date_format('-', '%Y-%m-%d')
        self.assertIsNone(result)

    def test_apply_date_format_value_blank(self):
        result = seq_dbutils.DataFrameUtils.apply_date_format(None, '%Y-%m-%d')
        self.assertIsNone(result)

    @staticmethod
    def test_apply_date_format_ok():
        input_date = '2020-09-18'
        date_format = '%Y-%m-%d'
        result = seq_dbutils.DataFrameUtils.apply_date_format(input_date, date_format)
        expected = datetime.strptime(input_date, date_format).date()
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
        expected = datetime.strptime('2020-09-18', date_format).date()
        assert result == expected

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
    @patch('sqlalchemy.engine.Engine')
    def test_add_prefixed_incremented_id_not_in_df_or_db(mock_engine):
        table = 'mock_table'
        id_name = 'lookup_id'
        id_prefix = 'ID'
        df = pd.DataFrame(data={'col1': ['a', 'b'],
                                'col2': ['x', 'y']},
                          columns=['col1', 'col2'])

        mock_engine.execute().fetchone.return_value = None
        df_result = seq_dbutils.DataFrameUtils.add_prefixed_incremented_id(mock_engine, df, table, id_name, id_prefix,
                                                                           id_zero_padding=2)
        df_expected = pd.DataFrame(data={'col1': ['a', 'b'],
                                         'col2': ['x', 'y'],
                                         id_name: [id_prefix + '01',
                                                   id_prefix + '02']},
                                   columns=['col1', 'col2', id_name])
        assert df_result.equals(df_expected)

    @staticmethod
    @patch('sqlalchemy.engine')
    def test_add_prefixed_incremented_id_exists_db(mock_engine):
        table = 'mock_table'
        id_name = 'lookup_id'
        id_prefix = 'ID'
        df = pd.DataFrame(data={'col1': ['a', 'b'],
                                'col2': ['x', 'y']},
                          columns=['col1', 'col2'])

        mock_engine.execute().fetchone.return_value = [id_prefix + '05']
        df_result = seq_dbutils.DataFrameUtils.add_prefixed_incremented_id(mock_engine, df, table, id_name, id_prefix,
                                                                           id_zero_padding=2)
        df_expected = pd.DataFrame(data={'col1': ['a', 'b'],
                                         'col2': ['x', 'y'],
                                         id_name: [id_prefix + '06',
                                                   id_prefix + '07']},
                                   columns=['col1', 'col2', id_name])
        assert df_result.equals(df_expected)

    @staticmethod
    @patch('sqlalchemy.engine.Engine')
    def test_add_prefixed_incremented_id_exists_df_and_db(mock_engine):
        table = 'mock_table'
        id_name = 'lookup_id'
        id_prefix = 'ID'
        df = pd.DataFrame(data={'col1': ['a', 'b'],
                                'col2': ['x', 'y'],
                                id_name: [id_prefix + '01',
                                          None]},
                          columns=['col1', 'col2', id_name])

        mock_engine.execute().fetchone.return_value = [id_prefix + '05']
        df_result = seq_dbutils.DataFrameUtils.add_prefixed_incremented_id(mock_engine, df, table, id_name, id_prefix,
                                                                           id_zero_padding=2)
        df_expected = pd.DataFrame(data={'col1': ['a', 'b'],
                                         'col2': ['x', 'y'],
                                         id_name: [id_prefix + '01',
                                                   id_prefix + '06']},
                                   columns=['col1', 'col2', id_name])
        assert df_result.equals(df_expected)
