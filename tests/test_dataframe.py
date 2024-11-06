import pandas as pd
import pytest
from mock import patch
from mock_alchemy.mocking import AlchemyMagicMock
from sqlalchemy.engine import Engine

from seq_dbutils import DataFrameUtils


@pytest.fixture(scope='session')
def engine():
    return AlchemyMagicMock(spec=Engine)


def test_get_db_table_col_list(engine):
    field_list = ['field_a', 'field_b', 'field_c']
    df = pd.DataFrame(data={
        'Field': field_list,
        'Type': ['varchar(45)', 'varchar(45)', 'int'],
    }, columns=['Field', 'Type'])
    with patch('pandas.read_sql', return_value=df):
        result = DataFrameUtils(engine, 'Test').get_db_table_col_list()
        assert result == field_list


def test_create_db_table_dataframe(engine):
    df = pd.DataFrame(data={
        'col1': ['a', 'b', None],
        'col2': ['some data', 'some more data', None],
        'col3': [None, None, None],
    }, columns=['col1', 'col2', 'col3'])
    df_expected = pd.DataFrame(data={
        'col1': ['a', 'b'],
        'col3': [None, None],
    }, columns=['col1', 'col3'])
    with patch('seq_dbutils.DataFrameUtils.get_db_table_col_list', return_value=['col1', 'col3']):
        df_result = DataFrameUtils(engine, 'Test').create_db_table_dataframe(df)
        pd.testing.assert_frame_equal(df_result, df_expected)
