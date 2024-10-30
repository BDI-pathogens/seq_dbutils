import logging
from datetime import datetime

import pandas as pd
from sqlalchemy.engine import Engine

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


class DataFrameUtils:

    def __init__(self, engine, tablename):
        assert isinstance(engine, Engine)
        assert isinstance(tablename, str)
        self.engine = engine
        self.tablename = tablename

    def get_db_table_col_list(self):
        df_db_table_cols = pd.read_sql(f'SHOW COLUMNS FROM {self.tablename};', self.engine)
        db_table_col_list = df_db_table_cols['Field'].tolist()
        return db_table_col_list

    def create_db_table_dataframe(self, df):
        db_table_col_list = self.get_db_table_col_list()
        df_db_table = df.filter(db_table_col_list, axis=1)
        df_db_table = df_db_table.dropna(subset=df_db_table.columns, how='all')
        return df_db_table
