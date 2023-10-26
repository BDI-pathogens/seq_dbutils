import logging
import sys
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

    @staticmethod
    def apply_date_format(input_date, format_date):
        if input_date:
            format_time = format_date + ' %H:%M:%S'
            try:
                input_date = datetime.strptime(input_date, format_date).date()
            except ValueError as ex:
                if 'unconverted data remains:' in ex.args[0]:
                    input_date = datetime.strptime(input_date, format_time).date()
                else:
                    logging.error(str(ex))
                    sys.exit(1)
        else:
            input_date = None
        return input_date
