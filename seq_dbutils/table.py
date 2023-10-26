import logging

from sqlalchemy.engine import Engine

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


class Table:

    def __init__(self, engine, table_class):
        assert isinstance(engine, Engine)
        assert hasattr(table_class, '__tablename__')
        self.engine = engine
        self.table_class = table_class

    def drop_table(self):
        logging.info(f"Dropping table '{self.table_class.__tablename__}'")
        self.table_class.__table__.drop(self.engine, check_first=True)

    def create_table(self):
        logging.info(f"Creating table '{self.table_class.__tablename__}'")
        self.table_class.__table__.create(self.engine, check_first=True)
