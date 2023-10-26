import logging
from os.path import isfile, splitext, basename

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


class View:

    def __init__(self, view_filepath, session_instance):
        assert isfile(view_filepath)
        assert hasattr(session_instance, 'execute')
        self.view_filepath = view_filepath
        self.session_instance = session_instance
        self.view_name = splitext(basename(self.view_filepath))[0]

    def drop_and_create_view(self):
        self.drop_view_if_exists(self.session_instance, self.view_name)
        self.create_view()

    @staticmethod
    def drop_view_if_exists(session_instance, view_name):
        drop_sql = f'DROP VIEW IF EXISTS {view_name};'
        logging.info(drop_sql)
        session_instance.execute(drop_sql)

    def create_view(self):
        with open(self.view_filepath, 'r') as reader:
            create_sql = reader.read()
            create_sql = f'CREATE VIEW {self.view_name} AS \n' + create_sql
            logging.info(create_sql)
            self.session_instance.execute(create_sql)
