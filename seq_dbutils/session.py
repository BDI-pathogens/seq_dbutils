import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


class Session:

    def __init__(self, session_instance):
        assert hasattr(session_instance, 'commit')
        assert hasattr(session_instance, 'execute')
        self.session_instance = session_instance

    def commit_changes(self, commit):
        if commit:
            self.session_instance.commit()
            logging.info('Changes committed')
        else:
            logging.info('Changes NOT committed')

    def log_and_execute_sql(self, sql):
        logging.info(sql)
        self.session_instance.execute(sql)
