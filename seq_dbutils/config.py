import logging
from configparser import ConfigParser

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


class Config:
    configParser = ConfigParser()

    @classmethod
    def initialize(cls, config_file_path):
        try:
            return cls.configParser.read(config_file_path)
        except FileNotFoundError:
            raise FileNotFoundError(f'Config file {config_file_path} does not exist. Exiting...')

    @classmethod
    def get_section_config(cls, required_section, required_key):
        return cls.configParser.get(required_section, required_key)

    @staticmethod
    def get_db_config(required_section):
        logging.info(f"Extracting config for '{required_section}'")
        user = Config.get_section_config(required_section, 'user')
        key = Config.get_section_config(required_section, 'key').encode()
        host = Config.get_section_config(required_section, 'host')
        db = Config.get_section_config(required_section, 'db')
        return user, key, host, db
