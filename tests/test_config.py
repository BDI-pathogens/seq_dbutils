import argparse
import logging
from os.path import abspath, dirname, join
from unittest import TestCase

import seq_dbutils
from mock import patch

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s:%(levelname)s:%(message)s")

THIS_DIR = dirname(abspath(__file__))
DATA_DIR = join(THIS_DIR, 'data')


class ArgsTestClass(TestCase):

    @classmethod
    @patch('sys.exit')
    @patch('logging.error')
    def test_initialize_no_file(cls, mock_error, mock_exit):
        file = join(DATA_DIR, 'fake.ini')
        seq_dbutils.Config().initialize(file)
        mock_error.assert_called_with(f'Config file {file} does not exist. Exiting...')

    @classmethod
    @patch('configparser.ConfigParser.read')
    def test_initialize_ok(cls, mock_read):
        file = join(DATA_DIR, 'test_initialize_ok.ini')
        seq_dbutils.Config().initialize(file)
        mock_read.assert_called_with(file)

    @classmethod
    @patch('configparser.ConfigParser.get')
    def test_get_section_config(cls, mock_get):
        required_section = 'Mock'
        required_key = 'mock'
        seq_dbutils.Config().get_section_config(required_section, required_key)
        mock_get.assert_called_with(required_section, required_key)

    @staticmethod
    def test_get_yn_boolean_y():
        result = seq_dbutils.Config().get_yn_boolean('Y')
        assert result

    @staticmethod
    def test_get_yn_boolean_n():
        result = seq_dbutils.Config().get_yn_boolean('N')
        assert not result

    @staticmethod
    @patch('logging.info')
    @patch('argparse.ArgumentParser')
    @patch('seq_dbutils.config.Config.get_yn_boolean')
    @patch('seq_dbutils.config.Config.get_section_config')
    def test_get_db_config_ok(mock_get_section, mock_get_bool, mock_args, mock_info):
        my_str = 'mock'
        args = seq_dbutils.Args.initialize_args()
        mock_get_section.return_value = my_str
        mock_get_section.encode.return_value = my_str
        mock_get_bool.return_value = True
        user, key, host, db = seq_dbutils.Config.get_db_config(args)
        assert user == my_str
        assert key == b'mock'
        assert host == my_str
        assert db == my_str

    @staticmethod
    @patch('sys.exit')
    @patch('logging.error')
    def test_get_db_config_fail(mock_error, mock_exit):
        parser = argparse.ArgumentParser()
        seq_dbutils.Config.get_db_config(parser)
        mock_error.assert_called_with("'ArgumentParser' object has no attribute 'config'")

    @staticmethod
    @patch('argparse.ArgumentParser')
    @patch('seq_dbutils.config.Config.get_db_config')
    def test_get_script_config(mock_get_db, mock_args):
        my_str = 'mock'
        args = seq_dbutils.Args.initialize_args()
        mock_get_db.return_value = my_str, my_str, my_str, my_str
        seq_dbutils.Config.get_script_config(args)

        user, key, host, db, commit, filepath, reporting = seq_dbutils.Config.get_script_config(args)
        assert user == my_str
        assert key == my_str
        assert host == my_str
        assert db == my_str
        assert not commit
        assert not filepath
        assert not reporting

    @staticmethod
    @patch('argparse.ArgumentParser')
    @patch('seq_dbutils.config.Config.get_db_config')
    def test_get_extract_config(mock_get_db, mock_args):
        my_str = 'mock'
        args = seq_dbutils.Args.initialize_args()
        mock_get_db.return_value = my_str, my_str, my_str, my_str
        seq_dbutils.Config.get_script_config(args)

        user, key, host, db, filepath = seq_dbutils.Config.get_extract_config(args)
        assert user == my_str
        assert key == my_str
        assert host == my_str
        assert db == my_str
        assert not filepath
