from configparser import NoOptionError
from os.path import abspath, dirname, join

import pytest
from mock import patch

from seq_dbutils import Config

DATA_DIR = join(dirname(abspath(__file__)), 'data')


def test_initialize_no_file():
    with patch('configparser.ConfigParser.read', side_effect=FileNotFoundError()):
        with pytest.raises(FileNotFoundError):
            Config.initialize('fake.ini')


def test_initialize_exception():
    with patch('configparser.ConfigParser.read', side_effect=Exception()):
        with pytest.raises(Exception):
            Config.initialize('fake.ini')


def test_initialize_ok():
    data = 'file_contents'
    with patch('configparser.ConfigParser.read', return_value=data):
        result = Config.initialize('fake.ini')
        assert result == data


def test_get_section_config_no_option():
    required_section = 'SOME_SECTION'
    required_key = 'some_key'
    with patch('configparser.ConfigParser.get', side_effect=NoOptionError(required_section, required_key)):
        with pytest.raises(NoOptionError):
            Config.get_section_config(required_section, required_key)


def test_get_section_config_exception():
    required_section = 'SOME_SECTION'
    required_key = 'some_key'
    with patch('configparser.ConfigParser.get', side_effect=Exception()):
        with pytest.raises(Exception):
            Config.get_section_config(required_section, required_key)


def test_get_section_config_ok():
    required_section = 'SOME_SECTION'
    required_key = 'some_key'
    data = 'some_config'
    with patch('configparser.ConfigParser.get', return_value=data):
        result = Config.get_section_config(required_section, required_key)
        assert result == data


def test_get_db_config_ok():
    result = 'some_config'
    with patch('logging.info'):
        with patch('seq_dbutils.config.Config.get_section_config') as mock_get_section:
            mock_get_section.return_value = result
            mock_get_section.encode.return_value = result
            user, key, host, db = Config.get_db_config('SOME_SECTION')
            assert mock_get_section.call_count == 4
            assert user == result
            assert key == b'some_config'
            assert host == result
            assert db == result
