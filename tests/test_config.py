from os.path import abspath, dirname, join

from mock import patch

from seq_dbutils import Config

DATA_DIR = join(dirname(abspath(__file__)), 'data')


@patch('logging.error')
@patch('sys.exit')
def test_initialize_no_file(mock_exit, mock_error):
    file = join(DATA_DIR, 'fake.ini')
    Config.initialize(file)
    mock_exit.assert_called_once()


@patch('logging.info')
@patch('configparser.ConfigParser.read')
def test_initialize_ok(mock_read, mock_info):
    file = join(DATA_DIR, 'test_initialize_ok.ini')
    Config.initialize(file)
    mock_read.assert_called_once_with(file)


@patch('logging.info')
@patch('configparser.ConfigParser.get')
def test_get_section_config(mock_get, mock_info):
    required_section = 'Mock'
    required_key = 'mock'
    Config.get_section_config(required_section, required_key)
    mock_get.assert_called_once_with(required_section, required_key)


@patch('logging.info')
@patch('seq_dbutils.config.Config.get_section_config')
def test_get_db_config_ok(mock_get_section, mock_info):
    my_str = 'mock'
    args = 'TEST'
    mock_get_section.return_value = my_str
    mock_get_section.encode.return_value = my_str
    user, key, host, db = Config.get_db_config(args)
    assert mock_get_section.call_count == 4
    assert user == my_str
    assert key == b'mock'
    assert host == my_str
    assert db == my_str


@patch('logging.error')
@patch('logging.info')
@patch('sys.exit')
def test_get_db_config_fail(mock_exit, mock_info, mock_error):
    Config.get_db_config('error')
    mock_exit.assert_called_once()
