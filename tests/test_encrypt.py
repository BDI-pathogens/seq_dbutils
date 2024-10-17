from os.path import abspath, dirname, join

from mock import patch, mock_open

import seq_dbutils

DATA_DIR = join(dirname(abspath(__file__)), 'data')
TEST_BIN_FILE = join(DATA_DIR, 'test_encrypt.bin')

seq_dbutils.encrypt.BIN_FILE = TEST_BIN_FILE


def test_initialize():
    key = b'ZmDfcTF7_60GrrY167zsiPd67pEvs0aGOv2oasOM1Pg='
    with patch('logging.info'):
        with patch('seq_dbutils.encrypt.getpass', return_value='password'):
            with patch('cryptography.fernet.Fernet.generate_key', return_value=key):
                with patch('builtins.open', mock_open()) as mock_file:
                    seq_dbutils.Encrypt.initialize()
                    mock_file.assert_called_once()
