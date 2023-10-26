import logging
from os import remove
from os.path import abspath, dirname, join, isfile
from unittest import TestCase

from mock import patch

import seq_dbutils

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

DATA_DIR = join(dirname(abspath(__file__)), 'data')
TEST_BIN_FILE = join(DATA_DIR, 'test_encrypt.bin')

seq_dbutils.encrypt.BIN_FILE = TEST_BIN_FILE


class EncryptTestClass(TestCase):

    @classmethod
    @patch('logging.info')
    @patch('seq_dbutils.encrypt.getpass')
    def test_initialize(cls, mock_pass, mock_info):
        mock_pass.return_value = 'password'
        seq_dbutils.Encrypt.initialize()
        assert isfile(TEST_BIN_FILE)

    @staticmethod
    def tearDown(**kwargs):
        if isfile(TEST_BIN_FILE):
            remove(TEST_BIN_FILE)
