import logging
from os.path import abspath, dirname, join
from unittest import TestCase

import seq_dbutils

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

THIS_DIR = dirname(abspath(__file__))
DATA_DIR = join(THIS_DIR, 'data')

seq_dbutils.decrypt.BIN_FILE = join(DATA_DIR, 'test_decrypt.bin')


class DecryptTestClass(TestCase):

    @classmethod
    def test_initialize(cls):
        key = '-zITTaJ8LJ_JFjsa6EG3ASlL-yZsxEYRmCX_wjaW34I='
        result = seq_dbutils.Decrypt.initialize(key)
        assert result == 'password'
