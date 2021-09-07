import logging
from os.path import abspath, dirname, join
from unittest import TestCase

import seq_dbutils
from mock import patch

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

THIS_DIR = dirname(abspath(__file__))
DATA_DIR = join(THIS_DIR, 'data')


class ArgsTestClass(TestCase):

    @classmethod
    @patch('argparse.ArgumentParser')
    def test_initialize(cls, mock_args):
        seq_dbutils.Args.initialize_args()
        print(len(mock_args.mock_calls))
        assert len(mock_args.mock_calls) == 2
