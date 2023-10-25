import logging
from unittest import TestCase

from seq_dbutils import Args

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


class ArgsTestClass(TestCase):

    def setUp(self):
        self.parser = Args.initialize_args()

    def test_initialize_args(self):
        parsed = self.parser.parse_args(['TEST'])
        config = vars(parsed)['config'][0]
        self.assertEqual(config, 'TEST')
