import pytest

from seq_dbutils import Args


@pytest.fixture()
def args():
    return Args.initialize_args()


def test_initialize_args(args):
    parsed = args.parse_args(['TEST'])
    config = vars(parsed)['config'][0]
    assert config == 'TEST'
