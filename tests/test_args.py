import pytest

from seq_dbutils import Args


@pytest.fixture(scope='session')
def args_fixture():
    return Args.initialize_args()


def test_initialize_args(args_fixture):
    parsed = args_fixture.parse_args(['TEST'])
    config = vars(parsed)['config'][0]
    assert config == 'TEST'
