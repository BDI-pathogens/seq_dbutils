from argparse import ArgumentParser


class Args:

    @classmethod
    def initialize_args(cls):
        parser = ArgumentParser()
        parser.add_argument('config', type=str, nargs=1, help='the relevant config section, e.g. LOCAL')
        return parser
