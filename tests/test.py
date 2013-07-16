#!/usr/bin/env python
import sys
import argparse
from nose.core import run


def main():
    parser = argparse.ArgumentParser()

    known_args, remaining_args = parser.parse_known_args()

    if run(argv=remaining_args):
        return 0
    else:
        return 1


if __name__ == "__main__":
    sys.exit(main())
