import argparse
from build import worker as build
from analyse import worker as analyse


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--build", action='store_true', help="build pubmed articles vector index", required=False)
    parser.add_argument("--find", nargs="+", help="find article by keywords in index", required=False)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.find and args.build:
        print("Only one command can be passed")
        exit(0)
    if args.find:
        keywords = " ".join(args.find)
        analyse(keywords)
    if args.build:
        build()