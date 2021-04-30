import inspect
import sys
from argparse import ArgumentParser

import app.detailization as detailization
from detailization import get_details_for_cells
from detailization.abstract_detailizer import AbstractDetailizer


def help_detailizer_names():
    for m in inspect.getmembers(detailization, inspect.isclass):
        yield m[0]


if __name__ == '__main__':
    argparser = ArgumentParser()
    argparser.add_argument('detailizer', help='One of %s' %
                                              list(help_detailizer_names()))
    argparser.add_argument('action', help='[l]earn | get [d]etails')
    argparser.add_argument('text', help='Text to get details for', nargs='?')
    argparser.add_argument('--ds', help='DS ID, to get details for'
                                        ' each cell in some column of DS')
    argparser.add_argument('--col', help='Column name')

    args = argparser.parse_args()

    detailizer: AbstractDetailizer = getattr(detailization, args.detailizer)()
    if args.action == 'l':
        detailizer.learn()
    elif args.action == 'd':
        if args.ds:
            get_details_for_cells(args.ds, args.col, detailizer)
            exit(0)
        text = args.text
        if not text:
            print("Enter text to get details for:")
            text = sys.stdin.readline()
            text = text.strip()
        print(detailizer.get_details(text))
    else:
        print("Bad action: " + args.action)
        exit(1)
