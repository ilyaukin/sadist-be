import inspect
import sys
from argparse import ArgumentParser

import app.classification as classification
from classification import classify_cells
from db import conn, dl_master
from mongomoron import query


def help_classifier_names():
    for m in inspect.getmembers(classification, inspect.isclass):
        yield m[0]

if __name__ == '__main__':
    argparser = ArgumentParser()
    argparser.add_argument('action', help='[l: learn, c: classify]')
    argparser.add_argument('classifier', help='One of %s' %
                                              list(help_classifier_names()))
    argparser.add_argument('text', help='Text to classify', nargs='?')
    argparser.add_argument('--ds', help='DS ID, to classify each '
                                        'cell in the DS')

    args = argparser.parse_args()

    classifier = getattr(classification, args.classifier)()
    if args.action == 'l':
        classifier.learn([(record['text'], record['labels'][0])
                          for record in conn.execute(query(dl_master))])
    elif args.action == 'c':
        if args.ds:
            classify_cells(args.ds, classifier)
            exit(0)
        text = args.text
        if not text:
            print('Enter text to classify:')
            text = sys.stdin.readline()
            text = text.strip()
        print(classifier.classify(text))
    else:
        print('Bad action: %s' % args.action)
        exit(1)
