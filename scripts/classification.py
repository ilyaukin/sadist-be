import inspect
from argparse import ArgumentParser

import app.classification as classification
import sys
from app import _db
from classification import classify_cells


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
        dl_master = _db()['dl_master']
        classifier.learn([(record['text'], record['labels'][0])
                          for record in dl_master.find()])
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
