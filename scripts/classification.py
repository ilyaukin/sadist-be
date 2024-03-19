import concurrent.futures
import sys
from argparse import ArgumentParser

import app.classification as classification
from classification import classify_cells, AbstractClassifier
from db import conn, dl_master
from mongomoron import query

from detailization import call_get_details_for_all_cols

if __name__ == '__main__':
    argparser = ArgumentParser()
    argparser.add_argument('action', help='[l: learn, c: classify]')
    argparser.add_argument('classifier', help='One of %s' %
                                              list(AbstractClassifier.__map__.keys()))
    argparser.add_argument('text', help='Text to classify', nargs='?')
    argparser.add_argument('--ds', help='DS ID, to classify each '
                                        'cell in the DS')
    argparser.add_argument('--get-details', help='Call appropriate detailizers '
                                                 'after classification finished, '
                                                 'in fact, simulate the processing '
                                                 'of the uploaded DS', action='store_true')

    args = argparser.parse_args()

    classifier = AbstractClassifier.get(args.classifier)
    if args.action == 'l':
        classifier.learn([(record['text'], record['labels'][0])
                          for record in conn.execute(query(dl_master))])
    elif args.action == 'c':
        if args.ds:
            classify_cells(args.ds, classifier)
            if args.get_details:
                ff = call_get_details_for_all_cols(args.ds)
                concurrent.futures.wait(ff)
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
