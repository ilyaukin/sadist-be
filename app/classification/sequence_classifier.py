from typing import Iterable, Tuple, Optional, List

from classification import AbstractClassifier
from detailization import SequenceDetailizer

from app import logger


@AbstractClassifier.sub
class SequenceClassifier(AbstractClassifier):
    """
    Classifier by sequences of tokens.
    (1) A string is split to tokens and labelled (by `SequenceDetailizer`),
    and (2) sequence is matched to the whole string label. Either
    (2.a) another neural network is used, or (2.b) just ad-hoc
    algorithm based on count or frequency of certain type of tokens.
    """

    @property
    def detailizer(self) -> SequenceDetailizer:
        return SequenceDetailizer.get()

    def learn(self, i: Iterable[Tuple[str, str]]):
        # let stick with option 2.a so far, so no special learning needed
        pass

    def classify(self, s: str) -> str:
        sequence = self.detailizer.get_details(s).get('sequence')
        if not sequence:
            logger.warn('Something went wrong, detailizer did not return sequence')
            return None

        if sum(1 for item in sequence if
               item['label'] in ['year', 'month', 'month[name]', 'day',
                                 'hour', 'minute', 'second']) >= 2:
            return 'datetime'
        elif sum(1 for item in sequence if
                 item['label'] in ['city']) >= 1:
            return 'city'
        elif sum(1 for item in sequence if
                 item['label'] in ['country', 'state', 'province']) >= 1:
            return 'country'
        elif sum(1 for item in sequence if
                 item['label'] in ['currency-code', 'currency-sign',
                                   'currency-name']) >= 1 and \
                sum(1 for item in sequence if
                    item['label'] in ['number']) >= 1:
            return 'money'
        elif sum(1 for item in sequence if item['label'] in ['number']):
            return 'number'
        elif sum(1 for item in sequence if
                 item['label'] in ['gender[male]', 'gender[female]', 'gender[non-binary]']) >= 1:
            return 'gender'
        elif sum(1 for item in sequence if item['label'] in ['word']) >= 1:
            return 'phrase'
        else:
            return 'trash'
