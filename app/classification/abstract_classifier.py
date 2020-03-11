from typing import Iterable, Tuple


class AbstractClassifier(object):
    """
    Base interface to implement classifiers.
    Classification here means assigning one of labels to a text.
    E.g. "city", "money", determining the particular city or amount
    of money is out of scope of this module.
    """

    def learn(self, i: Iterable[Tuple[str, str]]):
        """
        Update the Model by labelled data.
        :param i: iterable of tuples text, label
        :return: nothing
        """
        raise NotImplemented()

    def classify(self, s: str) -> str:
        """
        Classify text using the Model
        :param s: text
        :return: label
        """
        raise NotImplemented()
