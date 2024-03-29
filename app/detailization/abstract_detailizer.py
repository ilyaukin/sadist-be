from typing import Dict

from singleton_mixin import SingletonMixin


class AbstractDetailizer(SingletonMixin):

    """
    If more than `threshold` values of a column are
    labelled with one of `labels`, this detailizer
    will be applied to this column
    """
    threshold = 2/3
    labels = []

    def learn(self, **kwargs):
        """
        Learn by sample data, source of data is defined by
        particular classifiers.
        :param kwargs: Defined by particular classifier
        """
        raise NotImplemented()

    def get_details(self, value: str) -> Dict[str, object]:
        """
        Get details for the value of certain class(es).
        E.g. it can be amount and currency for money,
        name and geo coordinates for city etc.
        :param value: Raw value
        :return: Map of {class: details}, where key is a label,
        e.g. "city", and value depends on certain class,
        e.g. {"name": "Raleigh", "coordinates": [ -78.63861, 35.7721 ]}
        """
        raise NotImplemented()
