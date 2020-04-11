from typing import Dict


class AbstractDetailizer(object):

    """
    If more than `threshold` values of a column are
    labelled with one of `labels`, this detailizer
    will be applied to this column
    """
    threshold = 2/3
    labels = []

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
