import re
from typing import Any, Hashable, Iterable, Dict, Union, List

from mongomoron import query

from db import conn, dl_currency, currency_list
from detailization import AbstractDetailizer, NumberDetailizer, SequenceDetailizer
from detailization.bow_detailizer import BowDetailizer


@AbstractDetailizer.sub
class MoneyDetailizer(BowDetailizer):
    """
    Detailizer for money, result is in format {"number": <amount>,
    "currency": <currency code>}
    """

    labels = ['money']

    def __init__(self):
        super().__init__('currency')
        # dict code -> name, actually we need only code so far,
        # but let it be
        self.currencies = dict((currency['_id'], currency['name']) for
                               currency in conn.execute(query(currency_list)))

    @property
    def sequence_detailizer(self) -> SequenceDetailizer:
        return SequenceDetailizer.get()

    @property
    def number_detailizer(self) -> NumberDetailizer:
        return NumberDetailizer.get()

    def get_details(self, value: str) -> Dict[str, object]:
        # get number
        details = self.number_detailizer.get_details(value)

        # here we calculate the sequence second time, already being
        # calculated in number_detailizer, but let skip optimization for now
        sequence = self.sequence_detailizer.get_details(value).get('sequence')

        # get currency with help of neural network
        currency = self._get_currency(sequence)
        if currency:
            details.update({'currency': currency})

        return details

    def _get_bow(self, s: str) -> List[str]:
        words = re.split(r'([^\w])', s)
        return [self._normalize(w) for w in words if w]

    def _ttok(self, t: Any) -> Hashable:
        return t

    def _ktot(self, k: Hashable) -> Any:
        return k

    def _get_samples(self) -> Iterable[dict]:
        return conn.execute(query(dl_currency))

    def _fallback(self, s: str):
        return None

    def _get_currency(self, sequence) -> Union[str, None]:
        currency_code = None
        currency_str = ''

        for item in sequence:
            label = item['label']
            token = item['token']
            if label == 'currency-code':
                currency_code = token
                break
            elif label == 'currency-sign' or label == 'currency-name':
                currency_str += token
            elif currency_str:
                break

        if currency_code and currency_code in self.currencies:
            return currency_code
        elif currency_str:
            return super().get_details(currency_str)
        else:
            return None
