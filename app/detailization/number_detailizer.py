import re
from typing import Dict, Union

from detailization import AbstractDetailizer, SequenceDetailizer

from app import logger


@AbstractDetailizer.sub
class NumberDetailizer(AbstractDetailizer):
    """
    A detailizer for numbers, result is in format {"number": <number>}
    """

    labels = ['number']

    @property
    def sequence_detailizer(self) -> SequenceDetailizer:
        return SequenceDetailizer.get()

    def learn(self, **kwargs):
        pass

    def get_details(self, value: str) -> Dict[str, object]:
        sequence = self.sequence_detailizer.get_details(value).get('sequence')

        n = self._get_number(sequence)

        if n is not None:
            return {'number': n}
        return {}

    def _get_number(self, sequence) -> Union[float, int, None]:
        number_str = ''
        is_decimal_point_used = False
        for item in sequence:
            label = item['label']
            token = item['token']

            if label == 'operator[minus]' and not number_str:
                number_str = '-'
            elif label == 'number':
                number_str += token
            elif label == 'decimal-point' and not is_decimal_point_used:
                number_str += '.'
                is_decimal_point_used = True
            elif label == 'separator':
                pass
            elif number_str:
                break
        number_str = re.sub(r'[kK]', '000', number_str)
        number_str = re.sub(r'[mM]', '000000', number_str)
        number_str = re.sub(r'[bB]', '000000000', number_str)

        if not number_str:
            return None

        if '.' in number_str:
            number_type = float
        else:
            number_type = int

        try:
            return number_type(number_str)
        except Exception as e:
            logger.warn(f"{number_type.__name__}({repr(number_str)}) threw an error: {e}")
            return None
