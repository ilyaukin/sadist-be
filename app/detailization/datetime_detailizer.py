import datetime
from typing import Dict

from detailization import AbstractDetailizer, SequenceDetailizer

from app import logger


@AbstractDetailizer.sub
class DatetimeDetailizer(AbstractDetailizer):
    """
    A detailizer for date & time. The resulting details format is
    {"datetime": {"timestamp": <UNIX timestamp>, "offset": <optional, format TBD>}}
    The behaviour is inherited from python datetime lib.
    For example, missing date is filled by "01/01/1900" etc.
    """

    labels = ['datetime']

    @property
    def sequence_detailizer(self) -> SequenceDetailizer:
        return SequenceDetailizer.get()

    def learn(self, **kwargs):
        pass

    def get_details(self, value: str) -> Dict[str, object]:
        sequence = self.sequence_detailizer.get_details(value).get('sequence')

        format_str = ''
        value_str  = ''

        for item in sequence:
            label = item['label']
            token = item['token']
            if label == 'year':
                if len(token) <= 2:
                    format_str += '%y'
                    value_str += token
                else:
                    format_str += '%Y'
                    value_str += token
            elif label == 'month':
                format_str += '%m'
                value_str += token
            elif label == 'month[name]':
                # here we can mess up with locales,
                # but let it go for now
                if len(token) <= 3:
                    format_str += '%b'
                    value_str += token
                else:
                    format_str += '%B'
                    value_str += token
            elif label == 'day':
                format_str += '%d'
                value_str += token
            elif label == 'hour':
                if any(item1 for item1 in sequence if item1['token'].lower() in ['am', 'pm']):
                    format_str += '%I'
                else:
                    format_str += '%H'
                value_str += token
            elif label == 'minute':
                format_str += '%M'
                value_str += token
            elif label == 'second':
                format_str += '%S'
                value_str += token
            elif label == 'fraction':
                format_str += '%f'
                value_str += token
            elif token.lower() in ['am', 'pm']:
                format_str += '%p'
                value_str += token
            elif label == 'timezone':
                # TODO, not implemented
                pass
            elif format_str and (label == 'whitespace' or label == 'separator'):
                format_str += token.replace('%', '%%')
                value_str += token
            elif format_str:
                break

        value = None
        try:
            value = datetime.datetime.strptime(value_str, format_str)
        except Exception as e:
            logger.warn(f'datetime.strptime({repr(value_str)}, {repr(format_str)}) threw'
                        f' an error: {e}')

        if value:
            return {'datetime': {'timestamp': value.timestamp()}}

        return {}
