from typing import Dict, Union

from detailization import AbstractDetailizer, SequenceDetailizer


@AbstractDetailizer.sub
class GenderDetailizer(AbstractDetailizer):
    """
    A detailizer for gender. Result is in the format
    {"gender": "male" | "female" | "non-binary"}
    """

    labels = ['gender']

    @property
    def sequence_detailizer(self) -> SequenceDetailizer:
        return SequenceDetailizer.get()

    def get_details(self, value: str) -> Dict[str, object]:
        sequence = self.sequence_detailizer.get_details(value).get('sequence')

        gender = self._get_gender(sequence)
        if gender:
            return {'gender': gender}
        return {}

    def _get_gender(self, sequence) -> Union[str, None]:
        for item in sequence:
            label = item['label']
            if label == 'gender[male]':
                return 'male'
            elif label == 'gender[female]':
                return 'female'
            elif label == 'gender[non-binary]':
                return 'non-binary'

        return None
