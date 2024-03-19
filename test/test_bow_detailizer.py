from typing import Hashable, Any, Iterable

import app.detailization as detailization
from detailization.bow_detailizer import BowDetailizer


class TinyStupidBowDetailizer(BowDetailizer):

    def __init__(self):
        super().__init__('test')

    def _ttok(self, t: Any) -> Hashable:
        return t

    def _ktot(self, k: Hashable) -> Any:
        return k

    def _get_samples(self) -> Iterable[dict]:
        return [
            {'text': 'poopa', 'labels': ['poop']},
            {'text': 'loopa', 'labels': ['loop']},
            {'text': 'loopa doopa', 'labels': ['loop']},
        ]


def test_bow_detailizer():
    detailizer = TinyStupidBowDetailizer()
    detailizer.learn()

    assert 'poop' == detailizer.get_details('poopa')
    assert 'loop' == detailizer.get_details('loopa')
    assert 'loop' == detailizer.get_details('loopa moopa')
