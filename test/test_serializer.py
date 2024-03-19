from dataclasses import dataclass
from typing import Optional

from bson import ObjectId

from app.serializer import serialize, DO


def test_serialize_objectId():
    a = ObjectId("62f18d10d8f9aa7dbdcaf818")
    result = serialize({'_id': a})
    assert {'id': '62f18d10d8f9aa7dbdcaf818'} == result


def test_serialize_do():
    @dataclass
    class DO0(DO):
        a = 'constant'
        b: str
        b1: Optional[str] = None
        c: str = 'default'

    @dataclass
    class DO1(DO):
        e: int
        f: str

    @dataclass
    class DO2(DO1):
        g: str

    a = DO0(b='b')
    a1 = DO0(b='b', b1='a')
    a2 = DO2(e=1, f='foo', g='goo')

    result = serialize({'a': a, 'a1': a1, 'a2': a2})

    assert {'a': 'constant', 'b': 'b', 'c': 'default'} == result['a']
    assert {'a': 'constant', 'b': 'b', 'b1': 'a', 'c': 'default'} == result['a1']
    assert {'e': 1, 'f': 'foo', 'g': 'goo'} == result['a2']
