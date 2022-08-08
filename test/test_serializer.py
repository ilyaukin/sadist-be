from bson import ObjectId

from serializer import serialize


def test_serialize_objectId():
    a = ObjectId("62f18d10d8f9aa7dbdcaf818")
    result = serialize({'_id': a})
    assert {'id': '62f18d10d8f9aa7dbdcaf818'} == result
