def serialize_key(k):
    if k == '_id':
        return 'id'
    if isinstance(k, str):
        return k
    return str(k)


def serialize_value(v):
    # todo do cyclic reference check
    if isinstance(v, dict):
        return serialize(v)
    if isinstance(v, (int, float, str, bool)) or v is None:
        return v
    if hasattr(v, '__iter__'):
        return [serialize_value(i) for i in v]
    return str(v)


def serialize(record: dict) -> dict:
    return dict((serialize_key(k), serialize_value(v))
                for k, v in record.items())
