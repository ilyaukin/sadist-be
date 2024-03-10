class DO(object):
    """
    Base class for data object.
    Serializes via '__dict__',
    objects of other classes serialize by `str()`
    """
    pass


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
    if isinstance(v, DO):
        # dir() instead of __dict__ to keep class ("constant") attributes
        return serialize(dict((k, getattr(v, k))
                              for k in dir(v) if not k.startswith('__')
                              and getattr(v, k, None) is not None))
    if hasattr(v, '__iter__'):
        return [serialize_value(i) for i in v]
    return str(v)


def serialize(record: dict) -> dict:
    return dict((serialize_key(k), serialize_value(v))
                for k, v in record.items())
