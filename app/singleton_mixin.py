from typing import Union, Callable, Dict, Any, Iterable


class SingletonMixin(object):
    """
    This is a pattern which, I believe, is named "absract singleton
    factory" or something. It is responsible for making classes
    singletons, initializing them on demand, querying by type,
    name, or custom condition.
    """
    __key__: str = None
    __instance__: 'SingletonMixin' = None
    __map__: Dict[str, type['SingletonMixin']] = {}

    @classmethod
    def sub(cls, cls_or_key: Union[type['SingletonMixin'], str], key: str = None) \
            -> type['SingletonMixin']:
        """
        Mark a class as a sub-type of given cls.
        I.e. given class `A` inherited from `SingletonMixin`,
        any class decorated with `A.sub` will be in its 'family'
        and can be queried by `A.get()`
        @param cls_or_key: Can directly pass a class, or first pass a key,
        to make a second-order function that can be used as a decorator
        @param key: Key of the type, by default its __name__
        @return: Passed class, or a function that can be after that applied
        to a class
        """
        if isinstance(cls_or_key, str):
            return lambda cls1, key1=cls_or_key: cls.sub(cls1, key1)
        elif isinstance(cls_or_key, type) and issubclass(cls_or_key, SingletonMixin):
            # create own map for cls
            if cls.__map__ is SingletonMixin.__map__ and cls is not SingletonMixin:
                cls.__map__ = {}
            # set a key and put the subclass to cls's map
            cls_or_key.__key__ = key or cls_or_key.__name__
            cls.__map__[cls_or_key.__key__] = cls_or_key
            # return modified class as is
            return cls_or_key
        else:
            raise TypeError(f'cls_or_key must be Type[SingletonMixin] | str, '
                            f'got {type(cls_or_key).__name__} instead.')

    @classmethod
    def get(cls, q: Union[type['SingletonMixin'], str, Callable] = None) \
            -> Union['SingletonMixin', None]:
        """
        Query either by type of singleton, it's name (or more generally, key),
        or a custom condition
        @param q: can be:
        - type of singleton; in this case, singleton of this type
        will be returned. By default, the current cls
        - key, in this case, singleton by given key will be returned
        - callable, in this case, the first singleton, for which
        it returns truthy value, will be returned.

        If no singleton matches the condition None will be returned
        @return: Singleton instance of the queried class.
        """
        if q is None:
            q = cls
        elif isinstance(q, type) and issubclass(q, SingletonMixin):
            pass
        elif isinstance(q, str):
            q = cls.__map__.get(q)
        elif hasattr(q, '__call__'):
            q = next((cls1 for cls1 in cls.__map__.values() if
                      issubclass(cls1, cls) and
                      q(cls1)), None)
        else:
            raise TypeError(f'q must be Type[SingletonMixin] | str | Callable,'
                            f' got {type(q).__name__} instead.')

        if not q:
            return None
        if not q.__instance__:
            q.__instance__ = q()
        return q.__instance__

    @classmethod
    def get_all(cls, q: Callable = None) -> Iterable['SingletonMixin']:
        """
        Query all singletons by a certain condition.
        By default, all
        @param q: Callable condition
        @return: Iterable of singletons
        """
        if not q:
            q = lambda cls1: True
        cls_list = [cls1 for cls1 in cls.__map__.values() if
                    issubclass(cls1, cls) and
                    q(cls1)]
        for cls1 in cls_list:
            if not cls1.__instance__:
                cls1.__instance__ = cls1()
            yield cls1.__instance__
