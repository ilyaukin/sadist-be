# set up on a root level, for the sake of simplicity
from singleton_mixin import SingletonMixin


class Foo(SingletonMixin):
    specific = 0


@Foo.sub
class FooBar(Foo):
    specific = 10


@Foo.sub('buzz')
class FooBuzz(Foo):
    specific = 20


def test_singleton_mixin_get():
    instance0 = FooBar.get()
    instance1 = Foo.get(FooBar)
    instance2 = Foo.get('FooBar')
    instance3 = Foo.get('buzz')
    instance4 = Foo.get(lambda cls: cls.specific == 10)
    instance5 = Foo.get(lambda cls: cls.specific == 20)
    instance6 = Foo.get(lambda cls: cls.specific == 30)

    assert isinstance(instance1, FooBar)
    assert instance0 is instance1
    assert instance2 is instance1
    assert isinstance(instance3, FooBuzz)
    assert instance4 is instance1
    assert instance5 is instance3
    assert instance6 is None


def test_singleton_mixin_get_all():
    i_list = list(Foo.get_all(lambda cls: cls.specific > 1))
    assert len(i_list) == 2
    assert FooBar.get() in i_list
    assert FooBuzz.get() in i_list
