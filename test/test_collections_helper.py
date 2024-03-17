from collections_helper import objectset


def test_objectset():
    a = objectset([{1: 'a'}, {1: 'c'}, {2: 'a'}])
    b = objectset([{1: 'c'}, {1: 'd'}, {2: 'a'}])
    c = a | b

    # convert to list tom make assertions
    clist = list(c)
    assert 4 == len(clist)
    assert {1: 'a'} in clist
    assert {1: 'c'} in clist
    assert {1: 'd'} in clist
    assert {2: 'a'} in clist
