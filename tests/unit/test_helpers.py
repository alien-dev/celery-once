# -*- coding: utf-8 -*-
from celery_once.helpers import queue_once_key, kwargs_to_list, force_string

import pytest
import six


def test_force_string_1():
    assert force_string('a') == 'a'


def test_force_string_2():
    assert force_string(u'a') == 'a'


def test_force_string_3():
    assert force_string('é') == 'é'


def test_force_string_4():
    assert force_string(u'é') == 'é'


def test_kwargs_to_list_empty():
    keys = kwargs_to_list({})
    assert keys == []


def test_kwargs_to_list_1():
    keys = kwargs_to_list({'int': 1})
    assert keys == ["int-1"]


def test_kwargs_to_list_2():
    keys = kwargs_to_list({'int': 1, 'boolean': True})
    assert keys == ["boolean-True", "int-1"]


def test_kwargs_to_list_3():
    keys = kwargs_to_list({'int': 1, 'boolean': True, 'str': "abc"})
    assert keys == ["boolean-True", "int-1", "str-abc"]


def test_kwargs_to_list_4():
    keys = kwargs_to_list(
        {'int': 1, 'boolean': True, 'str': 'abc', 'list': [1, '2']})
    assert keys == ["boolean-True", "int-1", "list-[1, '2']", "str-abc"]


@pytest.mark.skipif(six.PY3, reason='requires python 2')
def test_kwargs_to_list_5():
    keys = kwargs_to_list(
        {'a': {u'é': 'c'}, 'b': [u'a', 'é'], u'c': 1, 'd': 'é', 'e': u'é'})
    assert keys == [
        "a-{'\\xc3\\xa9': 'c'}",
        "b-['a', '\\xc3\\xa9']",
        "c-1",
        "d-\xc3\xa9",
        "e-\xc3\xa9",
    ]


@pytest.mark.skipif(six.PY2, reason='requires python 3')
def test_kwargs_to_list_6():
    keys = kwargs_to_list(
        {'a': {u'é': 'c'}, 'b': [u'a', 'é'], u'c': 1, 'd': 'é', 'e': u'é'})
    assert keys == ["a-{'é': 'c'}", "b-['a', 'é']", "c-1", "d-é", 'e-é']


def test_queue_once_key():
    key = queue_once_key("example", {})
    assert key == "qo_example"


def test_queue_once_key_kwargs():
    key = queue_once_key("example", {'pk': 10})
    assert key == "qo_example_pk-10"


def test_queue_once_key_kwargs_restrict_keys():
    key = queue_once_key("example", {'pk': 10, 'id': 10}, restrict_to=['pk'])
    assert key == "qo_example_pk-10"
