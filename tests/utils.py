# -*- coding: utf-8 -*-

from unittest import main, TestCase

import sys
sys.path.insert(0, '..')

from antiorm.utils import namedtuple_factory, named2pyformat


class FakeCursor:
    def __init__(self, description):
        self.description = description


class TestUtils(TestCase):
    "Test for the AntiORM utility functions"

    fakecursor = FakeCursor(('a', 'b', 'c'))

    def test_namedtuple_factory(self):
        namedtuple = namedtuple_factory(self.fakecursor, ('x', 'y', 'z'))

        self.assertEqual(namedtuple, ('x', 'y', 'z'))
        self.assertEqual(namedtuple._asdict(), {'a': 'x', 'b': 'y', 'c': 'z'})

        self.assertEqual(namedtuple[0], 'x')
        self.assertEqual(namedtuple[1], 'y')
        self.assertEqual(namedtuple[2], 'z')

        self.assertEqual(namedtuple.a, 'x')
        self.assertEqual(namedtuple.b, 'y')
        self.assertEqual(namedtuple.c, 'z')

    def test_named2pyformat(self):
        self.assertEqual(named2pyformat(""), "")
        self.assertEqual(named2pyformat("asdf"), "asdf")
        self.assertEqual(named2pyformat("a :formated word"),
                         "a %(formated)s word")


if __name__ == "__main__":
    main()
