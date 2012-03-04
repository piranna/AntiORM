# -*- coding: utf-8 -*-

from unittest import main, TestCase

import sys
sys.path.insert(0, '..')

from antiorm.utils import TupleObj_factory, named2pyformat


class FakeCursor:
    def __init__(self, description):
        self.description = description


class TestUtils(TestCase):
    "Test for the AntiORM utility functions"

    fakecursor = FakeCursor(('a', 'b', 'c'))

    def test_TupleObj_factory(self):
        tupleobj = TupleObj_factory(self.fakecursor, ('x', 'y', 'z'))

        self.assertEqual(tupleobj, ('x', 'y', 'z'))

        self.assertEqual(tupleobj[0], 'x')
        self.assertEqual(tupleobj[1], 'y')
        self.assertEqual(tupleobj[2], 'z')

        self.assertEqual(tupleobj.a, 'x')
        self.assertEqual(tupleobj.b, 'y')
        self.assertEqual(tupleobj.c, 'z')

    def test_named2pyformat(self):
        self.assertEqual(named2pyformat(""), "")
        self.assertEqual(named2pyformat("asdf"), "asdf")
        self.assertEqual(named2pyformat("a :formated word"),
                         "a %(formated)s word")


if __name__ == "__main__":
    main()
