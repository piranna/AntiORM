# -*- coding: utf-8 -*-

from unittest import main, TestCase

import sys
sys.path.insert(0, '..')

from antiorm.utils           import DictObj_factory, named2pyformat


class TestUtils(TestCase):
    "Test for the AntiORM utility functions"
    def test_DictObj_factory(self):
        class FakeCursor:
            def __init__(self, description):
                self.description = description

        dictobj = DictObj_factory(FakeCursor(('a', 'b', 'c')), ('x', 'y', 'z'))

        self.assertEqual(dictobj, {'a': 'x', 'b': 'y', 'c': 'z'})

    def test_named2pyformat(self):
        self.assertEqual(named2pyformat(""), "")
        self.assertEqual(named2pyformat("asdf"), "asdf")
        self.assertEqual(named2pyformat("a :formated word"),
                         "a %(formated)s word")


if __name__ == "__main__":
    main()
