# -*- coding: utf-8 -*-

import unittest
from base import AntiORM
from utils import named2pyformat


class Test_S2SF(unittest.TestCase):

    def test_S2SF(self):
        self.assertEqual(named2pyformat(""), "")
        self.assertEqual(named2pyformat("asdf"), "asdf")
        self.assertEqual(named2pyformat("a :formated word"), "a %(formated)s word")


class Test_AntiORM(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_S2SF(self):
        self.assertEqual(named2pyformat(""), "")
        self.assertEqual(named2pyformat("asdf"), "asdf")
        self.assertEqual(named2pyformat("a :formated word"), "a %(formated)s word")


class Test_Sqlite(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_S2SF(self):
        self.assertEqual(named2pyformat(""), "")
        self.assertEqual(named2pyformat("asdf"), "asdf")
        self.assertEqual(named2pyformat("a :formated word"), "a %(formated)s word")


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
