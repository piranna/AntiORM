# -*- coding: utf-8 -*-

import sys
sys.path.insert(0, '..')

from sqlite3  import connect
from unittest import main, TestCase

from antiorm                 import AntiORM
from antiorm.backends.sqlite import Sqlite
from antiorm.utils           import DictObj_factory, named2pyformat


class MainTestCase(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.connection = connect(":memory:")
        cursor = cls.connection.cursor()
        cursor.execute("CREATE TABLE test_table (key TEXT);")
        cursor.close()
        cls.connection.commit()

    @classmethod
    def tearDownClass(cls):
        cls.connection.close()

    def setUp(self):
        self.engine = AntiORM(self.connection, "./samples_sql")

    def test_sample_method_exists(self):
        self.engine.test_simple_insert(key="hola")
        attr = getattr(self.engine, 'test_simple_insert', None)
        self.assertNotEqual(attr, None)

        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM test_table")
        result = cursor.fetchall()

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0], u'hola')


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


class TestAntiORM(TestCase):
    "Test for the AntiORM base driver"
    @classmethod
    def setUpClass(cls):
        cls.connection = connect(":memory:")
        cursor = cls.connection.cursor()
        cursor.execute("CREATE TABLE test_table (key TEXT);")
        cursor.close()
        cls.connection.commit()

    @classmethod
    def tearDownClass(cls):
        cls.connection.close()

    def setUp(self):
        self.engine = AntiORM(self.connection, "./samples_sql")

    def test_notparsed(self):
        with self.assertRaises(AttributeError):
            self.engine.notparsed()


class TestSqlite(TestAntiORM):
    "Test for the AntiORM SQLite driver"
    def setUp(self):
        self.engine = Sqlite(self.connection, "./samples_sql")


if __name__ == "__main__":
    main()
