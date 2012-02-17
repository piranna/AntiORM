# -*- coding: utf-8 -*-

from os.path  import abspath, dirname, join
from sqlite3  import connect
from unittest import main, TestCase

import sys
sys.path.insert(0, '..')

from antiorm                 import AntiORM


class TestAntiORM(TestCase):
    "Test for the AntiORM base driver"

    def __init__(self, methodName='runTest'):
        TestCase.__init__(self, methodName)

        self.dir_path = join(abspath(dirname(__file__)), 'samples_sql')

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
        self.engine = AntiORM(self.connection, self.dir_path)

    def test_method_notparsed(self):
        with self.assertRaises(AttributeError):
            self.engine.notparsed()

    def test_method_exists(self):
        attr = getattr(self.engine, 'test_statement_INSERT_single', None)
        self.assertNotEqual(attr, None)

    def test_statement_INSERT_single_1(self):
        rowid = self.engine.test_statement_INSERT_single(key="hola")

        self.assertIsNotNone(rowid)

    def test_statement_INSERT_single_2(self):
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM test_table")
        result = cursor.fetchall()

        self.assertEqual(len(result), 1)
        self.assertEqual(len(result[0]), 1)
        self.assertEqual(result[0][0], u'hola')

    def test_statement_INSERT_single_dict_1(self):
        rowid = self.engine.test_statement_INSERT_single({'key': "adios"})

        self.assertIsNotNone(rowid)

    def test_statement_INSERT_single_dict_2(self):
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM test_table")
        result = cursor.fetchall()

        self.assertEqual(len(result), 2)
        self.assertEqual(len(result[1]), 1)
        self.assertEqual(result[1][0], u'adios')

    def test_statement_INSERT_single_list_1(self):
        rowid = self.engine.test_statement_INSERT_single([{'key': 'a'},
                                                          {'key': 'b'}])

        self.assertIsNotNone(rowid)
        self.assertIsNotNone(rowid[0])
        self.assertIsNotNone(rowid[1])

    def test_statement_INSERT_single_list_2(self):
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM test_table")
        result = cursor.fetchall()

        self.assertEqual(len(result), 4)
        self.assertEqual(len(result[2]), 1)
        self.assertEqual(result[2][0], u'a')
        self.assertEqual(len(result[3]), 1)
        self.assertEqual(result[3][0], u'b')


if __name__ == "__main__":
    main()
