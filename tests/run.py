# -*- coding: utf-8 -*-

import sys
sys.path.insert(0, '..')

import unittest
from antiorm import AntiORM
from antiorm.utils import named2pyformat

class MainTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import sqlite3
        cls.connection = sqlite3.connect(":memory:")
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
    unittest.main()
