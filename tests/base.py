# -*- coding: utf-8 -*-

from os.path  import abspath, dirname, join
from unittest import main, TestCase

import sys
sys.path.insert(0, '..')


class Basic:
    def test_method_notparsed(self):
        with self.assertRaises(AttributeError):
            self.engine.notparsed()

    def test_method_exists(self):
        attr = getattr(self.engine, 'test_statement_INSERT_single', None)
        self.assertNotEqual(attr, None)


class StatementINSERTSingle:
    def setUp(self):
        cursor = self.connection.cursor()
        cursor.execute("CREATE TABLE test_statement_INSERT_single (key TEXT);")
        cursor.close()
        self.connection.commit()

    def test_statement_INSERT_single_1(self):
        rowid = self.engine.test_statement_INSERT_single(key="hola")

        self.assertIsNotNone(rowid)

    def test_statement_INSERT_single_2(self):
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM test_statement_INSERT_single")
        result = cursor.fetchall()

        self.assertEqual(len(result), 1)
        self.assertEqual(len(result[0]), 1)
        self.assertEqual(result[0][0], u'hola')

    def test_statement_INSERT_single_dict_1(self):
        rowid = self.engine.test_statement_INSERT_single({'key': "adios"})

        self.assertIsNotNone(rowid)

    def test_statement_INSERT_single_dict_2(self):
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM test_statement_INSERT_single")
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
        cursor.execute("SELECT * FROM test_statement_INSERT_single")
        result = cursor.fetchall()

        self.assertEqual(len(result), 4)
        self.assertEqual(len(result[2]), 1)
        self.assertEqual(result[2][0], u'a')
        self.assertEqual(len(result[3]), 1)
        self.assertEqual(result[3][0], u'b')


class StatementINSERTMultiple:
    def setUp(self):
        cursor = self.connection.cursor()
        cursor.execute("""CREATE TABLE test_statement_INSERT_multiple
        (
            key   TEXT,
            value TEXT NULL
        );""")
        cursor.close()
        self.connection.commit()

    def test_statement_INSERT_multiple_1(self):
        rowid = self.engine.test_statement_INSERT_multiple(key='a')

        self.assertIsNotNone(rowid)

    def test_statement_INSERT_multiple_2(self):
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM test_statement_INSERT_multiple")
        result = cursor.fetchall()

        self.assertEqual(len(result), 1)
        self.assertEqual(len(result[0]), 2)
        self.assertEqual(result[0][0], u'a')
        self.assertEqual(result[0][0], result[0][1])

    def test_statement_INSERT_multiple_dict_1(self):
        rowid = self.engine.test_statement_INSERT_multiple({'key': 'b'})

        self.assertIsNotNone(rowid)

    def test_statement_INSERT_multiple_dict_2(self):
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM test_statement_INSERT_multiple")
        result = cursor.fetchall()

        self.assertEqual(len(result), 2)

        self.assertEqual(len(result[1]), 2)
        self.assertEqual(result[1][0], u'b')
        self.assertEqual(result[1][0], result[1][1])

    def test_statement_INSERT_multiple_list_1(self):
        rowid = self.engine.test_statement_INSERT_multiple([{'key': 'c'},
                                                            {'key': 'd'}])

        self.assertIsNotNone(rowid)
        self.assertIsNotNone(rowid[0])
        self.assertIsNotNone(rowid[1])

    def test_statement_INSERT_multiple_list_2(self):
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM test_statement_INSERT_multiple")
        result = cursor.fetchall()

        self.assertEqual(len(result), 4)

        self.assertEqual(len(result[2]), 2)
        self.assertEqual(result[2][0], u'c')
        self.assertEqual(result[2][0], result[2][1])

        self.assertEqual(len(result[3]), 2)
        self.assertEqual(result[3][0], u'd')
        self.assertEqual(result[3][0], result[3][1])


class OneStatement_value:
    def setUp(self):
        cursor = self.connection.cursor()
        cursor.execute("CREATE TABLE test_one_statement_value (key TEXT);")
        cursor.close()
        self.connection.commit()

    def test_one_statement_value(self):
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO test_one_statement_value(key) VALUES('a')")

        result = self.engine.test_one_statement_value()

        self.assertEqual(result, u'a')


class OneStatement_register:
    def setUp(self):
        cursor = self.connection.cursor()
        cursor.execute("CREATE TABLE test_one_statement_register (key TEXT);")
        cursor.close()
        self.connection.commit()

    def test_one_statement_register(self):
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO test_one_statement_register(key) VALUES('a')")

        result = self.engine.test_one_statement_register()

        self.assertTupleEqual(result, (u'a',))


class OneStatement_table:
    def setUp(self):
        cursor = self.connection.cursor()
        cursor.execute("CREATE TABLE test_one_statement_table (key TEXT);")
        cursor.close()
        self.connection.commit()


    def test_one_statement_table(self):
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO test_one_statement_table(key) VALUES('a')")

        result = self.engine.test_one_statement_table()

        self.assertListEqual(result, [(u'a',)])


class MultipleStatement:
    def setUp(self):
        cursor = self.connection.cursor()
        cursor.execute("CREATE TABLE test_multiple_statement (key TEXT);")
        cursor.close()
        self.connection.commit()


    def test_multiple_statement(self):
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO test_multiple_statement(key) VALUES('a')")

        self.engine.test_multiple_statement(key='c')

        result = list(cursor.execute("SELECT * FROM test_multiple_statement"))

        self.assertListEqual(result, [(u'c',)])


class Test(TestCase, Basic,
           StatementINSERTSingle, StatementINSERTMultiple,
           OneStatement_value, OneStatement_register, OneStatement_table,
           MultipleStatement):
    def __init__(self, methodName='runTest'):
        TestCase.__init__(self, methodName)

        self.dir_path = join(abspath(dirname(__file__)), 'samples_sql')

    def setUp(self):
        for base in self.__class__.__bases__:
            if hasattr(base, 'setUp'):
                base.setUp(self)


if __name__ == "__main__":
    main()
