# -*- coding: utf-8 -*-

from os.path  import abspath, dirname, join

from antiorm.utils import Namedtuple_factory


class Base:
    def __init__(self):
        self.engine = None
        self.connection = None

    @classmethod
    def setUpClass(cls):
        cls.dir_path = join(abspath(dirname(__file__)), 'samples_sql')

    def setUp(self):
        cursor = self.connection.cursor()

        cursor.execute("""CREATE TEMPORARY TABLE test_statement_INSERT_single
        (key TEXT);""")
        cursor.execute("""CREATE TEMPORARY TABLE test_multiple_statement_INSERT
        (
            key   TEXT,
            value TEXT NULL
        );""")
        cursor.execute("""CREATE TEMPORARY TABLE test_one_statement_table
        (key TEXT);""")
        cursor.execute("""CREATE TEMPORARY TABLE test_multiple_statement
        (key TEXT);""")

        cursor.close()
#        self.connection.commit()

    def tearDown(self):
        self.connection.close()

    def test_method_notparsed(self):
        with self.assertRaises(AttributeError):
            self.engine.notparsed()

    def test_method_exists(self):
        attr = getattr(self.engine, 'test_statement_INSERT_single', None)
        self.assertNotEqual(attr, None)

    def test_statement_INSERT_single(self):
        rowid = self.engine.test_statement_INSERT_single(key="hola")

        self.assertIsNotNone(rowid)

        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM test_statement_INSERT_single")
        result = cursor.fetchall()

        self.assertEqual(len(result), 1)
        self.assertEqual(len(result[0]), 1)
        self.assertEqual(result[0][0], u'hola')

    def test_statement_INSERT_single_dict(self):
        rowid = self.engine.test_statement_INSERT_single({'key': "adios"})

        self.assertIsNotNone(rowid)

        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM test_statement_INSERT_single")
        result = cursor.fetchall()

        self.assertEqual(len(result), 1)
        self.assertEqual(len(result[0]), 1)
        self.assertEqual(result[0][0], u'adios')

    def test_statement_INSERT_single_list(self):
        rowid = self.engine.test_statement_INSERT_single([{'key': 'a'},
                                                          {'key': 'b'}])

        self.assertIsNotNone(rowid)
        self.assertIsNotNone(rowid[0])
        self.assertIsNotNone(rowid[1])

        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM test_statement_INSERT_single")
        result = cursor.fetchall()

        self.assertEqual(len(result), 2)
        self.assertEqual(len(result[0]), 1)
        self.assertEqual(result[0][0], u'a')
        self.assertEqual(len(result[1]), 1)
        self.assertEqual(result[1][0], u'b')

    def test_multiple_statement_INSERT(self):
        rowid = self.engine.test_multiple_statement_INSERT(key='a')

        self.assertIsNotNone(rowid)

        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM test_multiple_statement_INSERT")
        result = cursor.fetchall()

        self.assertEqual(len(result), 1)
        self.assertEqual(len(result[0]), 2)
        self.assertEqual(result[0][0], u'a')
        self.assertEqual(result[0][0], result[0][1])

    def test_multiple_statement_INSERT_dict(self):
        rowid = self.engine.test_multiple_statement_INSERT({'key': 'b'})

        self.assertIsNotNone(rowid)

        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM test_multiple_statement_INSERT")
        result = cursor.fetchall()

        self.assertEqual(len(result), 1)

        self.assertEqual(len(result[0]), 2)
        self.assertEqual(result[0][0], u'b')
        self.assertEqual(result[0][0], result[0][1])

    def test_multiple_statement_INSERT_list(self):
        rowid = self.engine.test_multiple_statement_INSERT([{'key': 'c'},
                                                            {'key': 'd'}])

        self.assertIsNotNone(rowid)
        self.assertIsNotNone(rowid[0])
        self.assertIsNotNone(rowid[1])

        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM test_multiple_statement_INSERT")
        result = cursor.fetchall()

        self.assertEqual(len(result), 2)

        self.assertEqual(len(result[0]), 2)
        self.assertEqual(result[0][0], u'c')
        self.assertEqual(result[0][0], result[0][1])

        self.assertEqual(len(result[1]), 2)
        self.assertEqual(result[1][0], u'd')
        self.assertEqual(result[1][0], result[1][1])

    def test_one_statement_value(self):
        result = self.engine.test_one_statement_value(doing='Roller Coaster')

        self.assertEqual(result, u'Roller Coaster')

    def test_one_statement_value_dict(self):
        result = self.engine.test_one_statement_value({'doing': 'Rocket'})

        self.assertEqual(result, u'Rocket')

    def test_one_statement_value_list(self):
        result = self.engine.test_one_statement_value([{'doing': 'Mummy'}])

        self.assertListEqual(result, [u'Mummy'])

    def test_one_statement_register(self):
        result = self.engine.test_one_statement_register(doing='Eiffel Tower')

        self.assertTupleEqual(result, (u'Eiffel Tower',))

    def test_one_statement_register_dict(self):
        result = self.engine.test_one_statement_register({'doing': 'Dodo'})

        self.assertTupleEqual(result, (u'Dodo',))

    def test_one_statement_register_list(self):
        result = self.engine.test_one_statement_register([{'doing': 'Monkey'}])

        self.assertListEqual(result, [(u'Monkey',)])

    def test_one_statement_table(self):
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO test_one_statement_table(key) VALUES('a')")

        result = self.engine.test_one_statement_table(key='a')

        self.assertListEqual(result, [(u'a',)])

    def test_one_statement_table_dict(self):
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO test_one_statement_table(key) VALUES('b')")

        result = self.engine.test_one_statement_table({'key': 'b'})

        self.assertListEqual(result, [(u'b',)])

    def test_one_statement_table_list(self):
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO test_one_statement_table(key) VALUES('c')")

        result = self.engine.test_one_statement_table([{'key': 'c'}])

        self.assertListEqual(result, [[(u'c',)]])

    def test_multiple_statement(self):
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO test_multiple_statement(key) VALUES('a')")

        self.engine.test_multiple_statement(key='c')

        result = list(cursor.execute("SELECT * FROM test_multiple_statement"))

        self.assertListEqual(result, [(u'c',)])

    def test_multiple_statement_dict(self):
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO test_multiple_statement(key) VALUES('a')")

        self.engine.test_multiple_statement({'key': 'd'})

        result = list(cursor.execute("SELECT * FROM test_multiple_statement"))

        self.assertListEqual(result, [(u'd',)])

    def test_multiple_statement_list(self):
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO test_multiple_statement(key) VALUES('a')")

        self.engine.test_multiple_statement([{'key': 'e'}])

        result = list(cursor.execute("SELECT * FROM test_multiple_statement"))

        self.assertListEqual(result, [(u'e',)])

    def test_row_factory(self):
        self.engine.row_factory = Namedtuple_factory

        result = self.engine.test_row_factory()

        self.assertTupleEqual(result, (u'Phineas', u'Flinn'))

        self.assertEqual(result.name, u'Phineas')
        self.assertEqual(result.surname, u'Flinn')
