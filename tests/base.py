# -*- coding: utf-8 -*-

from os.path import abspath, dirname, join

from antiorm.utils import namedtuple_factory


class Base:
    engine = None
    connection = None

    @classmethod
    def setUpClass(cls):
        cls.dir_path = join(abspath(dirname(__file__)), 'samples_sql')

    def setUp(self):
        cursor = self.connection.cursor()

        cursor.execute("""CREATE TEMPORARY TABLE test_statement_INSERT_single
        (name TEXT);""")
        cursor.execute("""CREATE TEMPORARY TABLE test_multiple_statement_INSERT
        (
            name    TEXT,
            surname TEXT NULL
        );""")
        cursor.execute("""CREATE TEMPORARY TABLE test_multiple_statement
        (name TEXT);""")

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
        rowid = self.engine.test_statement_INSERT_single(name="Gretchen")

        self.assertIsNotNone(rowid)

        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM test_statement_INSERT_single")
        result = cursor.fetchall()

        self.assertSequenceEqual(result, [(u'Gretchen',)])

    def test_statement_INSERT_single_dict(self):
        rowid = self.engine.test_statement_INSERT_single({'name': "Holly"})

        self.assertIsNotNone(rowid)

        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM test_statement_INSERT_single")
        result = cursor.fetchall()

        self.assertSequenceEqual(result, [(u'Holly',)])

    def test_statement_INSERT_single_list(self):
        rowid = self.engine.test_statement_INSERT_single([{'name': 'Katie'},
                                                          {'name': 'Milly'}])

        self.assertIsNotNone(rowid)
        self.assertIsNotNone(rowid[0])
        self.assertIsNotNone(rowid[1])

        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM test_statement_INSERT_single")
        result = cursor.fetchall()

        self.assertSequenceEqual(result, [(u'Katie',),
                                          (u'Milly',)])

    def test_multiple_statement_INSERT(self):
        rowid = self.engine.test_multiple_statement_INSERT(name='Isabella',
                                                           surname='Saphiro')

        self.assertIsNotNone(rowid)

        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM test_multiple_statement_INSERT")
        result = cursor.fetchall()

        self.assertSequenceEqual(result, [(u'Isabella', u'Saphiro')])

    def test_multiple_statement_INSERT_dict(self):
        rowid = self.engine.test_multiple_statement_INSERT({'name': 'Buford',
                                                            'surname': 'van Stomm'})

        self.assertIsNotNone(rowid)

        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM test_multiple_statement_INSERT")
        result = cursor.fetchall()

        self.assertSequenceEqual(result, [(u'Buford', u'van Stomm')])

    def test_multiple_statement_INSERT_list(self):
        rowid = self.engine.test_multiple_statement_INSERT([{'name': 'Candance',
                                                             'surname': 'Flinn'},
                                                            {'name': 'Jeremy',
                                                             'surname': 'Johnson'}])

        self.assertIsNotNone(rowid)
        self.assertIsNotNone(rowid[0])
        self.assertIsNotNone(rowid[1])

        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM test_multiple_statement_INSERT")
        result = cursor.fetchall()

        self.assertEqual(len(result), 2)

        self.assertSequenceEqual(result, [(u'Candance', u'Flinn'),
                                          (u'Jeremy', u'Johnson')])

    def test_one_statement_value(self):
        result = self.engine.test_one_statement_value(doing='Rocket')

        self.assertEqual(result, u'Rocket')

    def test_one_statement_value_dict(self):
        result = self.engine.test_one_statement_value({'doing': 'Mummy'})

        self.assertEqual(result, u'Mummy')

    def test_one_statement_value_list(self):
        result = self.engine.test_one_statement_value([{'doing': 'Eiffel Tower'}])

        self.assertListEqual(result, [u'Eiffel Tower'])

    def test_one_statement_register(self):
        result = self.engine.test_one_statement_register(doing='Monster')

        self.assertTupleEqual(result, (u'Monster',))

    def test_one_statement_register_dict(self):
        result = self.engine.test_one_statement_register({'doing': 'Monkey'})

        self.assertTupleEqual(result, (u'Monkey',))

    def test_one_statement_register_list(self):
        result = self.engine.test_one_statement_register([{'doing': 'Waves'}])

        self.assertListEqual(result, [(u'Waves',)])

    def test_one_statement_table(self):
        result = self.engine.test_one_statement_table(doing='Nanobots')

        self.assertSequenceEqual(result, [(u'Nanobots',)])

    def test_one_statement_table_dict(self):
        result = self.engine.test_one_statement_table({'doing': 'Frankestein'})

        self.assertSequenceEqual(result, [(u'Frankestein',)])

    def test_one_statement_table_list(self):
        result = self.engine.test_one_statement_table([{'doing': 'Painting'}])
        expected = [[(u'Painting',)]]

        for index, l in enumerate(result):
            self.assertSequenceEqual(l, expected[index])

    def test_multiple_statement(self):
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO test_multiple_statement(name) VALUES('a')")

        self.engine.test_multiple_statement(name='Adyson Sweetwater')

        cursor.execute("SELECT * FROM test_multiple_statement")
        result = cursor.fetchall()

        self.assertSequenceEqual(result, [(u'Adyson Sweetwater',)])

    def test_multiple_statement_dict(self):
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO test_multiple_statement(name) VALUES('a')")

        self.engine.test_multiple_statement({'name': 'Ginger'})

        cursor.execute("SELECT * FROM test_multiple_statement")
        result = cursor.fetchall()

        self.assertSequenceEqual(result, [(u'Ginger',)])

    def test_multiple_statement_list(self):
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO test_multiple_statement(name) VALUES('a')")

        self.engine.test_multiple_statement([{'name': 'Perry the Platypus'}])

        cursor.execute("SELECT * FROM test_multiple_statement")
        result = cursor.fetchall()

        self.assertSequenceEqual(result, [(u'Perry the Platypus',)])

    def test_row_factory(self):
        self.engine.row_factory = namedtuple_factory

        result = self.engine.test_row_factory()

        self.assertTupleEqual(result, (u'Phineas', u'Flinn'))

        self.assertEqual(result.name, u'Phineas')
        self.assertEqual(result.surname, u'Flinn')
