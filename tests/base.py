# -*- coding: utf-8 -*-


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

    def test_statement_INSERT_multiple(self):
        rowid = self.engine.test_statement_INSERT_multiple(key='a')

        self.assertIsNotNone(rowid)

        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM test_statement_INSERT_multiple")
        result = cursor.fetchall()

        self.assertEqual(len(result), 1)
        self.assertEqual(len(result[0]), 2)
        self.assertEqual(result[0][0], u'a')
        self.assertEqual(result[0][0], result[0][1])

    def test_statement_INSERT_multiple_dict(self):
        rowid = self.engine.test_statement_INSERT_multiple({'key': 'b'})

        self.assertIsNotNone(rowid)

        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM test_statement_INSERT_multiple")
        result = cursor.fetchall()

        self.assertEqual(len(result), 1)

        self.assertEqual(len(result[0]), 2)
        self.assertEqual(result[0][0], u'b')
        self.assertEqual(result[0][0], result[0][1])

    def test_statement_INSERT_multiple_list(self):
        rowid = self.engine.test_statement_INSERT_multiple([{'key': 'c'},
                                                            {'key': 'd'}])

        self.assertIsNotNone(rowid)
        self.assertIsNotNone(rowid[0])
        self.assertIsNotNone(rowid[1])

        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM test_statement_INSERT_multiple")
        result = cursor.fetchall()

        self.assertEqual(len(result), 2)

        self.assertEqual(len(result[0]), 2)
        self.assertEqual(result[0][0], u'c')
        self.assertEqual(result[0][0], result[0][1])

        self.assertEqual(len(result[1]), 2)
        self.assertEqual(result[1][0], u'd')
        self.assertEqual(result[1][0], result[1][1])


class OneStatement_value:
    def setUp(self):
        cursor = self.connection.cursor()
        cursor.execute("CREATE TABLE test_one_statement_value (key TEXT);")
        cursor.close()
        self.connection.commit()

    def test_one_statement_value(self):
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO test_one_statement_value(key) VALUES('a')")

        result = self.engine.test_one_statement_value(key='a')

        self.assertEqual(result, u'a')

    def test_one_statement_value_dict(self):
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO test_one_statement_value(key) VALUES('b')")

        result = self.engine.test_one_statement_value({'key': 'b'})

        self.assertEqual(result, u'b')

    def test_one_statement_value_list(self):
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO test_one_statement_value(key) VALUES('c')")

        result = self.engine.test_one_statement_value([{'key': 'c'}])

        self.assertEqual(result, u'c')


class OneStatement_register:
    def setUp(self):
        cursor = self.connection.cursor()
        cursor.execute("CREATE TABLE test_one_statement_register (key TEXT);")
        cursor.close()
        self.connection.commit()

    def test_one_statement_register(self):
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO test_one_statement_register(key) VALUES('a')")

        result = self.engine.test_one_statement_register(key='a')

        self.assertTupleEqual(result, (u'a',))

    def test_one_statement_register_dict(self):
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO test_one_statement_register(key) VALUES('b')")

        result = self.engine.test_one_statement_register({'key': 'b'})

        self.assertTupleEqual(result, (u'b',))

    def test_one_statement_register_list(self):
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO test_one_statement_register(key) VALUES('c')")

        result = self.engine.test_one_statement_register([{'key': 'c'}])

        self.assertTupleEqual(result, (u'c',))


class OneStatement_table:
    def setUp(self):
        cursor = self.connection.cursor()
        cursor.execute("CREATE TABLE test_one_statement_table (key TEXT);")
        cursor.close()
        self.connection.commit()

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

        result = list(self.engine.test_one_statement_table([{'key': 'c'}]))

        self.assertListEqual(result, [(u'c',)])


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
