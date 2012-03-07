# -*- coding: utf-8 -*-

from os.path  import abspath, dirname, join
from sqlite3  import connect
from unittest import main, TestCase

import sys
sys.path.insert(0, '..')

from antiorm.backends.apsw    import APSW
from antiorm.backends.generic import Generic
from antiorm.backends.sqlite  import Sqlite
from antiorm.utils            import Namedtuple_factory

from base import Basic
from base import StatementINSERTSingle, StatementINSERTMultiple
from base import OneStatement_value, OneStatement_register, OneStatement_table
from base import MultipleStatement


class TestAPSW(TestCase,
               Basic,
               StatementINSERTSingle, StatementINSERTMultiple,
               OneStatement_value, OneStatement_register, OneStatement_table,
               MultipleStatement):
    "Test for the AntiORM APSW driver"
    def setUp(self):
        self.dir_path = join(abspath(dirname(__file__)), 'samples_sql')

        self.connection = connect(":memory:")

        for base in self.__class__.__bases__:
            if hasattr(base, 'setUp'):
                base.setUp(self)

        self.engine = APSW(self.connection, self.dir_path)
        self.engine.row_factory = Namedtuple_factory

    def test_row_factory(self):
        pass


class TestGeneric(TestCase,
                  Basic,
                  StatementINSERTSingle, StatementINSERTMultiple,
                  OneStatement_value, OneStatement_register, OneStatement_table,
                  MultipleStatement):
    "Test for the AntiORM generic driver"
    def setUp(self):
        self.dir_path = join(abspath(dirname(__file__)), 'samples_sql')

        self.connection = connect(":memory:")

        for base in self.__class__.__bases__:
            if hasattr(base, 'setUp'):
                print repr(base)
                base.setUp(self)

        self.engine = Generic(self.connection, self.dir_path)

    def tearDown(self):
        self.connection.close()


class TestSqlite(TestCase,
                 Basic,
                 StatementINSERTSingle, StatementINSERTMultiple,
                 OneStatement_value, OneStatement_register, OneStatement_table,
                 MultipleStatement):
    "Test for the AntiORM SQLite driver"
    def setUp(self):
        self.dir_path = join(abspath(dirname(__file__)), 'samples_sql')

        self.connection = connect(":memory:")

        for base in self.__class__.__bases__:
            if hasattr(base, 'setUp'):
                base.setUp(self)

        self.engine = Sqlite(self.connection, self.dir_path)
        self.engine.row_factory = Namedtuple_factory

    def test_row_factory(self):
        pass


if __name__ == "__main__":
    main()
