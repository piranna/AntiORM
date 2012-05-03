# -*- coding: utf-8 -*-

from os.path  import abspath, dirname, join
from unittest import main, TestCase

from sqlite3 import connect

import sys
sys.path.insert(0, '..')

from antiorm.backends.sqlite import Sqlite
from antiorm.utils           import Namedtuple_factory, driver_factory

from base import Basic
from base import StatementINSERTSingle, StatementINSERTMultiple
from base import OneStatement_value, OneStatement_register, OneStatement_table
from base import MultipleStatement


class Driver(TestCase,
              Basic,
              StatementINSERTSingle, StatementINSERTMultiple,
              OneStatement_value, OneStatement_register, OneStatement_table,
              MultipleStatement):
    "Test for the AntiORM SQLite driver"
    def setUp(self):
        self.dir_path = join(abspath(dirname(__file__)), '../samples_sql')

        self.connection = connect(":memory:")
        self.engine = Sqlite(self.connection, self.dir_path, True)
        self.engine.row_factory = Namedtuple_factory

        for base in self.__class__.__bases__:
            if hasattr(base, 'setUp'):
                base.setUp(self)

    def tearDown(self):
        self.connection.close()

    def test_row_factory(self):
        pass


class Factory(TestCase,
               Basic,
               StatementINSERTSingle, StatementINSERTMultiple,
               OneStatement_value, OneStatement_register, OneStatement_table,
               MultipleStatement):
    "Test for drivers factory using the AntiORM SQLite driver"
    def setUp(self):
        self.dir_path = join(abspath(dirname(__file__)), '../samples_sql')

        self.connection = connect(":memory:")
        self.engine = driver_factory(self.connection, self.dir_path, True)
        self.engine.row_factory = Namedtuple_factory

        for base in self.__class__.__bases__:
            if hasattr(base, 'setUp'):
                base.setUp(self)

    def tearDown(self):
        self.connection.close()

    def test_row_factory(self):
        pass


if __name__ == "__main__":
    main()
