# -*- coding: utf-8 -*-

from os.path  import abspath, dirname, join
from unittest import skip, skipIf, main, TestCase

from MySQLdb import connect

import sys
sys.path.insert(0, '..')

from antiorm.backends.mysql import MySQL
from antiorm.utils          import Namedtuple_factory, driver_factory

from base import Basic
from base import StatementINSERTSingle, StatementINSERTMultiple
from base import OneStatement_value, OneStatement_register, OneStatement_table
from base import MultipleStatement


@skip
#@skipIf('MySQLdb' not in sys.modules, "MySQLdb not installed on the system")
class TestMySQL(TestCase,
                Basic,
                StatementINSERTSingle, StatementINSERTMultiple,
                OneStatement_value, OneStatement_register, OneStatement_table,
                MultipleStatement):
    "Test for the AntiORM generic driver"
    def setUp(self):
        self.dir_path = join(abspath(dirname(__file__)), '../samples_sql')

        self.connection = connect(":memory:")
        self.engine = MySQL(self.connection, self.dir_path, True)
        self.engine.row_factory = Namedtuple_factory

        for base in self.__class__.__bases__:
            if hasattr(base, 'setUp'):
                base.setUp(self)

    def tearDown(self):
        self.connection.close()

    def test_row_factory(self):
        pass


@skip
#@skipIf('MySQLdb' not in sys.modules, "MySQLdb not installed on the system")
class Factory(TestCase):
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

    def test_driver_factory(self):
        self.assertIsInstance(self.engine, MySQL)


if __name__ == "__main__":
    main()
