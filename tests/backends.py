# -*- coding: utf-8 -*-

from unittest import main

import sys
sys.path.insert(0, '..')

from antiorm.backends.apsw    import APSW
from antiorm.backends.generic import Generic
from antiorm.backends.sqlite  import Sqlite
from antiorm.utils            import Namedtuple_factory

from base import Test, Base, StatementINSERTSingle, StatementINSERTMultiple
from base import OneStatement_value, OneStatement_register
from base import OneStatement_table, MultipleStatement


class TestGeneric(Test, Base, StatementINSERTSingle, StatementINSERTMultiple,
                  OneStatement_value, OneStatement_register,
                  OneStatement_table, MultipleStatement):
    def setUp(self):
        self.engine = Generic(self.connection, self.dir_path)


class TestAPSW(TestGeneric):
    "Test for the AntiORM APSW driver"
    def setUp(self):
        self.engine = APSW(self.connection, self.dir_path)
        self.engine.row_factory = Namedtuple_factory

    def test_row_factory(self):
        pass


class TestSqlite(TestGeneric):
    "Test for the AntiORM SQLite driver"
    def setUp(self):
        self.engine = Sqlite(self.connection, self.dir_path)
        self.engine.row_factory = Namedtuple_factory

    def test_row_factory(self):
        pass


if __name__ == "__main__":
    main()
