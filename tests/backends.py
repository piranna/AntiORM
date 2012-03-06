# -*- coding: utf-8 -*-

from sqlite3  import connect
from unittest import main

import sys
sys.path.insert(0, '..')

from antiorm.backends.apsw    import APSW
from antiorm.backends.generic import Generic
from antiorm.backends.sqlite  import Sqlite
from antiorm.utils            import Namedtuple_factory

from base import Test


class TestAPSW(Test):
    "Test for the AntiORM APSW driver"
    def setUp(self):
        self.connection = connect(":memory:")

        Test.setUp(self)

        self.engine = APSW(self.connection, self.dir_path)
        self.engine.row_factory = Namedtuple_factory

    def test_row_factory(self):
        pass


class TestGeneric(Test):
    "Test for the AntiORM generic driver"
    def setUp(self):
        self.connection = connect(":memory:")

        Test.setUp(self)

        self.engine = Generic(self.connection, self.dir_path)

    def tearDown(self):
        self.connection.close()


class TestSqlite(Test):
    "Test for the AntiORM SQLite driver"
    def setUp(self):
        self.connection = connect(":memory:")

        Test.setUp(self)

        self.engine = Sqlite(self.connection, self.dir_path)
        self.engine.row_factory = Namedtuple_factory

    def test_row_factory(self):
        pass


if __name__ == "__main__":
    main()
