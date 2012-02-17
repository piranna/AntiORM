# -*- coding: utf-8 -*-

from unittest import main

import sys
sys.path.insert(0, '..')

from antiorm.backends.apsw   import APSW
from antiorm.backends.sqlite import Sqlite

from base import TestAntiORM


class TestAPSW(TestAntiORM):
    "Test for the APSW SQLite driver"
    def setUp(self):
        self.engine = APSW(self.connection, self.dir_path)


class TestSqlite(TestAntiORM):
    "Test for the AntiORM SQLite driver"
    def setUp(self):
        self.engine = Sqlite(self.connection, self.dir_path)


if __name__ == "__main__":
    main()
