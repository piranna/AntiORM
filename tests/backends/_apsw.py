# -*- coding: utf-8 -*-

from os.path  import abspath, dirname, join
from unittest import skipIf, main, TestCase

from apsw import Connection

import sys
sys.path.insert(0, '..')

from antiorm.backends.apsw import APSW
from antiorm.utils         import Namedtuple_factory

from base import Basic
from base import StatementINSERTSingle, StatementINSERTMultiple
from base import OneStatement_value, OneStatement_register, OneStatement_table
from base import MultipleStatement


@skipIf('apsw' not in sys.modules, "APSW not installed on the system")
class TestAPSW(TestCase,
               Basic,
               StatementINSERTSingle, StatementINSERTMultiple,
               OneStatement_value, OneStatement_register, OneStatement_table,
               MultipleStatement):
    "Test for the AntiORM APSW driver"
    def setUp(self):
        self.dir_path = join(abspath(dirname(__file__)), '../samples_sql')

        self.connection = Connection(":memory:")
        self.engine = APSW(self.connection, self.dir_path, False, True)
        self.engine.row_factory = Namedtuple_factory

        for base in self.__class__.__bases__:
            if hasattr(base, 'setUp'):
                base.setUp(self)

    def test_row_factory(self):
        pass


if __name__ == "__main__":
    main()
