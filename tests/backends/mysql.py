# -*- coding: utf-8 -*-

from os.path  import abspath, dirname, join
from unittest import skip, skipIf, main, TestCase

from MySQLdb import connect

import sys
sys.path.insert(0, '..')

from antiorm.backends.generic import Generic
from antiorm.backends.mysql   import MySQL
from antiorm.utils            import Namedtuple_factory, driver_factory

from base import Base


@skip
#@skipIf('MySQLdb' not in sys.modules, "MySQLdb not installed on the system")
class Driver(TestCase, Base):
    "Test for the AntiORM generic driver"
    def setUp(self):
        self.dir_path = join(abspath(dirname(__file__)), '../samples_sql')

        self.connection = connect(":memory:")
        self.engine = driver_factory(self.connection, self.dir_path, False, True)
        self.engine.row_factory = Namedtuple_factory

        Base.setUp(self)

    def tearDown(self):
        self.connection.close()

    def test_row_factory(self):
        pass

    def test_driver_factory(self):
        self.assertIsInstance(self.engine, MySQL)


@skip
#@skipIf('MySQLdb' not in sys.modules, "MySQLdb not installed on the system")
class GenericDriver(TestCase, Base):
    "Test for the AntiORM generic driver"
    def setUp(self):
        self.dir_path = join(abspath(dirname(__file__)), '../samples_sql')

        self.connection = connect(":memory:")
        self.engine = Generic(self.connection, self.dir_path, False, True)
        self.engine.row_factory = Namedtuple_factory

        Base.setUp(self)

    def tearDown(self):
        self.connection.close()


if __name__ == "__main__":
    main()
