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


class TestFactory(Base):
    def setUp(self):
        self.engine.row_factory = Namedtuple_factory

        Base.setUp(self)

    def test_driver_factory(self):
        self.assertIsInstance(self.engine, MySQL)


@skip
#@skipIf('MySQLdb' not in sys.modules, "MySQLdb not installed on the system")
class Driver(TestFactory, TestCase):
    "Test for the AntiORM generic driver"
    def setUp(self):
        self.connection = connect(":memory:")
        self.engine = driver_factory(self.connection, self.dir_path)

        TestFactory.setUp(self)


@skip
#@skipIf('MySQLdb' not in sys.modules, "MySQLdb not installed on the system")
class Driver__ByPass(TestFactory, TestCase):
    "Test for the AntiORM generic driver"
    def setUp(self):
        self.connection = connect(":memory:")
        self.engine = driver_factory(self.connection, self.dir_path, True)

        TestFactory.setUp(self)


@skip
#@skipIf('MySQLdb' not in sys.modules, "MySQLdb not installed on the system")
class Driver__LazyLoading(TestFactory, TestCase):
    "Test for the AntiORM generic driver"
    def setUp(self):
        self.connection = connect(":memory:")
        self.engine = driver_factory(self.connection, self.dir_path, False, True)

        TestFactory.setUp(self)

@skip
#@skipIf('MySQLdb' not in sys.modules, "MySQLdb not installed on the system")
class Driver__ByPass__LazyLoading(TestFactory, TestCase):
    "Test for the AntiORM generic driver"
    def setUp(self):
        self.connection = connect(":memory:")
        self.engine = driver_factory(self.connection, self.dir_path, True, True)

        TestFactory.setUp(self)


@skip
#@skipIf('MySQLdb' not in sys.modules, "MySQLdb not installed on the system")
class GenericDriver(Base, TestCase):
    "Test for the AntiORM generic driver"
    def setUp(self):
        self.connection = connect(":memory:")
        self.engine = Generic(self.connection, self.dir_path)
        self.engine.row_factory = Namedtuple_factory

        Base.setUp(self)


@skip
#@skipIf('MySQLdb' not in sys.modules, "MySQLdb not installed on the system")
class GenericDriver__ByPass(Base, TestCase):
    "Test for the AntiORM generic driver"
    def setUp(self):
        self.connection = connect(":memory:")
        self.engine = Generic(self.connection, self.dir_path, True)
        self.engine.row_factory = Namedtuple_factory

        Base.setUp(self)


@skip
#@skipIf('MySQLdb' not in sys.modules, "MySQLdb not installed on the system")
class GenericDriver__LazyLoading(Base, TestCase):
    "Test for the AntiORM generic driver"
    def setUp(self):
        self.connection = connect(":memory:")
        self.engine = Generic(self.connection, self.dir_path, False, True)
        self.engine.row_factory = Namedtuple_factory

        Base.setUp(self)


@skip
#@skipIf('MySQLdb' not in sys.modules, "MySQLdb not installed on the system")
class GenericDriver__ByPass__LazyLoading(Base, TestCase):
    "Test for the AntiORM generic driver"
    def setUp(self):
        self.connection = connect(":memory:")
        self.engine = Generic(self.connection, self.dir_path, True, True)
        self.engine.row_factory = Namedtuple_factory

        Base.setUp(self)


if __name__ == "__main__":
    main()
