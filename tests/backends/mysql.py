# -*- coding: utf-8 -*-

from unittest import skip, skipIf, main, TestCase

from MySQLdb import connect

import sys
sys.path.insert(0, '..')

from antiorm.backends.generic import Generic
from antiorm.backends.mysql   import MySQL
from antiorm.utils            import driver_factory

from base import Base


class TestFactory(Base):
    def test_driver_factory(self):
        self.assertIsInstance(self.engine, MySQL)


@skip
#@skipIf('MySQLdb' not in sys.modules, "MySQLdb not installed on the system")
class Driver(TestFactory, TestCase):
    "Test for the AntiORM MySQL driver"
    def setUp(self):
        self.connection = connect(db=":memory:")
        self.engine = driver_factory(self.connection, self.dir_path)

        TestFactory.setUp(self)


@skip
#@skipIf('MySQLdb' not in sys.modules, "MySQLdb not installed on the system")
class Driver__ByPass(TestFactory, TestCase):
    "Test for the AntiORM MySQL driver"
    def setUp(self):
        self.connection = connect(db=":memory:")
        self.engine = driver_factory(self.connection, self.dir_path, True)

        TestFactory.setUp(self)


@skip
#@skipIf('MySQLdb' not in sys.modules, "MySQLdb not installed on the system")
class Driver__LazyLoading(TestFactory, TestCase):
    "Test for the AntiORM MySQL driver"
    def setUp(self):
        self.connection = connect(db=":memory:")
        self.engine = driver_factory(self.connection, self.dir_path, False, True)

        TestFactory.setUp(self)

@skip
#@skipIf('MySQLdb' not in sys.modules, "MySQLdb not installed on the system")
class Driver__ByPass__LazyLoading(TestFactory, TestCase):
    "Test for the AntiORM MySQL driver"
    def setUp(self):
        self.connection = connect(db=":memory:")
        self.engine = driver_factory(self.connection, self.dir_path, True, True)

        TestFactory.setUp(self)


@skipIf('MySQLdb' not in sys.modules, "MySQLdb not installed on the system")
class GenericDriver(Base, TestCase):
    "Test for the AntiORM generic driver"
    def setUp(self):
        self.test_name = 'test_' + self.__class__.__name__

        self.connection = connect()
        cursor = self.connection.cursor()
        cursor.execute('CREATE DATABASE IF NOT EXISTS %s' % self.test_name)
        cursor.execute('USE %s' % self.test_name)

        self.engine = Generic(self.connection, self.dir_path)

        Base.setUp(self)

    def tearDown(self):
        cursor = self.connection.cursor()
        cursor.execute('DROP DATABASE %s' % self.test_name)


@skip
#@skipIf('MySQLdb' not in sys.modules, "MySQLdb not installed on the system")
class GenericDriver__ByPass(Base, TestCase):
    "Test for the AntiORM generic driver"
    def setUp(self):
        self.connection = connect(db=":memory:")
        self.engine = Generic(self.connection, self.dir_path, True)

        Base.setUp(self)


@skip
#@skipIf('MySQLdb' not in sys.modules, "MySQLdb not installed on the system")
class GenericDriver__LazyLoading(Base, TestCase):
    "Test for the AntiORM generic driver"
    def setUp(self):
        self.connection = connect(db=":memory:")
        self.engine = Generic(self.connection, self.dir_path, False, True)

        Base.setUp(self)


@skip
#@skipIf('MySQLdb' not in sys.modules, "MySQLdb not installed on the system")
class GenericDriver__ByPass__LazyLoading(Base, TestCase):
    "Test for the AntiORM generic driver"
    def setUp(self):
        self.connection = connect(db=":memory:")
        self.engine = Generic(self.connection, self.dir_path, True, True)

        Base.setUp(self)


if __name__ == "__main__":
    main()
