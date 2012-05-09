# -*- coding: utf-8 -*-

from unittest import main, skip, TestCase

from sqlite3 import connect

import sys
sys.path.insert(0, '..')

from antiorm.backends.generic import Generic
from antiorm.backends.sqlite  import Sqlite
from antiorm.utils            import driver_factory

from base import Base


class TestFactory(Base):
    def test_driver_factory(self):
        self.assertIsInstance(self.engine, Sqlite)


@skip
class Driver(TestFactory, TestCase):
    "Test for the AntiORM SQLite driver"
    def setUp(self):
        self.connection = connect(":memory:")
        self.engine = driver_factory(self.connection, self.dir_path)

        TestFactory.setUp(self)


@skip
class Driver__ByPass(TestFactory, TestCase):
    "Test for the AntiORM SQLite driver"
    def setUp(self):
        self.connection = connect(":memory:")
        self.engine = driver_factory(self.connection, self.dir_path, True)

        TestFactory.setUp(self)


class Driver__LazyLoading(TestFactory, TestCase):
    "Test for the AntiORM SQLite driver"
    def setUp(self):
        self.connection = connect(":memory:")
        self.engine = driver_factory(self.connection, self.dir_path, False, True)

        TestFactory.setUp(self)


@skip
class Driver__ByPass__LazyLoading(TestFactory, TestCase):
    "Test for the AntiORM SQLite driver"
    def setUp(self):
        self.connection = connect(":memory:")
        self.engine = driver_factory(self.connection, self.dir_path, True, True)

        TestFactory.setUp(self)


@skip
class GenericDriver(Base, TestCase):
    "Test for the AntiORM generic driver"
    def setUp(self):
        self.connection = connect(":memory:")
        self.engine = Generic(self.connection, self.dir_path)

        Base.setUp(self)


@skip
class GenericDriver__ByPass(Base, TestCase):
    "Test for the AntiORM generic driver"
    def setUp(self):
        self.connection = connect(":memory:")
        self.engine = Generic(self.connection, self.dir_path, True)

        Base.setUp(self)


class GenericDriver__LazyLoading(Base, TestCase):
    "Test for the AntiORM generic driver"
    def setUp(self):
        self.connection = connect(":memory:")
        self.engine = Generic(self.connection, self.dir_path, False, True)

        Base.setUp(self)


@skip
class GenericDriver__ByPass__LazyLoading(Base, TestCase):
    "Test for the AntiORM generic driver"
    def setUp(self):
        self.connection = connect(":memory:")
        self.engine = Generic(self.connection, self.dir_path, True, True)

        Base.setUp(self)


if __name__ == "__main__":
    main()
