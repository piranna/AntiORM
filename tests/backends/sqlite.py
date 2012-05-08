# -*- coding: utf-8 -*-

from os.path  import abspath, dirname, join
from unittest import main, skip, TestCase

from sqlite3 import connect

import sys
sys.path.insert(0, '..')

from antiorm.backends.generic import Generic
from antiorm.backends.sqlite  import Sqlite
from antiorm.utils            import Namedtuple_factory, driver_factory

from base import Base


@skip
class Driver(TestCase, Base):
    "Test for the AntiORM SQLite driver"
    @classmethod
    def setUpClass(cls):
        cls.dir_path = join(abspath(dirname(__file__)), '../samples_sql')

    def setUp(self):
        self.connection = connect(":memory:")
        self.engine = driver_factory(self.connection, self.dir_path)
        self.engine.row_factory = Namedtuple_factory

        Base.setUp(self)

    def test_driver_factory(self):
        self.assertIsInstance(self.engine, Sqlite)


@skip
class Driver__ByPass(TestCase, Base):
    "Test for the AntiORM SQLite driver"
    @classmethod
    def setUpClass(cls):
        cls.dir_path = join(abspath(dirname(__file__)), '../samples_sql')

    def setUp(self):
        self.connection = connect(":memory:")
        self.engine = driver_factory(self.connection, self.dir_path, True)
        self.engine.row_factory = Namedtuple_factory

        Base.setUp(self)

    def test_driver_factory(self):
        self.assertIsInstance(self.engine, Sqlite)


class Driver__LazyLoading(TestCase, Base):
    "Test for the AntiORM SQLite driver"
    @classmethod
    def setUpClass(cls):
        cls.dir_path = join(abspath(dirname(__file__)), '../samples_sql')

    def setUp(self):
        self.connection = connect(":memory:")
        self.engine = driver_factory(self.connection, self.dir_path, False, True)
        self.engine.row_factory = Namedtuple_factory

        Base.setUp(self)

    def test_driver_factory(self):
        self.assertIsInstance(self.engine, Sqlite)


@skip
class Driver__ByPass__LazyLoading(TestCase, Base):
    "Test for the AntiORM SQLite driver"
    def setUp(self):
        self.dir_path = join(abspath(dirname(__file__)), '../samples_sql')

        self.connection = connect(":memory:")
        self.engine = driver_factory(self.connection, self.dir_path, True, True)
        self.engine.row_factory = Namedtuple_factory

        Base.setUp(self)

    def test_driver_factory(self):
        self.assertIsInstance(self.engine, Sqlite)


@skip
class GenericDriver(TestCase, Base):
    "Test for the AntiORM generic driver"
    def setUp(self):
        self.dir_path = join(abspath(dirname(__file__)), '../samples_sql')

        self.connection = connect(":memory:")
        self.engine = Generic(self.connection, self.dir_path)
        self.engine.row_factory = Namedtuple_factory

        Base.setUp(self)


@skip
class GenericDriver__ByPass(TestCase, Base):
    "Test for the AntiORM generic driver"
    def setUp(self):
        self.dir_path = join(abspath(dirname(__file__)), '../samples_sql')

        self.connection = connect(":memory:")
        self.engine = Generic(self.connection, self.dir_path, True)
        self.engine.row_factory = Namedtuple_factory

        Base.setUp(self)


class GenericDriver__LazyLoading(TestCase, Base):
    "Test for the AntiORM generic driver"
    @classmethod
    def setUpClass(cls):
        cls.dir_path = join(abspath(dirname(__file__)), '../samples_sql')

    def setUp(self):
        self.connection = connect(":memory:")
        self.engine = Generic(self.connection, self.dir_path, False, True)
        self.engine.row_factory = Namedtuple_factory

        Base.setUp(self)


@skip
class GenericDriver__ByPass__LazyLoading(TestCase, Base):
    "Test for the AntiORM generic driver"
    def setUp(self):
        self.dir_path = join(abspath(dirname(__file__)), '../samples_sql')

        self.connection = connect(":memory:")
        self.engine = Generic(self.connection, self.dir_path, True, True)
        self.engine.row_factory = Namedtuple_factory

        Base.setUp(self)


if __name__ == "__main__":
    main()
