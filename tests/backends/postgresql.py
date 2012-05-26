# -*- coding: utf-8 -*-

try:
    from unittest import skip, skipIf, main, TestCase
except ImportError:
    from unittest2 import skip, skipIf, main, TestCase

from psycopg2 import connect

import sys
sys.path.insert(0, '..')

from antiorm.backends.generic import Generic

from base import Base


@skipIf('psycopg2' not in sys.modules, "psycopg2 not installed on the system")
class GenericDriver(Base, TestCase):
    "Test for the AntiORM generic driver"
    def setUp(self):
        self.test_name = 'test_' + self.__class__.__name__

        self.connection = connect("")
        self.connection.set_isolation_level(0)
        cursor = self.connection.cursor()
        cursor.execute('CREATE DATABASE %s' % self.test_name)
        self.connection.set_isolation_level(1)

        self.engine = Generic(self.connection, self.dir_path)

        Base.setUp(self)

    def tearDown(self):
        self.connection.set_isolation_level(0)
        cursor = self.connection.cursor()
        cursor.execute('DROP DATABASE %s' % self.test_name)
        self.connection.set_isolation_level(1)


@skip
#@skipIf('psycopg2' not in sys.modules, "psycopg2 not installed on the system")
class GenericDriver__ByPass(Base, TestCase):
    "Test for the AntiORM generic driver"
    def setUp(self):
        self.connection = connect(db=":memory:")
        self.engine = Generic(self.connection, self.dir_path, True)

        Base.setUp(self)


@skip
#@skipIf('psycopg2' not in sys.modules, "psycopg2 not installed on the system")
class GenericDriver__LazyLoading(Base, TestCase):
    "Test for the AntiORM generic driver"
    def setUp(self):
        self.connection = connect(db=":memory:")
        self.engine = Generic(self.connection, self.dir_path, False, True)

        Base.setUp(self)


@skip
#@skipIf('psycopg2' not in sys.modules, "psycopg2 not installed on the system")
class GenericDriver__ByPass__LazyLoading(Base, TestCase):
    "Test for the AntiORM generic driver"
    def setUp(self):
        self.connection = connect(db=":memory:")
        self.engine = Generic(self.connection, self.dir_path, True, True)

        Base.setUp(self)


if __name__ == "__main__":
    main()
