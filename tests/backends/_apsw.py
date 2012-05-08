# -*- coding: utf-8 -*-

from os.path  import abspath, dirname, join
from unittest import skipIf, main, TestCase

from apsw import Connection

import sys
sys.path.insert(0, '..')

from antiorm.backends.apsw    import APSW
from antiorm.backends.generic import Generic
from antiorm.utils            import Namedtuple_factory, driver_factory

from base import Base


@skipIf('apsw' not in sys.modules, "APSW not installed on the system")
class Driver(TestCase, Base):
    "Test for the AntiORM APSW driver"
    def setUp(self):
        self.dir_path = join(abspath(dirname(__file__)), '../samples_sql')

        self.connection = Connection(":memory:")
        self.engine = APSW(self.connection, self.dir_path, False, True)
        self.engine.row_factory = Namedtuple_factory

        for base in self.__class__.__bases__:
            if hasattr(base, 'setUp'):
                base.setUp(self)

    def tearDown(self):
        self.connection.close()

    def test_row_factory(self):
        pass


@skipIf('apsw' not in sys.modules, "APSW not installed on the system")
class GenericDriver(TestCase, Base):
    "Test for the AntiORM generic driver"
    def setUp(self):
        self.dir_path = join(abspath(dirname(__file__)), '../samples_sql')

        self.connection = Connection(":memory:")
        self.engine = Generic(self.connection, self.dir_path, False, True)
        self.engine.row_factory = Namedtuple_factory

        for base in self.__class__.__bases__:
            if hasattr(base, 'setUp'):
                base.setUp(self)

    def tearDown(self):
        self.connection.close()


@skipIf('apsw' not in sys.modules, "APSW not installed on the system")
class Factory(TestCase):
    "Test for drivers factory using the AntiORM SQLite driver"
    def setUp(self):
        self.dir_path = join(abspath(dirname(__file__)), '../samples_sql')

        self.connection = Connection(":memory:")
        self.engine = driver_factory(self.connection, self.dir_path, True)
        self.engine.row_factory = Namedtuple_factory

        for base in self.__class__.__bases__:
            if hasattr(base, 'setUp'):
                base.setUp(self)

    def tearDown(self):
        self.connection.close()

    def test_driver_factory(self):
        self.assertIsInstance(self.engine, APSW)


if __name__ == "__main__":
    main()
