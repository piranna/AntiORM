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


#@skipIf('apsw' not in sys.modules, "APSW not installed on the system")
#class Driver(TestCase, Base):
#    "Test for the AntiORM APSW driver"
#    def setUp(self):
#        self.dir_path = join(abspath(dirname(__file__)), '../samples_sql')
#
#        self.connection = Connection(":memory:")
#        self.engine = driver_factory(self.connection, self.dir_path)
#        self.engine.row_factory = Namedtuple_factory
#
#        Base.setUp(self)
#
#    def test_driver_factory(self):
#        self.assertIsInstance(self.engine, APSW)
#
#
#@skipIf('apsw' not in sys.modules, "APSW not installed on the system")
#class Driver__ByPass(TestCase, Base):
#    "Test for the AntiORM APSW driver"
#    def setUp(self):
#        self.dir_path = join(abspath(dirname(__file__)), '../samples_sql')
#
#        self.connection = Connection(":memory:")
#        self.engine = driver_factory(self.connection, self.dir_path, True)
#        self.engine.row_factory = Namedtuple_factory
#
#        Base.setUp(self)
#
#    def test_driver_factory(self):
#        self.assertIsInstance(self.engine, APSW)


@skipIf('apsw' not in sys.modules, "APSW not installed on the system")
class Driver__LazyLoading(TestCase, Base):
    "Test for the AntiORM APSW driver"
    def setUp(self):
        self.dir_path = join(abspath(dirname(__file__)), '../samples_sql')

        self.connection = Connection(":memory:")
        self.engine = driver_factory(self.connection, self.dir_path, False, True)
        self.engine.row_factory = Namedtuple_factory

        Base.setUp(self)

    def test_driver_factory(self):
        self.assertIsInstance(self.engine, APSW)


#@skipIf('apsw' not in sys.modules, "APSW not installed on the system")
#class Driver__ByPass__LazyLoading(TestCase, Base):
#    "Test for the AntiORM APSW driver"
#    def setUp(self):
#        self.dir_path = join(abspath(dirname(__file__)), '../samples_sql')
#
#        self.connection = Connection(":memory:")
#        self.engine = driver_factory(self.connection, self.dir_path, True, True)
#        self.engine.row_factory = Namedtuple_factory
#
#        Base.setUp(self)
#
#    def test_driver_factory(self):
#        self.assertIsInstance(self.engine, APSW)
#
#
#@skipIf('apsw' not in sys.modules, "APSW not installed on the system")
#class GenericDriver(TestCase, Base):
#    "Test for the AntiORM generic driver"
#    def setUp(self):
#        self.dir_path = join(abspath(dirname(__file__)), '../samples_sql')
#
#        self.connection = Connection(":memory:")
#        self.engine = Generic(self.connection, self.dir_path)
#        self.engine.row_factory = Namedtuple_factory
#
#        Base.setUp(self)
#
#
#@skipIf('apsw' not in sys.modules, "APSW not installed on the system")
#class GenericDriver__ByPass(TestCase, Base):
#    "Test for the AntiORM generic driver"
#    def setUp(self):
#        self.dir_path = join(abspath(dirname(__file__)), '../samples_sql')
#
#        self.connection = Connection(":memory:")
#        self.engine = Generic(self.connection, self.dir_path, True)
#        self.engine.row_factory = Namedtuple_factory
#
#        Base.setUp(self)


@skipIf('apsw' not in sys.modules, "APSW not installed on the system")
class GenericDriver__LazyLoading(TestCase, Base):
    "Test for the AntiORM generic driver"
    def setUp(self):
        self.dir_path = join(abspath(dirname(__file__)), '../samples_sql')

        self.connection = Connection(":memory:")
        self.engine = Generic(self.connection, self.dir_path, False, True)
        self.engine.row_factory = Namedtuple_factory

        Base.setUp(self)


#@skipIf('apsw' not in sys.modules, "APSW not installed on the system")
#class GenericDriver__ByPass__LazyLoading(TestCase, Base):
#    "Test for the AntiORM generic driver"
#    def setUp(self):
#        self.dir_path = join(abspath(dirname(__file__)), '../samples_sql')
#
#        self.connection = Connection(":memory:")
#        self.engine = Generic(self.connection, self.dir_path, True, True)
#        self.engine.row_factory = Namedtuple_factory
#
#        Base.setUp(self)


if __name__ == "__main__":
    main()
