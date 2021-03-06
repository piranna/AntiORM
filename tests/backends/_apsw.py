# -*- coding: utf-8 -*-

try:
    from unittest import skip, skipIf, main, TestCase
except ImportError:
    from unittest2 import skip, skipIf, main, TestCase

from apsw import Connection

import sys
sys.path.insert(0, '..')

from antiorm.backends.apsw    import APSW
from antiorm.backends.generic import Generic
from antiorm.utils            import driver_factory

from base import Base


class TestFactory(Base):
    def test_driver_factory(self):
        self.assertIsInstance(self.engine, APSW)


@skipIf('apsw' not in sys.modules, "APSW not installed on the system")
class Driver(TestFactory, TestCase):
    "Test for the AntiORM APSW driver"
    def setUp(self):
        self.connection = Connection(":memory:")
        self.engine = driver_factory(self.connection, self.dir_path)

        TestFactory.setUp(self)


@skip
#@skipIf('apsw' not in sys.modules, "APSW not installed on the system")
class Driver__ByPass(TestFactory, TestCase):
    "Test for the AntiORM APSW driver"
    def setUp(self):
        self.connection = Connection(":memory:")
        self.engine = driver_factory(self.connection, self.dir_path, True)

        TestFactory.setUp(self)


@skipIf('apsw' not in sys.modules, "APSW not installed on the system")
class Driver__LazyLoading(TestFactory, TestCase):
    "Test for the AntiORM APSW driver"
    def setUp(self):
        self.connection = Connection(":memory:")
        self.engine = driver_factory(self.connection, self.dir_path, False, True)

        TestFactory.setUp(self)


@skipIf('apsw' not in sys.modules, "APSW not installed on the system")
class Driver__ByPass__LazyLoading(TestFactory, TestCase):
    "Test for the AntiORM APSW driver"
    def setUp(self):
        self.connection = Connection(":memory:")
        self.engine = driver_factory(self.connection, self.dir_path, True, True)

        TestFactory.setUp(self)


@skipIf('apsw' not in sys.modules, "APSW not installed on the system")
class GenericDriver(Base, TestCase):
    "Test for the AntiORM generic driver"
    def setUp(self):
        self.connection = Connection(":memory:")
        self.engine = Generic(self.connection, self.dir_path)

        Base.setUp(self)


@skip
#@skipIf('apsw' not in sys.modules, "APSW not installed on the system")
class GenericDriver__ByPass(Base, TestCase):
    "Test for the AntiORM generic driver"
    def setUp(self):
        self.connection = Connection(":memory:")
        self.engine = Generic(self.connection, self.dir_path, True)

        Base.setUp(self)


@skipIf('apsw' not in sys.modules, "APSW not installed on the system")
class GenericDriver__LazyLoading(Base, TestCase):
    "Test for the AntiORM generic driver"
    def setUp(self):
        self.connection = Connection(":memory:")
        self.engine = Generic(self.connection, self.dir_path, False, True)

        Base.setUp(self)


@skipIf('apsw' not in sys.modules, "APSW not installed on the system")
class GenericDriver__ByPass__LazyLoading(Base, TestCase):
    "Test for the AntiORM generic driver"
    def setUp(self):
        self.connection = Connection(":memory:")
        self.engine = Generic(self.connection, self.dir_path, True, True)

        Base.setUp(self)


if __name__ == "__main__":
    main()
