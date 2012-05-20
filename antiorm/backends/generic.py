'''
Created on 05/03/2012

@author: piranna
'''

from imp import find_module, load_module

from antiorm.backends.apsw import APSWConnection
from antiorm.base          import Base
from antiorm.utils         import _TransactionManager


class GenericConnection(object):
    def __init__(self, connection):
        # This protect of apply the wrapper over another one
        if isinstance(connection, APSWConnection):
            self._connection = connection._connection
        else:
            self._connection = connection

        # Import correct Cursor class for the connection
        name = connection.__class__.__module__
        file, filename, description = find_module(name)
        module = load_module(name, file, filename, description)
        Cursor = module.Cursor

        class cursorclass(Cursor):
            row_factory = None

            def fetchone(self):
                result = Cursor.fetchone(self)
                if self.row_factory:
                    result = self.row_factory(result)
                return result

        self._cursorclass = cursorclass

    def commit(self):
        return self._connection.commit()

    def cursor(self):
        return self._connection.cursor(self._cursorclass)

    def rollback(self):
        return self._connection.rollback()

    @property
    def row_factory(self):
        return self._cursorclass.row_factory

    @row_factory.setter
    def row_factory(self, value):
        self._cursorclass.row_factory = value


class Generic(Base):
    """Generic driver for AntiORM.

    Using this should be enought for any project, but it's recomended to use a
    specific driver for your type of database connection to be able to use some
    optimizations.
    """

    def __init__(self, db_conn, dir_path=None, bypass_types=False, lazy=False):
        """Constructor

        @param db_conn: connection of the database
        @type db_conn: DB-API 2.0 database connection
        @param dir_path: path of the dir with files from where to load SQL code
        @type dir_path: string
        @param lazy: set if SQL code at dir_path should be lazy loaded
        @type lazy: boolean
        """
        # Check if database connection is from APSW so we force the wrapper
        if db_conn.__class__.__module__ == 'apsw':
            db_conn = APSWConnection(db_conn)
        else:
            db_conn = GenericConnection(db_conn)

        Base.__init__(self, db_conn, dir_path, bypass_types, lazy)

        self.tx_manager = _TransactionManager(db_conn)
