'''
Created on 05/03/2012

@author: piranna
'''

from antiorm.backends      import BaseConnection, BaseCursor
from antiorm.backends.apsw import APSWConnection
from antiorm.base          import Base
from antiorm.utils         import TransactionManager


class GenericCursor(BaseCursor):
    """Cursor class wrapper that add support to define row_factory"""
    def fetchone(self):
        result = self._cursor.fetchone()

        row_factory = self._conn.row_factory
        if row_factory:
            result = row_factory(self, result)

        return result


class GenericConnection(BaseConnection):
    """Connection class wrapper that add support to define row_factory"""
    def __init__(self, connection):
        """Constructor

        @param connection: the connection to wrap
        @type connection: DB-API 2.0 connection
        """
        BaseConnection.__init__(self, connection)

        self.row_factory = None

    def commit(self):
        return self._connection.commit()

    def cursor(self):
        return GenericCursor(self._connection.cursor(), self)

    def rollback(self):
        return self._connection.rollback()


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

        self.tx_manager = TransactionManager(db_conn)
