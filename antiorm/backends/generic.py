'''
Created on 05/03/2012

@author: piranna
'''

from ..base import Base


class InTransactionError(Exception):
    pass


class _TransactionManager(object):
    """
    Transaction context manager for databases that doesn't has support for it
    """

    _in_transaction = False

    def __init__(self, db_conn):
        self.connection = db_conn

    def __enter__(self):
        if self._in_transaction:
            raise InTransactionError("Already in a transaction")

        self._in_transaction = True

        return self.connection

    def __exit__(self, exc_type, exc_value, traceback):
        # There was an exception on the context manager, rollback and raise
        if exc_type:
            self.connection.rollback()
            self._in_transaction = False

            raise exc_type, exc_value, traceback

        # There were no problems on the context manager, commit
        self.connection.commit()
        self._in_transaction = False


class Generic(Base):
    """Generic driver for AntiORM.

    Using this should be enought for any project, but it's recomended to use a
    specific driver for your type of database connection to be able to use some
    optimizations.
    """

    def __init__(self, db_conn, dir_path=None, lazy=False):
        """Constructor

        @param db_conn: connection of the database
        @type db_conn: DB-API 2.0 database connection
        @param dir_path: path of the dir with files from where to load SQL code
        @type dir_path: string
        @param lazy: set if SQL code at dir_path should be lazy loaded
        @type lazy: boolean
        """
        Base.__init__(self, db_conn, dir_path, lazy)

        self.tx_manager = _TransactionManager(db_conn)
