# -*- coding: utf-8 -*-

from collections import namedtuple
from re          import sub

# Factory classes
import backends
#from backends import *
#from backends._apsw   import APSW
#from backends.generic import Generic
#from backends.sqlite  import Sqlite


def Namedtuple_factory(cursor, row):
    "Create a namedtuple from a DB-API 2.0 cursor description and its values"

    try:
        description = cursor.description

    # APSW
    except AttributeError:
        description = cursor.getdescription()

    return namedtuple('namedtuple', (col[0] for col in description))(*row)


def named2pyformat(sql):
    "Convert from 'named' paramstyle format to Python string 'pyformat' format"
    return sub(":\w+", lambda m: "%%(%s)s" % m.group(0)[1:], sql)


def driver_factory(db_conn, *args, **kwargs):
    type_conn = db_conn.__class__.__module__

    if type_conn == 'apsw':
        return backends.apsw.APSW(db_conn, *args, **kwargs)

#    if type_conn == 'mysqldb':
#        return backends.mysql.MySQL(db_conn, *args, **kwargs)

    if type_conn == 'sqlite3':
        return backends.sqlite.Sqlite(db_conn, *args, **kwargs)

    return backends.generic.Generic(db_conn, *args, **kwargs)


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
        # Use the connection context manager if its supported
        try:
            return self.connection.__enter__()
        except AttributeError:
            pass

        # Use custom context manager
        if self._in_transaction:
            raise InTransactionError("Already in a transaction")

        self._in_transaction = True

        return self.connection

    def __exit__(self, exc_type, exc_value, traceback):
        # Use the connection context manager if its supported
        try:
            return self.connection.__exit__(exc_type, exc_value, traceback)
        except AttributeError:
            pass

        # There was an exception on the context manager, rollback and raise
        if exc_type:
            self.connection.rollback()
            self._in_transaction = False

            raise exc_type, exc_value, traceback

        # There were no problems on the context manager, commit
        self.connection.commit()
        self._in_transaction = False
