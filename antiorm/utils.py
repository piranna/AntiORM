# -*- coding: utf-8 -*-
"""
Several utilities functions and classes
"""

from collections import namedtuple
from re          import sub
from thread      import allocate_lock

# Factory classes
import backends
#from backends import *
#from backends._apsw   import APSW
#from backends.generic import Generic
#from backends.sqlite  import Sqlite


def namedtuple_factory(cursor, row):
    """
    Create a namedtuple from a DB-API 2.0 cursor description and its values

    @param cursor: the cursor with the columsn description
    @type cursor: a DB-API 2.0 cursor
    @param row: the row of data to be returned
    @type row: an iterable

    @return: a namedtuple object with the row data
    @rtype: namedtuple
    """
    try:
        description = cursor.description
    except AttributeError:  # APSW previously to version 3.7.12
        description = cursor.getdescription()

    return namedtuple('namedtuple', (col[0] for col in description))(*row)


def named2pyformat(sql):
    """
    Convert from 'named' paramstyle format to Python string 'pyformat' format

    @param sql: a sql query in named format
    @type sql: string

    @return: a sql query in pyformat format
    @rtype: string
    """
    return sub(":\w+", lambda m: "%%(%s)s" % m.group(0)[1:], sql)


def driver_factory(db_conn, *args, **kwargs):
    """
    A factory function to generate the correct type of database driver backend

    @param db_conn: connection to the database
    @type db_conn: DB-API 2.0 connection

    @return: an optimized backend driver for the database connection
    @rtype: an antiorm.base.Base child class instance
    """
    type_conn = db_conn.__class__.__module__

    if type_conn == 'apsw':
        return backends.apsw.APSW(db_conn, *args, **kwargs)

#    if type_conn == 'mysqldb':
#        return backends.mysql.MySQL(db_conn, *args, **kwargs)

    if type_conn == 'sqlite3':
        return backends.sqlite.Sqlite(db_conn, *args, **kwargs)

    return backends.generic.Generic(db_conn, *args, **kwargs)


class TransactionManager(object):
    """
    Transaction context manager for databases that doesn't has support for it
    """

    _lock = allocate_lock()

    def __init__(self, db_conn):
        self.connection = db_conn

    def __enter__(self):
        # Use the connection context manager if its supported
        try:
            func = self.connection.__enter__
        except AttributeError:
            pass
        else:
            return func()

        # Use custom context manager
        self._lock.acquire()
        return self.connection

    def __exit__(self, exc_type, exc_value, traceback):
        # Use the connection context manager if its supported
        try:
            func = self.connection.__exit__
        except AttributeError:
            pass
        else:
            return func(exc_type, exc_value, traceback)

        # There was an exception on the context manager, rollback and raise
        if exc_type:
            self.connection.rollback()
            self._lock.release()

            raise exc_type, exc_value, traceback

        # There were no problems on the context manager, commit
        self.connection.commit()
        self._lock.release()
