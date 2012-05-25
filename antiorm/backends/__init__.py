# -*- coding: utf-8 -*-


class BaseConnection(object):
    """Connection class wrapper that add support to define row_factory"""
    def __init__(self, connection):
        """Constructor

        @param connection: the connection to wrap
        @type connection: DB-API 2.0 connection
        """
        # This protect of apply the wrapper over another one
        if isinstance(connection, self.__class__):
            self._connection = connection._connection
        else:
            self._connection = connection


class BaseCursor:
    """Cursor class wrapper that add support to define row_factory"""
    def __init__(self, cursor, conn=None):
        """Constructor

        @param cursor: the cursor to wrap
        @type cursor: apsw.Cursor"""
        # This protect of apply the wrapper over another one
        if isinstance(cursor, self.__class__):
            self._cursor = cursor._cursor
        else:
            self._cursor = cursor

        self._conn = conn

    def __getattr__(self, name):
        return getattr(self._cursor, name)
