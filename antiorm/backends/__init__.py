# -*- coding: utf-8 -*-
"""
Base utilities classes for the connection and cursor wrappers
"""


class BaseConnection(object):
    """Connection class wrapper that add support to define row_factory"""
    def __init__(self, connection):
        """
        Constructor

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
        @type cursor: DB-API 2.0 Cursor
        """
        # This protect of apply the wrapper over another one
        if isinstance(cursor, self.__class__):
            self._cursor = cursor._cursor
        else:
            self._cursor = cursor

        self._conn = conn

    def __getattr__(self, name):
        """
        Get the attributes and methods from the wrapped cursor
        """
        return getattr(self._cursor, name)
