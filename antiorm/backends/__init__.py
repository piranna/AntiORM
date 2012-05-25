# -*- coding: utf-8 -*-


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
