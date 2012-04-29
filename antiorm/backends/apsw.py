'''
Created on 17/02/2012

@author: piranna
'''

from logging import warning

from sqlparse.filters import Tokens2Unicode

from ..base import Base, register


class CursorWrapper(object):
    """Python DB-API 2.0 compatibility wrapper for APSW Cursor objects

    This is done this way because since apsw.Cursor is a compiled extension
    it doesn't allow to set attributes, and also it's called internally so i
    can't be able to make a subclass"""
    def __init__(self, cursor):
        """Constructor

        @param cursor: the cursor to wrap
        @type cursor: apsw.Cursor"""
        # This protect of apply the wrapper over another one
        if isinstance(cursor, CursorWrapper):
            self._cursor = cursor._cursor
        else:
            self._cursor = cursor

    def __getattr__(self, name):
        return getattr(self._cursor, name)

    @property
    def lastrowid(self):
        return self._cursor.getconnection().last_insert_rowid()


class ConnectionWrapper(object):
    """Python DB-API 2.0 compatibility wrapper for APSW Connection objects

    This is done this way because since apsw.Connection is a compiled extension
    it doesn't allow to set attributes"""
    def __init__(self, connection):
        """Constructor

        @param connection: the connection to wrap
        @type connection: apsw.Connection"""
        # This protect of apply the wrapper over another one
        if isinstance(connection, ConnectionWrapper):
            self._connection = connection._connection
        else:
            self._connection = connection

    def cursor(self):
        return CursorWrapper(self._connection.cursor())

    # Context manager - this two should be get via __getattr__...
    def __enter__(self):
        self._connection.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return self._connection.__exit__(exc_type, exc_value, traceback)

    @property
    def row_factory(self):
        return self._connection.getrowtrace()

    @row_factory.setter
    def row_factory(self, value):
        self._connection.setrowtrace(value)


class APSW(Base):
    "APSW driver for AntiORM"
    _max_cachedmethods = 100

    def __init__(self, db_conn, dir_path=None, lazy=False):
        """Constructor

        @param db_conn: connection of the database
        @type db_conn: DB-API 2.0 database connection
        @param dir_path: path of the dir with files from where to load SQL code
        @type dir_path: string
        @param lazy: set if SQL code at dir_path should be lazy loaded
        @type lazy: boolean
        """
        self._cachedmethods = 0

        db_conn = ConnectionWrapper(db_conn)

        Base.__init__(self, db_conn, dir_path, lazy)

        self.tx_manager = db_conn

    def parse_string(self, sql, method_name, include_path='sql', lazy=False):
        """Build a function from a string containing a SQL query

        If the number of parsed methods is bigger of the APSW SQLite bytecode
        cache it shows an alert because performance will decrease.
        """
        try:
            result = Base.parse_string(self, sql, method_name, include_path,
                                       lazy)
        except:
            raise

        self._cachedmethods += 1
        if self._cachedmethods > self._max_cachedmethods:
            warning("Surpased APSW cache size (methods: %s; limit: %s)",
                    (self._cachedmethods, self._max_cachedmethods))

        return result

    @register
    def _one_statement_value(self, sql):
        """
        `stream` SQL statement return a cell
        """
        def _wrapped_method(self, list_or_dict=None, **kwargs):
            "Execute the statement and return its cell value"
            def _priv(kwargs):
                with self.tx_manager as conn:
                    cursor = conn.cursor()

                    result = cursor.execute(sql, kwargs)

                    try:
                        result = result.next()
                    except StopIteration:
                        return

                    if result:
                        return result[0]

            def _priv_list(list_kwargs):
                with self.tx_manager as conn:
                    cursor = conn.cursor()

                    for kwargs in list_kwargs:
                        row = cursor.execute(sql, kwargs)

                        try:
                            row = row.next()
                        except StopIteration:
                            pass

                        else:
                            if row:
                                yield row[0]

            # Received un-named parameter, it would be a iterable
            if list_or_dict != None:
                if isinstance(list_or_dict, dict):
                    return _priv(list_or_dict)
                return _priv_list(list_or_dict)
            return _priv(kwargs)

        return _wrapped_method

    @register
    def _one_statement_register(self, sql):
        """
        `stream` SQL statement return a row
        """
        def _wrapped_method(self, list_or_dict=None, **kwargs):
            "Execute the statement and return a row"
            def _priv(kwargs):
                with self.tx_manager as conn:
                    cursor = conn.cursor()

                    row = cursor.execute(sql, kwargs)

                    try:
                        return row.next()
                    except StopIteration:
                        pass

            def _priv_list(list_kwargs):
                with self.tx_manager as conn:
                    cursor = conn.cursor()

                    for kwargs in list_kwargs:
                        row = cursor.execute(sql, kwargs)

                        try:
                            row = row.next()
                        except StopIteration:
                            pass
                        else:
                            if row:
                                yield row

            # Received un-named parameter, it would be a iterable
            if list_or_dict != None:
                if isinstance(list_or_dict, dict):
                    return _priv(list_or_dict)
                return _priv_list(list_or_dict)
            return _priv(kwargs)

        return _wrapped_method
