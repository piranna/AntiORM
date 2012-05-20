'''
Created on 17/02/2012

@author: piranna
'''

from logging import warning

from antiorm.base import Base


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

    def execute(self, *args, **kwargs):
        self._cursor = self._cursor.execute(*args, **kwargs)
        return self

    def fetchone(self):
        try:
            return self._cursor.next()
        except StopIteration:
            pass

#    @property
#    def description(self):
#        return self._cursor.getdescription()

    @property
    def lastrowid(self):
        return self._cursor.getconnection().last_insert_rowid()


class APSWConnection(object):
    """Python DB-API 2.0 compatibility wrapper for APSW Connection objects

    This is done this way because since apsw.Connection is a compiled extension
    it doesn't allow to set attributes"""
    def __init__(self, connection):
        """Constructor

        @param connection: the connection to wrap
        @type connection: apsw.Connection
        """
        # This protect of apply the wrapper over another one
        if isinstance(connection, APSWConnection):
            self._connection = connection._connection
        else:
            self._connection = connection

        self._activecursor = None

    def close(self):
        self._connection.close()

    def cursor(self):
        self._activecursor = CursorWrapper(self._connection.cursor())
        return self._activecursor

    # Context manager - this two should be get via __getattr__...
    def __enter__(self):
        self._connection.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._activecursor.close()
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

    def __init__(self, db_conn, dir_path=None, bypass_types=False, lazy=False):
        """Constructor

        @param db_conn: connection of the database
        @type db_conn: DB-API 2.0 database connection
        @param dir_path: path of the dir with files from where to load SQL code
        @type dir_path: string
        @param lazy: set if SQL code at dir_path should be lazy loaded
        @type lazy: boolean
        """
        self._cachedmethods = 0

        db_conn = APSWConnection(db_conn)

        Base.__init__(self, db_conn, dir_path, bypass_types, lazy)

        self.tx_manager = db_conn

    def parse_string(self, sql, method_name, include_path='sql',
                     bypass_types=False, lazy=False):
        """Build a function from a string containing a SQL query

        If the number of parsed methods is bigger of the APSW SQLite bytecode
        cache it shows an alert because performance will decrease.
        """
        result = Base.parse_string(self, sql, method_name, include_path,
                                   bypass_types, lazy)

        self._cachedmethods += 1
        if self._cachedmethods > self._max_cachedmethods:
            warning("Surpased APSW cache size (methods: %s; limit: %s)",
                    (self._cachedmethods, self._max_cachedmethods))

        return result
