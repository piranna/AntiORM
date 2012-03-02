# -*- coding: utf-8 -*-

import io
from os import listdir
from os.path import basename, join, splitext

from sqlparse import split2
from sqlparse.filters import Tokens2Unicode

from sql import Compact
from sql import GetColumns
from sql import GetLimit
from sql import IsType


def register(func):
    "Decorator to register a wrapped method inside AntiORM class"
    def wrapper(self, method_name, *args, **kwargs):
        "Get method name for registration and give the other args to the func"
        _wrapped_method = func(self, *args, **kwargs)

        setattr(self.__class__, method_name, _wrapped_method)
        return _wrapped_method

    return wrapper


class InTransactionError(Exception):
    pass


class _TransactionManager(object):
    """
    Transaction manager.
    TODO: correct handling of database exceptions.
    """

    _in_transaction = False
    _cursor = None

    def __init__(self, cls):
        self.cls = cls

    def __enter__(self):
        if self._in_transaction:
            raise InTransactionError("Already in a transaction")

        self._in_transaction = True

        if not self._cursor:
            self._cursor = self.cls.connection.cursor()

        return self._cursor

    def __exit__(self, exc_type, exc_value, traceback):
        self.cls.connection.commit()
        self._in_transaction = False


#def _transaction(func):
#    def _wrapped(self, *args, **kwargs):
#        try:
#            return func(self, *args, **kwargs)
#        finally:
#            self.connection.commit()
#
#    return _wrapped


class AntiORM(object):
    """Base driver for AntiORM.

    Using this should be enought for any project, but it's recomended to use a
    specific driver for your type of database connection to be able to use some
    optimizations.
    """
    # TODO: database independent layer with full transaction managment

    _cursor = None

    def __init__(self, db_conn, dir_path=None, lazy=False):
        """Constructor

        @param db_conn: connection of the database
        @type db_conn: DB-API 2.0 database connection
        @param dir_path: path of the dir with files from where to load SQL code
        @type dir_path: string
        @param lazy: set if SQL code at dir_path should be lazy loaded
        @type lazy: boolean
        """
        self.connection = db_conn
        self.tx_manager = _TransactionManager(self)

        self._lazy = {}

        if dir_path:
            self.parse_dir(dir_path, lazy)

    def __getattr__(self, name):
        """
        Parse and return the methods marked previously for lazy loading
        """
        # Get the lazy loading stored data
        try:
            parser, data, include_path = self._lazy.pop(name)

        # method was not marked for lazy loading, raise exception
        except KeyError:
            raise AttributeError

        # Do the parsing right now and return the method
        result = parser(data, name, include_path)
        return result.__get__(self, self.__class__)

    def transaction(self):
        "Return the current transaction manager"
        return self.tx_manager

    def parse_dir(self, dir_path='sql', lazy=False):
        """
        Build functions from the SQL queries inside the files at `dir_path`

        Also add the functions as a methods to the AntiORM class

        @param dir_path: path to the dir with the SQL files (for INCLUDE)
        @type dir_path: string
        @param lazy: set if parsing should be postpone until required
        @type lazy: boolean

        @return: nothing
        @rtype: None
        """

#        # Lazy processing, store data & only do the parse if later is required
#        if lazy:
#            self._lazy[method_name] = (self.parse_dir, dir_path)
#            return

        for filename in listdir(dir_path):
            self.parse_file(join(dir_path, filename), include_path=dir_path,
                            lazy=lazy)

    def parse_file(self, file_path, method_name=None, include_path='sql',
                   lazy=False):
        """
        Build a function from a file containing a SQL query

        Also add the function as a method to the AntiORM class

        @param file_path: the path of SQL file of the method to be parsed
        @type file_path: string
        @param method_name: the name of the method
        @type method_name: string
        @param include_path: path to the dir with the SQL files (for INCLUDE)
        @type include_path: string
        @param lazy: set if parsing should be postpone until required
        @type lazy: boolean

        @return: the parsed function (except if lazy is True)
        @rtype: function
        """

        if not method_name:
            method_name = splitext(basename(file_path))[0]

        # Lazy processing, store data & only do the parse if later is required
        if lazy:
            self._lazy[method_name] = self.parse_file, file_path, include_path
            return

        with io.open(file_path, 'rt') as file_sql:
            return self.parse_string(file_sql.read(), method_name,
                                     include_path)

    def parse_string(self, sql, method_name, include_path='sql', lazy=False):
        """
        Build a function from a string containing a SQL query

        Also add the function as a method to the AntiORM class

        @param sql: the SQL code of the method to be parsed
        @type sql: string
        @param method_name: the name of the method
        @type method_name: string
        @param include_path: path to the dir with the SQL files (for INCLUDE)
        @type include_path: string
        @param lazy: set if parsing should be postpone until required
        @type lazy: boolean

        @return: the parsed function (except if lazy is True)
        @rtype: function
        """

        # Lazy processing, store data & only do the parse if later is required
        if lazy:
            self._lazy[method_name] = (self.parse_string, sql, include_path)
            return

        stream = Compact(sql.strip(), include_path)

        # Insert statement (return last row id)
        if IsType('INSERT')(stream):
            return self._statement_INSERT(method_name, stream)

        # One statement query
        if len(split2(stream)) == 1:
            return self._one_statement(method_name, stream)

        # Multiple statement query
        return self._multiple_statement(method_name, stream)

    def _statement_INSERT(self, method_name, stream):
        """
        Special case because we are interested on inserted row id
        """
        stmts = split2(stream)

        # One statement query
        if len(stmts) == 1:
            return self._statement_INSERT_single(method_name, stmts)

        # Multiple statement query (return last row id of first one)
        return self._statement_INSERT_multiple(method_name, stmts)

    @register
    def _statement_INSERT_single(self, stmts):
        """Single INSERT statement query

        @return: the inserted row id
        """
        sql = unicode(stmts[0])

        def _wrapped_method(self, _=None, **kwargs):
            """Execute the INSERT statement

            @return: the inserted row id (or a list with them)
            """
            with self.transaction() as cursor:
                def _priv(kwargs):
                    "Exec the statement and return the inserted row id"
                    cursor.execute(sql, kwargs)
                    return cursor.lastrowid

                # Received un-named parameter, it would be a iterable
                if _ != None:
                    if isinstance(_, dict):
                        kwargs = _
                    else:
                        return map(_priv, _)

                return _priv(kwargs)

        return _wrapped_method

    @register
    def _statement_INSERT_multiple(self, stmts):
        """Multiple INSERT statement query

        Function that execute several SQL statements sequentially, being the
        first an INSERT one.

        @return: the inserted row id of first one (or a list of first ones)
        """
        sql = map(unicode, stmts)

        def _wrapped_method(self, _=None, **kwargs):
            """Execute the statements sequentially

            @return: the inserted row id from the first INSERT one
            """
            with self.transaction() as cursor:
                def _priv(kwargs):
                    "Exec the statements and return the row id of the first"
                    cursor.execute(sql[0], kwargs)
                    rowid = cursor.lastrowid

                    for stmt in sql[1:]:
                        cursor.execute(stmt, kwargs)

                    return rowid

                # Received un-named parameter, it would be a iterable
                if _ != None:
                    if isinstance(_, dict):
                        kwargs = _
                    else:
                        return map(_priv, _)

                return _priv(kwargs)

        return _wrapped_method

    def _one_statement(self, method_name, stream):
        """
        `stream` SQL code only have one statement
        """
        # One-value function (a row of a cell)
        if GetLimit(stream) == 1:
            columns = GetColumns(stream)
            column = columns[0]

            # Value function (one row, one field)
            if len(columns) == 1 and column != '*':
                return self._one_statement_value(method_name, stream, column)

            # Register function (one row, several fields)
            return self._one_statement_register(method_name, stream)

        # Table function (several rows)
        return self._one_statement_table(method_name, stream)

    @register
    def _one_statement_value(self, stream, column):
        """
        `stream` SQL statement return a cell
        """
        sql = Tokens2Unicode(stream)

        def _wrapped_method(self, **kwargs):
            "Execute the statement and return its cell value"
            with self.transaction() as cursor:
                result = cursor.execute(sql, kwargs)
                result = result.fetchone()

                if result:
                    return result[0]
#                    return result[column]

        return _wrapped_method

    @register
    def _one_statement_register(self, stream):
        """
        `stream` SQL statement return a row
        """
        sql = Tokens2Unicode(stream)

        def _wrapped_method(self, **kwargs):
            "Execute the statement and return a row"
            with self.transaction() as cursor:
                return cursor.execute(sql, kwargs).fetchone()

        return _wrapped_method

    @register
    def _one_statement_table(self, stream):
        """
        `stream` SQL statement return several values (a table)
        """
        sql = Tokens2Unicode(stream)

        def _wrapped_method(self, _=None, **kwargs):
            "Execute a statement. If a list is given, they are exec at once"
            with self.transaction() as cursor:
                if _ != None:
                    if isinstance(_, dict):
                        kwargs = _
                    else:
                        return cursor.executemany(sql, _)

                return cursor.execute(sql, kwargs).fetchall()

        return _wrapped_method

    @register
    def _multiple_statement(self, stream):
        """
        `stream` SQL have several statements (script)
        """
        sql = map(unicode, split2(stream))

        def _wrapped_method(self, **kwargs):
            "Execute the statements sequentially"
            with self.transaction() as cursor:
                for sql_stmt in sql:
                    cursor.execute(sql_stmt, kwargs)

        return _wrapped_method
