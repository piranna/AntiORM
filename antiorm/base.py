# -*- coding: utf-8 -*-

import io
from os import listdir
from os.path import basename, join, splitext

from sqlparse import split2

from sql import Compact
from sql import GetColumns
from sql import GetLimit
from sql import IsType


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


class Base(object):
    """Base functionality for AntiORM.

    This class has the basic administrative functionality for AntiORM drivers
    and it's useless for the final user. To use AntiORM, select a specialized
    driver or in case of doubt select the Generic one.

    In the same way, if you are developing a specialized driver you should use
    the Generic one as basic and overwrite only its functions, you should have
    a very good reason to overwrite the functions of this Base class (like to
    check a functions cache if your database engine support it or similar).
    """
    # TODO: database independent layer with full transaction management

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

    def _one_statement(self, method_name, stream):
        """
        `stream` SQL code only have one statement
        """
        # One-value function (a row of a cell)
        if GetLimit(stream) == 1:
            columns = GetColumns(stream)

            # Value function (one row, one field)
            if len(columns) == 1 and columns[0] != '*':
                return self._one_statement_value(method_name, stream)

            # Register function (one row, several fields)
            return self._one_statement_register(method_name, stream)

        # Table function (several rows)
        return self._one_statement_table(method_name, stream)
