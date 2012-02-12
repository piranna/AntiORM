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
    # TODO: database independent layer with full transaction managment

    _cursor = None

    def __init__(self, db_conn, dir_path=None, lazy=False):
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
            sql, include_path = self._lazy[name]

        # method was not marked for lazy loading, raise exception
        except KeyError:
            raise AttributeError

        # Do the parsing right now, unmark the method and return it
        self.parse_string(sql, name, include_path)
        del self._lazy[name]
        print self.__dict__
        return getattr(self, name)

    def transaction(self):
        return self.tx_manager

    def parse_dir(self, dir_path='sql', lazy=False):
        """
        Build functions from the SQL queries inside the files at `dir_path`
        """

        for filename in listdir(dir_path):
            self.parse_file(join(dir_path, filename), include_path=dir_path,
                            lazy=lazy)

    def parse_file(self, file_path, method_name=None, include_path='sql',
                   lazy=False):
        """
        Build a function from a file containing a SQL query
        """

        if not method_name:
            method_name = splitext(basename(file_path))[0]

        with io.open(file_path, 'rt') as f:
            self.parse_string(f.read(), method_name, include_path, lazy)

    def parse_string(self, sql, method_name, include_path='sql', lazy=False):
        """
        Build a function from a string containing a SQL query
        """

        # Lazy processing, store data & only do the parse if later is required
        if lazy:
            self._lazy[method_name] = (sql, include_path)
            return

        stream = Compact(sql.strip(), include_path)

        # Insert statement (return last row id)
        if IsType('INSERT')(stream):
            self._statement_INSERT(stream, method_name)

        # One statement query
        elif len(split2(stream)) == 1:
            self._one_statement(stream, method_name)

        # Multiple statement query
        else:
            self._multiple_statement(stream, method_name)

    def _statement_INSERT(self, stream, method_name):
        """
        Special case because we are interested in get inserted row id
        """
        stmts = split2(stream)

        # One statement query
        if len(stmts) == 1:
            sql = unicode(stmts[0])

            def _wrapped_method(self, **kwargs):
                with self.transaction() as cursor:
                    cursor.execute(sql, kwargs)
                    return cursor.lastrowid

        # Multiple statement query (return last row id of first one)
        else:
            sql = map(unicode, stmts)

            def _wrapped_method(self, _=None, **kwargs):
                with self.transaction() as cursor:
                    # Received un-named parameter, it would be a iterable
                    if _ != None:
                        if isinstance(_, dict):
                            kwargs = _
                        else:
                            for _kwargs in _:
                                for stmt in sql:
                                    cursor.execute(stmt, kwargs)
                            return

                    cursor.execute(sql[0], kwargs)
                    rowid = cursor.lastrowid

                    for stmt in sql[1:]:
                        cursor.execute(stmt, kwargs)

                    return rowid

        setattr(self.__class__, method_name, _wrapped_method)

    def _one_statement(self, stream, method_name):
        """
        `stream` SQL code only have one statement
        """
        # One-value function
        if GetLimit(stream) == 1:
            self._one_statement_value(stream, method_name)

        # Table function (several registers)
        else:
            self._one_statement_table(stream, method_name)

    def _one_statement_value(self, stream, method_name):
        """
        `stream` SQL statement only return one value (a row or a cell)
        """
        columns = GetColumns(stream)
        sql = Tokens2Unicode(stream)

        # Value function (one row, one field)
        if len(columns) == 1 and columns[0] != '*':
            def _wrapped_method(self, **kwargs):
                with self.transaction() as cursor:
                    result = cursor.execute(sql, kwargs)
                    result = result.fetchone()

                    if result:
                        return result[columns[0]]

        # Register function (one row, several fields)
        else:
            def _wrapped_method(self, **kwargs):
                with self.transaction() as cursor:
                    return cursor.execute(sql, kwargs).fetchone() or None

        setattr(self.__class__, method_name, _wrapped_method)

    def _one_statement_table(self, stream, method_name):
        """
        `stream` SQL statement return several values (a table)
        """
        sql = Tokens2Unicode(stream)

        def _wrapped_method(self, _=None, **kwargs):
            with self.transaction() as cursor:
                if _ != None:
                    if isinstance(_, dict):
                        kwargs = _
                    else:
                        return cursor.executemany(sql, _)

                result = cursor.execute(sql, kwargs)
                return result.fetchall()

        setattr(self.__class__, method_name, _wrapped_method)

    def _multiple_statement(self, stream, method_name):
        """
        `stream` SQL have several statements (script)
        """
        sql = map(unicode, split2(stream))

        def _wrapped_method(self, **kwargs):
            with self.transaction() as cursor:
                for sql_stmt in sql:
                    cursor.execute(sql_stmt, kwargs)

        setattr(self.__class__, method_name, _wrapped_method)
