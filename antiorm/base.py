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

    def __init__(self, db_conn, dir_path=None):
        self.connection = db_conn
        self.tx_manager = _TransactionManager(self)

        if dir_path:
            self.parse_dir(dir_path)

    def add_to_class(self, method_name, method):
        setattr(self.__class__, method_name, method)

    @property
    def transaction(self):
        return self.tx_manager
    
    def parse_dir(self, dir_path='sql'):
        """
        Build functions from the SQL queries inside the files at `dir_path`
        """

        for filename in listdir(dir_path):
            self.parse_file(join(dir_path, filename), include_path=dir_path)

    def parse_file(self, file_path, method_name=None, include_path='sql'):
        """
        Build a function from a file containing a SQL query
        """

        if not method_name:
            method_name = splitext(basename(file_path))[0]

        with io.open(file_path, 'rt') as f:
            self.parse_string(f.read(), method_name, include_path)

    def parse_string(self, sql, method_name, include_path='sql'):
        """
        Build a function from a string containing a SQL query
        """

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
        _wrapped_method = lambda x: None

        if len(stmts) == 1:
            sql = unicode(stmts[0])

            def _wrapped_method(self, **kwargs):
                with self.transaction as cursor:
                    cursor.execute(sql, kwargs)
                    return cursor.lastrowid

        else:
            sql = map(unicode, stmts)
            def _wrapped_method(self, _=None, **kwargs):
                with self.transaction as cursor:
                    if isinstance(_, dict):
                        kwargs = _
                    else:
                        for _kwargs in _:
                            for stmt in sql:
                                cursor.execute(stmt, kwargs)
                        return

                    cursor.execute(stmts[0], kwargs)
                    rowid = cursor.lastrowid
                    
                    for stmt in sql[1:]:
                        cursor.execute(stmt, kwargs)
                    
                    return rowid
                    
        self.add_to_class(method_name, _wrapped_method)
                    
    def _one_statement(self, stream, method_name):
        # One-value function
        if GetLimit(stream) == 1:
            self._one_statement_value(stream, method_name)

        # Table function (several registers)
        else:
            self._one_statement_table(stream, method_name)

    def _one_statement_value(self, stream, method_name):
        columns = GetColumns(stream)
        sql = Tokens2Unicode(stream)

        _wrapped_method = lambda x: None

        if len(columns) == 1 and columns[0] != '*':
            def _wrapped_method(self, **kwargs):
                with self.transaction as cursor:
                    result = cursor.execute(sql, kwargs)
                    result = result.fetchone()

                    if result:
                        return result[columns[0]]

        else:
            def _wrapped_method(self, **kwargs):
                with self.transaction as cursor:
                    return cursor.execute(sql, kwargs).fetchone() or None

        self.add_to_class(method_name, _wrapped_method)

    def _one_statement_table(self, stream, method_name):
        sql = Tokens2Unicode(stream)

        def _wrapped_method(self, _=None, **kwargs):
            with self.transaction as cursor:
                if _ != None:
                    if isinstance(_, dict):
                        kwargs = _
                    else:
                        return cursor.executemany(sql, _)

                result = cursor.execute(sql, kwargs)
                return result.fetchall()

        self.add_to_class(method_name, _wrapped_method)
    
    def _multiple_statement(self, stream, method_name):
        sql = [unicode(x) for x in split2(stream)]

        def _wrapped_method(self, **kwargs):
            with self.transaction as cursor:
                for sql_stmt in sql:
                    self.cursor.execute(stmt, kwargs)


        self.add_to_class(method_name, _wrapped_method)
