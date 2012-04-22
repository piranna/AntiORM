# -*- coding: utf-8 -*-

from io      import open
from os      import listdir
from os.path import basename, join, splitext

from sqlparse         import split2
from sqlparse.filters import Tokens2Unicode

from sql import Compact, GetColumns, GetLimit, IsType


def register(func):
    "Decorator to register a wrapped method inside AntiORM class"
    def wrapper(self, method_name, *args, **kwargs):
        "Get method name for registration and give the other args to the func"
        _wrapped_method = func(self, *args, **kwargs)

        setattr(self.__class__, method_name, _wrapped_method)
        return _wrapped_method

    return wrapper


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

    def commit(self):
        self.connection.commit()

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

        with open(file_path, 'rt') as file_sql:
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

        @return: the parsed function or None if `lazy` is True
        @rtype: function or None
        """

        # Lazy processing, store data & only do the parse if later is required
        if lazy:
            self._lazy[method_name] = (self.parse_string, sql, include_path)
            return

        stream = Compact(sql.strip(), include_path)

        # One statement query
        if len(split2(stream)) == 1:
            return self._one_statement(method_name, stream)

        # Multiple statement query
        return self._multiple_statement(method_name, stream)

    def _one_statement(self, method_name, stream):
        """
        `stream` SQL code only have one statement
        """
        # Insert statement (return last row id)
        if IsType('INSERT')(stream):
            return self._one_statement_INSERT(method_name,
                                              Tokens2Unicode(stream))

        # One-value function (a row of a cell)
        if GetLimit(stream) == 1:
#            # Update statement (return affected row count)
#            if IsType('UPDATE')(stream):
#                return self._statement_UPDATE_single(method_name, stream)

            columns = GetColumns(stream)

            # Value function (one row, one field)
            if len(columns) == 1 and columns[0] != '*':
                return self._one_statement_value(method_name,
                                                 Tokens2Unicode(stream))

            # Register function (one row, several fields)
            return self._one_statement_register(method_name,
                                                Tokens2Unicode(stream))

        # Table function (several rows)
        return self._one_statement_table(method_name, Tokens2Unicode(stream))

    def _multiple_statement(self, method_name, stream):
        """
        `stream` SQL have several statements (script)
        """
        # Insert statement (return last row id)
        if IsType('INSERT')(stream):
            return self._multiple_statement_INSERT(method_name,
                                                   map(unicode, split2(stream)))

        # Standard multiple statement query
        return self._multiple_statement_standard(method_name,
                                                 map(unicode, split2(stream)))

    @register
    def _one_statement_INSERT(self, sql):
        """Single INSERT statement query

        @return: the inserted row id
        """
        def _wrapped_method(self, list_or_dict=None, **kwargs):
            """Execute the INSERT statement

            @return: the inserted row id (or a list with them)
            """
            def _priv(kwargs):
                "Exec the statement and return the inserted row id"
                with self.tx_manager as conn:
                    cursor = conn.cursor()

                    cursor.execute(sql, kwargs)
                    return cursor.lastrowid

            def _priv_list(list_kwargs):
                "Exec the statement and return the inserted row id"
                result = []

                with self.tx_manager as conn:
                    cursor = conn.cursor()

                    for kwargs in list_kwargs:
                        cursor.execute(sql, kwargs)
                        result.append(cursor.lastrowid)

                return result

            # Received un-named parameter, it would be a iterable
            if list_or_dict != None:
                if isinstance(list_or_dict, dict):
                    return _priv(list_or_dict)
                return _priv_list(list_or_dict)
            return _priv(kwargs)

        return _wrapped_method

    @register
    def _multiple_statement_INSERT(self, stmts):
        """Multiple INSERT statement query

        Function that execute several SQL statements sequentially, being the
        first an INSERT one.

        @return: the inserted row id of first one (or a list of first ones)
        """
        def _wrapped_method(self, list_or_dict=None, **kwargs):
            """Execute the statements sequentially

            @return: the inserted row id from the first INSERT one
            """
            def _priv(kwargs):
                "Exec the statements and return the row id of the first"
                with self.tx_manager as conn:
                    cursor = conn.cursor()

                    cursor.execute(stmts[0], kwargs)
                    rowid = cursor.lastrowid

                    for stmt in stmts[1:]:
                        cursor.execute(stmt, kwargs)

                    return rowid

            def _priv_list(list_kwargs):
                "Exec the statements and return the row id of the first"
                result = []

                with self.tx_manager as conn:
                    cursor = conn.cursor()

                    for kwargs in list_kwargs:
                        cursor.execute(stmts[0], kwargs)
                        result.append(cursor.lastrowid)

                        for stmt in stmts[1:]:
                            cursor.execute(stmt, kwargs)

                return result

            # Received un-named parameter, it would be a iterable
            if list_or_dict != None:
                if isinstance(list_or_dict, dict):
                    return _priv(list_or_dict)
                return _priv_list(list_or_dict)
            return _priv(kwargs)

        return _wrapped_method

#    @register
#    def _statement_UPDATE_single(self, stmts):
#        """Single UPDATE statement query
#
#        @return: the number of updated rows
#        """
#        sql = unicode(stmts[0])
#
#        def _wrapped_method(self, list_or_dict=None, **kwargs):
#            """Execute the UPDATE statement
#
#            @return: the inserted row id (or a list with them)
#            """
#            def _priv(kwargs):
#                "Exec the statement and return the number of updated rows"
#                with self.tx_manager as conn:
#                    cursor = conn.cursor()
#
#                    cursor.execute(sql, kwargs).rowcount
#
#            def _priv_list(list_kwargs):
#                "Exec the statement and return the number of updated rows"
#                with self.tx_manager as conn:
#                    cursor = conn.cursor()
#
#                    return cursor.executemany(sql, list_kwargs).rowcount
#
#            # Received un-named parameter, it would be a iterable
#            if list_or_dict != None:
#                if isinstance(list_or_dict, dict):
#                    return _priv(list_or_dict)
#                return _priv_list(list_or_dict)
#            return _priv(kwargs)
#
#        return _wrapped_method

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

                    result = cursor.execute(sql, kwargs).fetchone()
                    if result:
                        return result[0]

            def _priv_list(list_kwargs):
                result = []

                with self.tx_manager as conn:
                    cursor = conn.cursor()

                    for kwargs in list_kwargs:
                        value = cursor.execute(sql, kwargs).fetchone()
                        if value:
                            value = value[0]
                        result.append(value)

                return result

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

                    return cursor.execute(sql, kwargs).fetchone()

            def _priv_list(list_kwargs):
                result = []

                with self.tx_manager as conn:
                    cursor = conn.cursor()

                    for kwargs in list_kwargs:
                        result.append(cursor.execute(sql, kwargs).fetchone())

                return result

            # Received un-named parameter, it would be a iterable
            if list_or_dict != None:
                if isinstance(list_or_dict, dict):
                    return _priv(list_or_dict)
                return _priv_list(list_or_dict)
            return _priv(kwargs)

        return _wrapped_method

    @register
    def _one_statement_table(self, sql):
        """
        `stream` SQL statement return several values (a table)
        """
        def _wrapped_method(self, list_or_dict=None, **kwargs):
            "Execute a statement. If a list is given, they are exec at once"
            def _priv(kwargs):
                with self.tx_manager as conn:
                    cursor = conn.cursor()

                    return cursor.execute(sql, kwargs).fetchall()

            def _priv_list(list_kwargs):
                result = []

                with self.tx_manager as conn:
                    cursor = conn.cursor()

                    for kwargs in list_kwargs:
                        result.append(cursor.execute(sql, kwargs).fetchall())

                return result

            # Received un-named parameter, it would be a iterable
            if list_or_dict != None:
                if isinstance(list_or_dict, dict):
                    return _priv(list_or_dict)
                return _priv_list(list_or_dict)
            return _priv(kwargs)

        return _wrapped_method

    @register
    def _multiple_statement_standard(self, stmts):
        """
        `stream` SQL have several statements (script)
        """
        def _wrapped_method(self, list_or_dict=None, **kwargs):
            "Execute the statements sequentially"
            def _priv(kwargs):
                result = []

                with self.tx_manager as conn:
                    cursor = conn.cursor()

                    for stmt in stmts:
                        result.append(cursor.execute(stmt, kwargs))

                return result

            def _priv_list(list_kwargs):
                result = []

                with self.tx_manager as conn:
                    cursor = conn.cursor()

                    for kwargs in list_kwargs:
                        result2 = []

                        for stmt in stmts:
                            result2.append(cursor.execute(stmt, kwargs))

                        result.append(result2)

                return result

            # Received un-named parameter, it would be a iterable
            if list_or_dict != None:
                if isinstance(list_or_dict, dict):
                    return _priv(list_or_dict)
                return _priv_list(list_or_dict)
            return _priv(kwargs)

        return _wrapped_method
