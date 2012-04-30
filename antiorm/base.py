# -*- coding: utf-8 -*-

from io       import open
from os       import listdir
from os.path  import basename, join, splitext
from platform import python_implementation
from warnings import warn

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


def proxy_factory():
    def _wrapped_method(self, method_name, sql, bypass_types):
        """Single INSERT statement query

        @return: the inserted row id
        """
        def _priv_dict(self, kwargs):
            "Exec the statement and return the inserted row id"
            with self.tx_manager as conn:
                cursor = conn.cursor()

                cursor.execute(sql, kwargs)
                return cursor.lastrowid

        def _priv_list(self, list_kwargs):
            "Exec the statement and return the inserted row id"
            result = []

            with self.tx_manager as conn:
                cursor = conn.cursor()

                for kwargs in list_kwargs:
                    cursor.execute(sql, kwargs)
                    result.append(cursor.lastrowid)

            return result

        def _priv_l_kw(self, *args):
            "Exec the statement and return the inserted row id"
            return _priv_dict(self, args)

        def _priv_keyw(self, **kwargs):
            "Exec the statement and return the inserted row id"
            return _priv_dict(self, kwargs)

        # Use type specific functions
        if bypass_types:
            def _bypass_types(self, list_or_dict=None, *args, **kwargs):
                """Execute the INSERT statement

                @return: the inserted row id (or a list with them)
                """
                def bypass(func):
                    print "bypass", func

                # Do the by-pass on the caller function
                if list_or_dict != None:
                    if isinstance(list_or_dict, dict):
                        bypass(_priv_dict)
                        return _priv_dict(self, list_or_dict)

                    bypass(_priv_list)
                    return _priv_list(self, list_or_dict)

                if args:
                    bypass(_priv_l_kw)
                    return _priv_l_kw(self, *args)

                bypass(_priv_keyw)
                return _priv_keyw(self, **kwargs)

            # Register type specific optimized functions as class methods
            setattr(self.__class__, method_name + '_dict', _priv_dict)
            setattr(self.__class__, method_name + '_list', _priv_list)
            setattr(self.__class__, method_name + '_l_kw', _priv_l_kw)
            setattr(self.__class__, method_name + '_keyw', _priv_keyw)

            # Register and return by-pass
            setattr(self.__class__, method_name, _bypass_types)
            return _bypass_types

        def _proxy_types(self, list_or_dict=None, *args, **kwargs):
            """Execute the INSERT statement

            @return: the inserted row id (or a list with them)
            """
            # Received un-named parameter, it would be a iterable
            if list_or_dict != None:
                if isinstance(list_or_dict, dict):
                    return _priv_dict(self, list_or_dict)
                return _priv_list(self, list_or_dict)
            if args:
                return _priv_l_kw(self, *args)
            return _priv_keyw(self, **kwargs)

        # Register and return types proxy
        setattr(self.__class__, method_name, _proxy_types)
        return _proxy_types

    return _wrapped_method


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

    def __init__(self, db_conn, dir_path=None, bypass_types=False, lazy=False):
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
            self.parse_dir(dir_path, bypass_types, lazy)

    def __getattr__(self, method_name):
        """
        Parse and return the methods marked previously for lazy loading
        """
        # Get the lazy loading stored data
        try:
            parser, data, dir_path, bypass_types = self._lazy.pop(method_name)

        # method was not marked for lazy loading, raise exception
        except KeyError:
            raise AttributeError

        # Do the parsing right now and return the method
        result = parser(data, method_name, dir_path, bypass_types)
        return result.__get__(self, self.__class__)

    def parse_dir(self, dir_path='sql', bypass_types=False, lazy=False):
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
            self.parse_file(join(dir_path, filename), None, dir_path,
                            bypass_types, lazy)

    def parse_file(self, file_path, method_name=None, dir_path='sql',
                    bypass_types=False, lazy=False):
        """
        Build a function from a file containing a SQL query

        Also add the function as a method to the AntiORM class

        @param file_path: the path of SQL file of the method to be parsed
        @type file_path: string
        @param method_name: the name of the method
        @type method_name: string
        @param dir_path: path to the dir with the SQL files (for INCLUDE)
        @type dir_path: string
        @param lazy: set if parsing should be postpone until required
        @type lazy: boolean

        @return: the parsed function (except if lazy is True)
        @rtype: function
        """

        if not method_name:
            method_name = splitext(basename(file_path))[0]

        # Lazy processing, store data & only do the parse if later is required
        if lazy:
            self._lazy[method_name] = (self.parse_file, file_path, dir_path,
                                       bypass_types)
            return

        with open(file_path, 'rt') as file_sql:
            return self.parse_string(file_sql.read(), method_name, dir_path,
                                     bypass_types)

    def parse_string(self, sql, method_name, dir_path='sql',
                       bypass_types=False, lazy=False):
        """
        Build a function from a string containing a SQL query

        Also add the function as a method to the AntiORM class

        @param sql: the SQL code of the method to be parsed
        @type sql: string
        @param method_name: the name of the method
        @type method_name: string
        @param dir_path: path to the dir with the SQL files (for INCLUDE)
        @type dir_path: string
        @param lazy: set if parsing should be postpone until required
        @type lazy: boolean

        @return: the parsed function or None if `lazy` is True
        @rtype: function or None
        """

        # Lazy processing, store data & only do the parse if later is required
        if lazy:
            self._lazy[method_name] = (self.parse_string, sql, dir_path,
                                       bypass_types)
            return

        # Disable by-pass of types if not using CPython compatible bytecode
        if bypass_types:
            vm = python_implementation()
            if vm not in ('CPython', 'PyMite'):
                warn(RuntimeWarning("%s don't have a compatible bytecode. "
                                    "Disabling by-pass of types" % vm))
                bypass_types = False

        stream = Compact(sql.strip(), dir_path)

        # One statement query
        if len(split2(stream)) == 1:
            return self._one_statement(method_name, stream, bypass_types)

        # Multiple statement query
        return self._multiple_statement(method_name, stream, bypass_types)

    def _one_statement(self, method_name, stream, bypass_types):
        """
        `stream` SQL code only have one statement
        """
        sql = Tokens2Unicode(stream)

        # Insert statement (return last row id)
        if IsType('INSERT')(stream):
            return self._one_statement_INSERT(method_name, sql, bypass_types)

#        # Update statement (return affected row count)
#        if IsType('UPDATE')(stream):
#            return self._one_statement_UPDATE(method_name, sql, bypass_types)

        # One-value function (a row of a cell)
        if GetLimit(stream) == 1:
            columns = GetColumns(stream)

            # Value function (one row, one field)
            if len(columns) == 1 and columns[0] != '*':
                return self._one_statement_value(method_name, sql, bypass_types)

            # Register function (one row, several fields)
            return self._one_statement_register(method_name, sql, bypass_types)

        # Table function (several rows)
        return self._one_statement_table(method_name, sql, bypass_types)

    def _multiple_statement(self, method_name, stream, bypass_types):
        """
        `stream` SQL have several statements (script)
        """
        stmts = map(unicode, split2(stream))

        # Insert statement (return last row id)
        if IsType('INSERT')(stream):
            return self._multiple_statement_INSERT(method_name, stmts,
                                                   bypass_types)

        # Standard multiple statement query
        return self._multiple_statement_standard(method_name, stmts,
                                                 bypass_types)

    _one_statement_INSERT = proxy_factory()

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
    def _one_statement_value(self, sql, bypass_types):
        """
        `stream` SQL statement return a cell
        """
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

        def _wrapped_method(self, list_or_dict=None, **kwargs):
            "Execute the statement and return its cell value"
            # Received un-named parameter, it would be a iterable
            if list_or_dict != None:
                if isinstance(list_or_dict, dict):
                    return _priv(list_or_dict)
                return _priv_list(list_or_dict)
            return _priv(kwargs)

        return _wrapped_method

    @register
    def _one_statement_register(self, sql, bypass_types):
        """
        `stream` SQL statement return a row
        """
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

        def _wrapped_method(self, list_or_dict=None, **kwargs):
            "Execute the statement and return a row"
            # Received un-named parameter, it would be a iterable
            if list_or_dict != None:
                if isinstance(list_or_dict, dict):
                    return _priv(list_or_dict)
                return _priv_list(list_or_dict)
            return _priv(kwargs)

        return _wrapped_method

    @register
    def _one_statement_table(self, sql, bypass_types):
        """
        `stream` SQL statement return several values (a table)
        """
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

        def _wrapped_method(self, list_or_dict=None, **kwargs):
            "Execute a statement. If a list is given, they are exec at once"
            # Received un-named parameter, it would be a iterable
            if list_or_dict != None:
                if isinstance(list_or_dict, dict):
                    return _priv(list_or_dict)
                return _priv_list(list_or_dict)
            return _priv(kwargs)

        return _wrapped_method

    @register
    def _multiple_statement_INSERT(self, stmts, bypass_types):
        """Multiple INSERT statement query

        Function that execute several SQL statements sequentially, being the
        first an INSERT one.

        @return: the inserted row id of first one (or a list of first ones)
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

        def _wrapped_method(self, list_or_dict=None, **kwargs):
            """Execute the statements sequentially

            @return: the inserted row id from the first INSERT one
            """
            # Received un-named parameter, it would be a iterable
            if list_or_dict != None:
                if isinstance(list_or_dict, dict):
                    return _priv(list_or_dict)
                return _priv_list(list_or_dict)
            return _priv(kwargs)

        return _wrapped_method

    @register
    def _multiple_statement_standard(self, stmts, bypass_types):
        """
        `stream` SQL have several statements (script)
        """
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

        def _wrapped_method(self, list_or_dict=None, **kwargs):
            "Execute the statements sequentially"
            # Received un-named parameter, it would be a iterable
            if list_or_dict != None:
                if isinstance(list_or_dict, dict):
                    return _priv(list_or_dict)
                return _priv_list(list_or_dict)
            return _priv(kwargs)

        return _wrapped_method
