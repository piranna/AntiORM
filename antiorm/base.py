# -*- coding: utf-8 -*-
"""
Base class of AntiORM wrapper class and proxy factory
"""

from io       import open
from opcode   import opmap
from os       import listdir
from os.path  import basename, join, splitext
from warnings import warn

try:
    from sys import _getframe
except ImportError:
    _getframe = None

try:
    import byteplay
except ImportError:
    byteplay = None

from sqlparse           import split2
from sqlparse.filters   import compact, IncludeStatement, Tokens2Unicode
from sqlparse.functions import IsType, getcolumns, getlimit
from sqlparse.lexer     import tokenize
from sqlparse.pipeline  import Pipeline

from antiorm.utils import named2pyformat


LOAD_ATTR = opmap['LOAD_ATTR']


def proxy_factory(priv_dict, priv_list):
    """
    Factory function to proxy the optimized functions to the class
    """
    def _wrapped_method(self, method_name, sql, bypass_types):
        """
        Single INSERT statement query

        @param method_name: name of the method
        @type method_name: string
        @param sql: sql query of the optimized function
        @type sql: string
        @param bypass_types: flag to set if types should be bypassed on calling
        @type bypass_types: boolean

        :returns: the registered optimized function
        """
        _priv_dict = priv_dict(self, sql)
        _priv_list = priv_list(self, sql)

        def _priv_l_kw(self, *args):
            """
            Adapt `*args` to list and calls to _priv_list method
            """
            return _priv_list(self, args)

        def _priv_keyw(self, **kwargs):
            """
            Adapt `**kwargs` to dict and calls to _priv_dict method
            """
            return _priv_dict(self, kwargs)

        # Use type specific functions
        if byteplay and bypass_types:
            def _bypass_types(self, list_or_dict=None, *args, **kwargs):
                """
                Proxy function that bypass types on calling function

                @param list_or_dict: arguments passed as a list or a dict.
                    Cannonically this should be called by *args or **kwargs
                @type list_or_dict: a list or a dict

                @return: the result of the call after being bypassed
                @rtype: single value, a row or an iterable (depending of `sql`)
                """

                def bypass(suffix):
                    """
                    Do the bypass overriding the calling function

                    @param suffix: the method suffix to be added to the call
                    @type suffix: string
                    """
                    # Get the caller stack frame
                    frame = _getframe(2)

                    # Get the real method func from the stack frame
                    # http://bytes.com/topic/python/answers/43957-how-get-function-object-frame-object#post167698
                    func = getattr(frame.f_back.f_locals['self'],
                                   frame.f_code.co_name)

                    # Get the code object of the function and convert it in a
                    # list of instructions to work with them
                    code = byteplay.Code.from_code(func.func_code)

                    # Loop over the instructions looking for the last load of
                    # the method attribute before (or on) the current source
                    # code line and change it for the type specific one. This
                    # is because if a function call occupy several lines it's
                    # set internally as it's only on the last one.
                    method_index = None

                    lineno = frame.f_lineno
                    for index, (opcode, arg) in enumerate(code.code):
                        # We have reached the current source code line
                        if opcode == byteplay.SetLineno and arg > lineno:
                            break

                        # We have found a load of the method attribute, store
                        # its index on the instructions list
                        if opcode == LOAD_ATTR and arg == method_name:
                            method_index = index

                    # We have found the last load of the method, change it
                    if method_index != None:
                        code.code[method_index] = (LOAD_ATTR,
                                                   method_name + suffix)

                        #func.func_code = code.to_code()
                        func.__func__.func_code = code.to_code()

                # Do the by-pass on the caller function
                if list_or_dict != None:
                    if isinstance(list_or_dict, dict):
                        bypass('__dict')
                        return _priv_dict(self, list_or_dict)

                    bypass('__list')
                    return _priv_list(self, list_or_dict)

                if args:
                    bypass('__l_kw')
                    return _priv_l_kw(self, *args)

                bypass('__keyw')
                return _priv_keyw(self, **kwargs)

            # Register type specific optimized functions as class methods
            setattr(self.__class__, method_name + '__dict', _priv_dict)
            setattr(self.__class__, method_name + '__list', _priv_list)
            setattr(self.__class__, method_name + '__l_kw', _priv_l_kw)
            setattr(self.__class__, method_name + '__keyw', _priv_keyw)

            # Register and return by-pass
            setattr(self.__class__, method_name, _bypass_types)
            return _bypass_types

        def _proxy_types(self, list_or_dict=None, *args, **kwargs):
            """
            Proxy function that proxy to the correct type function

            @param list_or_dict: arguments passed as a list or a dict.
                Cannonically this should be called by *args or **kwargs
            @type list_or_dict: a list or a dict

            @return: the result of the call after being bypassed
            @rtype: single value, a row or an iterable (depending of `sql`)
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

    def __init__(self, db_conn, dir_path=None, bypass_types=False, lazy=False):
        """
        Constructor

        :param db_conn: connection of the database
        :type db_conn: DB-API 2.0 database connection
        :param dir_path: path of the dir with files from where to load SQL code
        :type dir_path: string
        :param bypass_types: set if parsing should bypass types
        :type bypass_types: boolean
        :param lazy: set if SQL code at dir_path should be lazy loaded
        :type lazy: boolean
        """
        self.connection = db_conn

        # Get type of the connection
        type_conn = db_conn.__class__.__module__
        if type_conn == 'antiorm.backends.generic':
            type_conn = db_conn._connection.__class__.__module__

        # Get paramstyle adapter for the connection
        if type_conn in ('MySQLdb.connections', 'psycopg2._psycopg'):
            self._paramstyle = named2pyformat
        else:
            self._paramstyle = None

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
            raise AttributeError(method_name)

        # Do the parsing right now and return the method
        result = parser(data, method_name, dir_path, bypass_types)
        return result.__get__(self, self.__class__)

    def parse_dir(self, dir_path='sql', bypass_types=False, lazy=False):
        """
        Build functions from the SQL queries inside the files at `dir_path`

        Also add the functions as a methods to the AntiORM class

        :param dir_path: path to the dir with the SQL files (for INCLUDE)
        :type dir_path: string
        :param bypass_types: set if parsing should bypass types
        :type bypass_types: boolean
        :param lazy: set if parsing should be postpone until required
        :type lazy: boolean

        :return: nothing
        :rtype: None
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

        :param file_path: the path of SQL file of the method to be parsed
        :type file_path: string
        :param method_name: the name of the method
        :type method_name: string
        :param dir_path: path to the dir with the SQL files (for INCLUDE)
        :type dir_path: string
        :param lazy: set if parsing should be postpone until required
        :type lazy: boolean

        :returns: the parsed function (except if lazy is True)
        :rtype: function
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

        :param sql: the SQL code of the method to be parsed
        :type sql: string
        :param method_name: the name of the method
        :type method_name: string
        :param dir_path: path to the dir with the SQL files (for INCLUDE)
        :type dir_path: string
        :param bypass_types: set if parsing should bypass types
        :type bypass_types: boolean
        :param lazy: set if parsing should be postpone until required
        :type lazy: boolean

        :return: the parsed function or None if `lazy` is True
        :rtype: function or None
        """

        # Lazy processing, store data & only do the parse if later is required
        if lazy:
            self._lazy[method_name] = (self.parse_string, sql, dir_path,
                                       bypass_types)
            return

        # Disable by-pass of types if not using CPython compatible bytecode
        if bypass_types and not _getframe:
            warn(RuntimeWarning("Can't acces to stack. "
                                "Disabling by-pass of types."))
            bypass_types = False

        pipe = Pipeline()
        pipe.append(tokenize)
        pipe.append(IncludeStatement(dir_path))

        stream = compact(pipe(sql.strip()))

        # One statement query
        if len(split2(stream)) == 1:
            return self._one_statement(method_name, stream, bypass_types)

        # Multiple statement query
        return self._multiple_statement(method_name, stream, bypass_types)

    @property
    def row_factory(self):
        """
        Getter of row_factory property
        """
        return self.connection.row_factory

    @row_factory.setter
    def row_factory(self, value):
        """
        Setter of row_factory property
        """
        self.connection.row_factory = value

    def _one_statement(self, method_name, stream, bypass_types):
        """
        Route to the correct optimized function for one statement queries

        :param method_name: the name of the method
        :type method_name: string
        :param stream: the stream of tokens
        :type stream: iterable of tokens
        :param bypass_types: set if parsing should bypass types
        :type bypass_types: boolean
        """
        sql = Tokens2Unicode(stream)

        if self._paramstyle:
            sql = self._paramstyle(sql)

        # Insert statement (return last row id)
        if IsType('INSERT')(stream):
            return self._one_statement_INSERT(method_name, sql, bypass_types)

        ## Update statement (return affected row count)
        #if IsType('UPDATE')(stream):
        #    return self._one_statement_UPDATE(method_name, sql, bypass_types)

        # One-value function (a row of a cell)
        if getlimit(stream) == 1:
            columns = getcolumns(stream)

            # Value function (one row, one field)
            if len(columns) == 1 and columns[0] != '*':
                return self._one_statement_value(method_name, sql, bypass_types)

            # Register function (one row, several fields)
            return self._one_statement_register(method_name, sql, bypass_types)

        # Table function (several rows)
        return self._one_statement_table(method_name, sql, bypass_types)

    def _multiple_statement(self, method_name, stream, bypass_types):
        """
        Route to the correct optimized function for multiple statements queries

        :param method_name: the name of the method
        :type method_name: string
        :param stream: the stream of tokens
        :type stream: iterable of tokens
        :param bypass_types: set if parsing should bypass types
        :type bypass_types: boolean
        """
        stmts = map(unicode, split2(stream))

        if self._paramstyle:
            stmts = map(self._paramstyle, stmts)

        # Insert statement (return last row id)
        if IsType('INSERT')(stream):
            return self._multiple_statement_INSERT(method_name, stmts,
                                                   bypass_types)

        # Standard multiple statement query
        return self._multiple_statement_standard(method_name, stmts,
                                                 bypass_types)

    # Optimized functions

    def _one_statement_INSERT__dict(self, sql):
        """
        Factory for functions with one INSERT statement by dict

        @param sql: the sql query
        @type sql: string

        @return: the optimized function
        @rtype: method function
        """
        def _wrapped_method(self, kwargs):
            """
            Exec the statement and return the inserted row id

            @param kwargs: the keyword arguments of the query
            @type kwargs: dict

            @return: the inserted row id
            @rtype: integer
            """
            with self.tx_manager as conn:
                cursor = conn.cursor()

                cursor.execute(sql, kwargs)
                return cursor.lastrowid

        return _wrapped_method

    def _one_statement_INSERT__list(self, sql):
        """
        Factory for functions with one INSERT statement by list

        @param sql: the sql query
        @type sql: string

        @return: the optimized function
        @rtype: method function
        """
        def _wrapped_method(self, list_kwargs):
            """
            Exec the statement and return the inserted row ids

            @param list_kwargs: a list with the keyword arguments of the query
            @type list_kwargs: list of dicts

            @return: the inserted row ids
            @rtype: list of integers
            """
            result = []

            with self.tx_manager as conn:
                cursor = conn.cursor()

                for kwargs in list_kwargs:
                    cursor.execute(sql, kwargs)
                    result.append(cursor.lastrowid)

            return result

        return _wrapped_method

    _one_statement_INSERT = proxy_factory(_one_statement_INSERT__dict,
                                          _one_statement_INSERT__list)

    #@register
    #def _statement_UPDATE_single(self, stmts):
    #    """Single UPDATE statement query
    #
    #    @return: the number of updated rows
    #    """
    #    sql = unicode(stmts[0])
    #
    #    def _wrapped_method(self, list_or_dict=None, **kwargs):
    #        """Execute the UPDATE statement
    #
    #        @return: the inserted row id (or a list with them)
    #        """
    #        def _priv(kwargs):
    #            "Exec the statement and return the number of updated rows"
    #            with self.tx_manager as conn:
    #                cursor = conn.cursor()
    #                cursor = cursor.execute(sql, kwargs)
    #
    #                cursor.rowcount
    #
    #        def _priv_list(list_kwargs):
    #            "Exec the statement and return the number of updated rows"
    #            with self.tx_manager as conn:
    #                cursor = conn.cursor()
    #                cursor = cursor.executemany(sql, list_kwargs)
    #
    #                return cursor.rowcount
    #
    #        # Received un-named parameter, it would be a iterable
    #        if list_or_dict != None:
    #            if isinstance(list_or_dict, dict):
    #                return _priv(list_or_dict)
    #            return _priv_list(list_or_dict)
    #        return _priv(kwargs)
    #
    #    return _wrapped_method

    def _one_statement_value__dict(self, sql):
        """
        Factory for functions with one statement that return a value by dict

        @param sql: the sql query
        @type sql: string

        @return: the optimized function
        @rtype: method function
        """
        def _wrapped_method(self, kwargs):
            """
            Exec the statement and return the value

            @param kwargs: the keyword arguments of the query
            @type kwargs: dict

            @return: the queried value
            @rtype: stored value type or None
            """
            with self.tx_manager as conn:
                cursor = conn.cursor()
                cursor.execute(sql, kwargs)

                result = cursor.fetchone()
                if result:
                    return result[0]

        return _wrapped_method

    def _one_statement_value__list(self, sql):
        """
        Factory for functions with one statement that return a value by list

        @param sql: the sql query
        @type sql: string

        @return: the optimized function
        @rtype: method function
        """
        def _wrapped_method(self, list_kwargs):
            """
            Exec the statement and return a list with the values

            @param list_kwargs: a list with the keyword arguments of the query
            @type list_kwargs: list of dicts

            @return: the queried values
            @rtype: list of stored value type or None
            """
            result = []

            with self.tx_manager as conn:
                cursor = conn.cursor()

                for kwargs in list_kwargs:
                    cursor.execute(sql, kwargs)

                    value = cursor.fetchone()
                    if value:
                        value = value[0]
                    result.append(value)

            return result

        return _wrapped_method

    _one_statement_value = proxy_factory(_one_statement_value__dict,
                                         _one_statement_value__list)

    def _one_statement_register__dict(self, sql):
        """
        Factory for functions with one statement that return a row by dict

        @param sql: the sql query
        @type sql: string

        @return: the optimized function
        @rtype: method function
        """
        def _wrapped_method(self, kwargs):
            """
            Exec the statement and return the row

            @param kwargs: the keyword arguments of the query
            @type kwargs: dict

            @return: the queried row
            @rtype: row (tuple) or None
            """
            with self.tx_manager as conn:
                cursor = conn.cursor()
                cursor.execute(sql, kwargs)

                return cursor.fetchone()

        return _wrapped_method

    def _one_statement_register__list(self, sql):
        """
        Factory for functions with one statement that return a row by list

        @param sql: the sql query
        @type sql: string

        @return: the optimized function
        @rtype: method function
        """
        def _wrapped_method(self, list_kwargs):
            """
            Exec the statement and return a list with the rows

            @param list_kwargs: the keyword arguments of the query
            @type list_kwargs: dict

            @return: the queried rows
            @rtype: list of rows (tuples) or None
            """
            result = []

            with self.tx_manager as conn:
                cursor = conn.cursor()

                for kwargs in list_kwargs:
                    cursor.execute(sql, kwargs)

                    result.append(cursor.fetchone())

            return result

        return _wrapped_method

    _one_statement_register = proxy_factory(_one_statement_register__dict,
                                            _one_statement_register__list)

    def _one_statement_table__dict(self, sql):
        """
        Factory for functions with one statement that return a query by dict

        @param sql: the sql query
        @type sql: string

        @return: the optimized function
        @rtype: method function
        """
        def _wrapped_method(self, kwargs):
            """
            Exec the statement and return the query (table, list of tuples...)

            @param kwargs: the keyword arguments of the query
            @type kwargs: dict

            @return: the query
            @rtype: table or list of tuples or generator or None
            """
            with self.tx_manager as conn:
                cursor = conn.cursor()
                cursor.execute(sql, kwargs)

                return cursor.fetchall()

        return _wrapped_method

    def _one_statement_table__list(self, sql):
        """
        Factory for functions with one statement that return a table by list

        @param sql: the sql query
        @type sql: string

        @return: the optimized function
        @rtype: method function
        """
        def _wrapped_method(self, list_kwargs):
            """
            Exec the statement and return a list with the queries

            @param list_kwargs: the keyword arguments of the query
            @type list_kwargs: dict

            @return: the queried tables
            @rtype: list of queries (tables, list of tuples...) or None
            """
            result = []

            with self.tx_manager as conn:
                cursor = conn.cursor()

                for kwargs in list_kwargs:
                    cursor.execute(sql, kwargs)

                    result.append(cursor.fetchall())

            return result

        return _wrapped_method

    _one_statement_table = proxy_factory(_one_statement_table__dict,
                                         _one_statement_table__list)

    def _multiple_statement_INSERT__dict(self, stmts):
        """
        Factory for functions with multiple INSERT statement by dict

        @param stmts: the list of sql queries
        @type stmts: iterable of strings

        @return: the optimized function
        @rtype: method function
        """
        def _wrapped_method(self, kwargs):
            """
            Exec the statement and return the first inserted row id

            @param kwargs: the keyword arguments of the query
            @type kwargs: dict

            @return: the first inserted row id
            @rtype: integer
            """
            with self.tx_manager as conn:
                cursor = conn.cursor()

                cursor.execute(stmts[0], kwargs)
                rowid = cursor.lastrowid

                for stmt in stmts[1:]:
                    cursor.execute(stmt, kwargs)

                return rowid

        return _wrapped_method

    def _multiple_statement_INSERT__list(self, stmts):
        """
        Factory for functions with multiple INSERT statement by list

        @param stmts: the list of sql queries
        @type stmts: iterable of strings

        @return: the optimized function
        @rtype: method function
        """
        def _wrapped_method(self, list_kwargs):
            """
            Exec the statement and return the inserted row ids

            @param list_kwargs: a list with the keyword arguments of the query
            @type list_kwargs: list of dicts

            @return: the first inserted row ids
            @rtype: list of integers
            """
            result = []

            with self.tx_manager as conn:
                cursor = conn.cursor()

                for kwargs in list_kwargs:
                    cursor.execute(stmts[0], kwargs)
                    result.append(cursor.lastrowid)

                    for stmt in stmts[1:]:
                        cursor.execute(stmt, kwargs)

            return result

        return _wrapped_method

    _multiple_statement_INSERT = proxy_factory(_multiple_statement_INSERT__dict,
                                               _multiple_statement_INSERT__list)

    def _multiple_statement_standard__dict(self, stmts):
        """
        Factory for functions with multiple statements by dict

        @param stmts: the list of sql queries
        @type stmts: iterable of strings

        @return: the optimized function
        @rtype: method function
        """
        def _wrapped_method(self, kwargs):
            """
            Exec the statement and return the result of executes

            @param kwargs: the keyword arguments of the query
            @type kwargs: dict

            @return: the result of executes
            @rtype: list
            """
            result = []

            with self.tx_manager as conn:
                cursor = conn.cursor()

                for stmt in stmts:
                    result.append(cursor.execute(stmt, kwargs))

            return result

        return _wrapped_method

    def _multiple_statement_standard__list(self, stmts):
        """
        Factory for functions with multiple statements by list

        @param stmts: the list of sql queries
        @type stmts: iterable of strings

        @return: the optimized function
        @rtype: method function
        """
        def _wrapped_method(self, list_kwargs):
            """
            Exec the statement and return the a list of lists of results

            @param list_kwargs: a list with the keyword arguments of the query
            @type list_kwargs: list of dicts

            @return: the list of lists of results of execute
            @rtype: list of lists
            """
            result = []

            with self.tx_manager as conn:
                cursor = conn.cursor()

                for kwargs in list_kwargs:
                    result2 = []

                    for stmt in stmts:
                        result2.append(cursor.execute(stmt, kwargs))

                    result.append(result2)

            return result

        return _wrapped_method

    _multiple_statement_standard = proxy_factory(_multiple_statement_standard__dict,
                                                 _multiple_statement_standard__list)
