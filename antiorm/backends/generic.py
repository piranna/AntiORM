'''
Created on 05/03/2012

@author: piranna
'''

from sqlparse import split2
from sqlparse.filters import Tokens2Unicode

from ..base  import Base


def register(func):
    "Decorator to register a wrapped method inside AntiORM class"
    def wrapper(self, method_name, *args, **kwargs):
        "Get method name for registration and give the other args to the func"
        _wrapped_method = func(self, *args, **kwargs)

        setattr(self.__class__, method_name, _wrapped_method)
        return _wrapped_method

    return wrapper


class Generic(Base):
    """Generic driver for AntiORM.

    Using this should be enought for any project, but it's recomended to use a
    specific driver for your type of database connection to be able to use some
    optimizations.
    """

    @register
    def _statement_INSERT_single(self, stmts):
        """Single INSERT statement query

        @return: the inserted row id
        """
        sql = unicode(stmts[0])

        def _wrapped_method(self, list_or_dict=None, **kwargs):
            """Execute the INSERT statement

            @return: the inserted row id (or a list with them)
            """
            def _priv(kwargs):
                "Exec the statement and return the inserted row id"
                with self.transaction() as cursor:
                    cursor.execute(sql, kwargs)
                    return cursor.lastrowid

            def _priv_list(list_kwargs):
                "Exec the statement and return the inserted row id"
                result = []

                with self.transaction() as cursor:
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
    def _statement_INSERT_multiple(self, stmts):
        """Multiple INSERT statement query

        Function that execute several SQL statements sequentially, being the
        first an INSERT one.

        @return: the inserted row id of first one (or a list of first ones)
        """
        sql = map(unicode, stmts)

        def _wrapped_method(self, list_or_dict=None, **kwargs):
            """Execute the statements sequentially

            @return: the inserted row id from the first INSERT one
            """
            def _priv(kwargs):
                "Exec the statements and return the row id of the first"
                with self.transaction() as cursor:
                    cursor.execute(sql[0], kwargs)
                    rowid = cursor.lastrowid

                    for stmt in sql[1:]:
                        cursor.execute(stmt, kwargs)

                    return rowid

            def _priv_list(list_kwargs):
                "Exec the statements and return the row id of the first"
                result = []

                with self.transaction() as cursor:
                    for kwargs in list_kwargs:
                        cursor.execute(sql[0], kwargs)
                        result.append(cursor.lastrowid)

                        for stmt in sql[1:]:
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
#                with self.transaction() as cursor:
#                    cursor.execute(sql, kwargs).rowcount
#
#            def _priv_list(list_kwargs):
#                "Exec the statement and return the number of updated rows"
#                with self.transaction() as cursor:
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
    def _one_statement_value(self, stream):
        """
        `stream` SQL statement return a cell
        """
        sql = Tokens2Unicode(stream)

        def _wrapped_method(self, list_or_dict=None, **kwargs):
            "Execute the statement and return its cell value"
            def _priv(kwargs):
                with self.transaction() as cursor:
                    result = cursor.execute(sql, kwargs).fetchone()
                    if result:
                        return result[0]

            def _priv_list(list_kwargs):
                result = []

                with self.transaction() as cursor:
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
    def _one_statement_register(self, stream):
        """
        `stream` SQL statement return a row
        """
        sql = Tokens2Unicode(stream)

        def _wrapped_method(self, list_or_dict=None, **kwargs):
            "Execute the statement and return a row"
            def _priv(kwargs):
                with self.transaction() as cursor:
                    return cursor.execute(sql, kwargs).fetchone()

            def _priv_list(list_kwargs):
                with self.transaction() as cursor:
                    result = []

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
    def _one_statement_table(self, stream):
        """
        `stream` SQL statement return several values (a table)
        """
        sql = Tokens2Unicode(stream)

        def _wrapped_method(self, list_or_dict=None, **kwargs):
            "Execute a statement. If a list is given, they are exec at once"
            def _priv(kwargs):
                with self.transaction() as cursor:
                    return cursor.execute(sql, kwargs).fetchall()

            def _priv_list(list_kwargs):
                result = []

                with self.transaction() as cursor:
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
    def _multiple_statement(self, stream):
        """
        `stream` SQL have several statements (script)
        """
        sql = map(unicode, split2(stream))

        def _wrapped_method(self, list_or_dict=None, **kwargs):
            "Execute the statements sequentially"
            def _priv(kwargs):
                result = []

                with self.transaction() as cursor:
                    for sql_stmt in sql:
                        result.append(cursor.execute(sql_stmt, kwargs))

                return result

            def _priv_list(list_kwargs):
                result = []

                with self.transaction() as cursor:
                    for kwargs in list_kwargs:
                        result2 = []

                        for sql_stmt in sql:
                            result2.append(cursor.execute(sql_stmt, kwargs))

                        result.append(result2)

                return result

            # Received un-named parameter, it would be a iterable
            if list_or_dict != None:
                if isinstance(list_or_dict, dict):
                    return _priv(list_or_dict)
                return _priv_list(list_or_dict)
            return _priv(kwargs)

        return _wrapped_method
