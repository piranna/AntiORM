'''
Created on 20/01/2012

@author: piranna
'''

from sqlparse.filters import Tokens2Unicode

from ..utils import named2pyformat
from generic import Generic, register


class Sqlite(Generic):
    "SQLite driver for AntiORM"

    @register
    def _multiple_statement(self, stream):
        """Execute the script optimized using SQLite non-standard method
        executescript() instead of exec the statements sequentially.
        """
        sql = named2pyformat(Tokens2Unicode(stream))

        def _priv_dict(kwargs):
            with self.transaction() as cursor:
                return cursor.executescript(sql % kwargs)

        def _priv_list(list_kwargs):
            result = []

            with self.transaction() as cursor:
                for kwargs in list_kwargs:
                    result.append(cursor.executescript(sql % kwargs))

            return result

        def _wrapped_method(self, list_or_dict=None, **kwargs):
            """Use executescript() instead of iterate over the statements

            @param list_or_dict: a (dict | list of dicts) with the parameters
            @type list_or_dict: dict or list of dicts

            @return: the procesed data from the SQL query
            """
            # Received un-named parameter, it would be a iterable
            if list_or_dict != None:
                if isinstance(list_or_dict, dict):
                    return _priv_dict(list_or_dict)
                return _priv_list(list_or_dict)
            return _priv_dict(kwargs)

        return _wrapped_method
