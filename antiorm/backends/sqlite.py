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

        def _wrapped_method(self, list_or_dict=None, **kwargs):
            """Use executescript() instead of iterate over the statements

            @param list_or_dict: a (dict | list of dicts) with the parameters
            @type list_or_dict: dict or list of dicts

            @return: the procesed data from the SQL query
            """
            def _priv(kwargs):
                with self.tx_manager as cursor:
                    return cursor.executescript(sql % kwargs)

            def _priv_list(list_kwargs):
                result = []
                with self.tx_manager as cursor:
                    for kwargs in list_kwargs:
                        result.append(cursor.executescript(sql % kwargs))
                return result

            # Received un-named parameter, it would be a iterable
            if list_or_dict != None:
                if isinstance(list_or_dict, dict):
                    return _priv(list_or_dict)
                return _priv_list(list_or_dict)
            return _priv(kwargs)

        return _wrapped_method
