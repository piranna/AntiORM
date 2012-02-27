'''
Created on 20/01/2012

@author: piranna
'''

from sqlparse.filters import Tokens2Unicode

from ..utils import named2pyformat
from .. import AntiORM


def register(func):
    def wrapper(self, method_name, *args, **kwargs):
        setattr(self.__class__, method_name, func(self, *args, **kwargs))
    return wrapper


class Sqlite(AntiORM):
    "SQLite driver for AntiORM"

    @register
    def _multiple_statement(self, stream):
        """Execute the script optimized using SQLite non-standard method
        executescript() instead of exec the statements sequentially.
        """
        sql = named2pyformat(Tokens2Unicode(stream))

        def _wrapped_method(self, **kwargs):
            "Use executescript() instead of iterate over the statements"
            with self.transaction() as cursor:
                cursor.executescript(sql % kwargs)

        return _wrapped_method
