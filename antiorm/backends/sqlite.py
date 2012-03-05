'''
Created on 20/01/2012

@author: piranna
'''

from sqlparse.filters import Tokens2Unicode

from ..base  import register
from ..utils import named2pyformat
from generic import Generic


class Sqlite(Generic):
    "SQLite driver for AntiORM"

    @property
    def row_factory(self):
        return self.connection.row_factory

    @row_factory.setter
    def row_factory(self, value):
        self.connection.row_factory = value

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
