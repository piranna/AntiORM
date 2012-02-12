'''
Created on 20/01/2012

@author: piranna
'''

from sqlparse.filters import Tokens2Unicode

from ..utils import named2pyformat
from .. import AntiORM


class Sqlite(AntiORM):
    def _multiple_statement(self, stream, method_name):
        sql = named2pyformat(Tokens2Unicode(stream))

        def _wrapped_method(self, **kwargs):
            with self.transaction() as cursor:
                cursor.executescript(sql % kwargs)

        setattr(self.__class__, method_name, _wrapped_method)
