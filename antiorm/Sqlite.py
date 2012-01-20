'''
Created on 20/01/2012

@author: piranna
'''

from sqlparse.filters import Tokens2Unicode

from AntiORM import AntiORM, S2SF, _transaction


class Sqlite(AntiORM):
    def _multipleStatement(self, stream, methodName):
        def applyMethod(sql, methodName):
            @_transaction
            def method(self, **kwargs):
                self.cursor.executescript(sql % kwargs)

            setattr(self.__class__, methodName, method)

        applyMethod(S2SF(Tokens2Unicode(stream)), methodName)
