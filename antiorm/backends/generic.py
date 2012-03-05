'''
Created on 05/03/2012

@author: piranna
'''

from sqlparse import split2

from ..base  import Base, register


class Generic(Base):
    """Generic driver for AntiORM.

    Using this should be enought for any project, but it's recomended to use a
    specific driver for your type of database connection to be able to use some
    optimizations.
    """

    @register
    def _multiple_statement(self, stream):
        """
        `stream` SQL have several statements (script)
        """
        sql = map(unicode, split2(stream))

        def _wrapped_method(self, **kwargs):
            "Execute the statements sequentially"
            with self.transaction() as cursor:
                for sql_stmt in sql:
                    cursor.execute(sql_stmt, kwargs)

        return _wrapped_method
