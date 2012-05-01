'''
Created on 20/01/2012

@author: piranna
'''

from ..base  import Base, proxy_factory
from ..utils import named2pyformat


class Sqlite(Base):
    "SQLite driver for AntiORM"

    def __init__(self, db_conn, dir_path=None, lazy=False, bypass_types=False):
        """Constructor

        @param db_conn: connection of the database
        @type db_conn: DB-API 2.0 database connection
        @param dir_path: path of the dir with files from where to load SQL code
        @type dir_path: string
        @param lazy: set if SQL code at dir_path should be lazy loaded
        @type lazy: boolean
        """
        Base.__init__(self, db_conn, dir_path, lazy, bypass_types)

        self.tx_manager = db_conn

    def _multiple_statement_standard__dict(self, stmts):
        sql = named2pyformat(''.join(stmts))

        def _wrapped_method(_, kwargs):
            with self.tx_manager as conn:
                cursor = conn.cursor()

                return cursor.executescript(sql % kwargs)

        return _wrapped_method

    def _multiple_statement_standard__list(self, stmts):
        sql = named2pyformat(''.join(stmts))

        def _wrapped_method(_, list_kwargs):
            result = []
            with self.tx_manager as conn:
                cursor = conn.cursor()

                for kwargs in list_kwargs:
                    result.append(cursor.executescript(sql % kwargs))
            return result

        return _wrapped_method

    _multiple_statement_standard = proxy_factory(_multiple_statement_standard__dict,
                                                 _multiple_statement_standard__list)
