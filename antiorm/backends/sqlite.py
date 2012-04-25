'''
Created on 20/01/2012

@author: piranna
'''

from sqlparse.filters import Tokens2Unicode

from ..base  import Base, register
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

    @register
    def _multiple_statement_standard(self, stmts, bypass_types):
        """Execute the script optimized using SQLite non-standard method
        executescript() instead of exec the statements sequentially.
        """
        sql = named2pyformat(''.join(stmts))

        def _wrapped_method(self, list_or_dict=None, **kwargs):
            """Use executescript() instead of iterate over the statements

            @param list_or_dict: a (dict | list of dicts) with the parameters
            @type list_or_dict: dict or list of dicts

            @return: the procesed data from the SQL query
            """
            def _priv(kwargs):
                with self.tx_manager as conn:
                    cursor = conn.cursor()

                    return cursor.executescript(sql % kwargs)

            def _priv_list(list_kwargs):
                result = []
                with self.tx_manager as conn:
                    cursor = conn.cursor()

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
