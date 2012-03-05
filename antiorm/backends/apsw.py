'''
Created on 17/02/2012

@author: piranna
'''

from logging import warning

from .. import AntiORM


class APSW(AntiORM):
    "APSW driver for AntiORM"
    _max_cachedmethods = 100

    def __init__(self, db_conn, dir_path=None, lazy=False):
        """Constructor

        @param db_conn: connection of the database
        @type db_conn: DB-API 2.0 database connection
        @param dir_path: path of the dir with files from where to load SQL code
        @type dir_path: string
        @param lazy: set if SQL code at dir_path should be lazy loaded
        @type lazy: boolean
        """
        self._cachedmethods = 0

        AntiORM.__init__(self, db_conn, dir_path, lazy)

    @property
    def row_factory(self):
        return self.connection.getrowtrace()

    @row_factory.setter
    def row_factory(self, value):
        self.connection.setrowtrace(value)

    def parse_string(self, sql, method_name, include_path='sql', lazy=False):
        """Build a function from a string containing a SQL query

        If the number of parsed methods is bigger of the APSW SQLite bytecode
        cache it shows an alert because performance will decrease.
        """
        try:
            result = AntiORM.parse_string(self, sql, method_name, include_path,
                                          lazy)
        except:
            raise

        self._cachedmethods += 1
        if self._cachedmethods > self._max_cachedmethods:
            warning("Surpased APSW cache size (methods: %s; limit: %s)",
                    (self._cachedmethods, self._max_cachedmethods))

        return result
