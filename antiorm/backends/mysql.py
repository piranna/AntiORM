'''
Created on 22/04/2012

@author: piranna
'''

from antiorm.base  import Base
from antiorm.utils import _TransactionManager


class MySQL(Base):
    "MySQL driver for AntiORM"

    def __init__(self, db_conn, dir_path=None, lazy=False):
        """Constructor

        @param db_conn: connection of the database
        @type db_conn: DB-API 2.0 database connection
        @param dir_path: path of the dir with files from where to load SQL code
        @type dir_path: string
        @param lazy: set if SQL code at dir_path should be lazy loaded
        @type lazy: boolean
        """
        Base.__init__(self, db_conn, dir_path, lazy)

        self.tx_manager = _TransactionManager(db_conn)