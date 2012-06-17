'''
AntiORM specialized backend for MySQL

Created on 22/04/2012

@author: piranna
'''

from antiorm.base  import Base
from antiorm.utils import TransactionManager


class MySQL(Base):
    """
    MySQL driver for AntiORM
    """
    def __init__(self, db_conn, dir_path=None, bypass_types=False, lazy=False):
        """
        Constructor

        @param db_conn: connection of the database
        @type db_conn: DB-API 2.0 database connection
        @param dir_path: path of the dir with files from where to load SQL code
        @type dir_path: string
        @param bypass_types: set if types should be bypassed on calling
        @type bypass_types: boolean
        @param lazy: set if SQL code at dir_path should be lazy loaded
        @type lazy: boolean
        """
        Base.__init__(self, db_conn, dir_path, bypass_types, lazy)

        self.tx_manager = TransactionManager(db_conn)
