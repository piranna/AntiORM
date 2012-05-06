'''
Created on 05/03/2012

@author: piranna
'''

from antiorm.backends.apsw import ConnectionWrapper
from antiorm.base          import Base
from antiorm.utils         import _TransactionManager


class Generic(Base):
    """Generic driver for AntiORM.

    Using this should be enought for any project, but it's recomended to use a
    specific driver for your type of database connection to be able to use some
    optimizations.
    """

    def __init__(self, db_conn, dir_path=None, bypass_types=False, lazy=False):
        """Constructor

        @param db_conn: connection of the database
        @type db_conn: DB-API 2.0 database connection
        @param dir_path: path of the dir with files from where to load SQL code
        @type dir_path: string
        @param lazy: set if SQL code at dir_path should be lazy loaded
        @type lazy: boolean
        """
        # Check if database connection is from APSW so we force the wrapper
        if db_conn.__class__.__module__ == 'apsw':
            db_conn = ConnectionWrapper(db_conn)

        Base.__init__(self, db_conn, dir_path, bypass_types, lazy)

        self.tx_manager = _TransactionManager(db_conn)
