'''
Created on 17/02/2012

@author: piranna
'''

from sys import stderr

from .. import AntiORM


class APSW(AntiORM):
    '''
    classdocs
    '''

    def __init__(self, db_conn, dir_path=None, lazy=False):
        '''
        Constructor
        '''
        self._cachedmethods = 0

        AntiORM.__init__(self, db_conn, dir_path, lazy)

    def parse_string(self, sql, method_name, include_path='sql', lazy=False):
        """Build a function from a string containing a SQL query

        If the number of parsed methods is bigger of the APSW cache it shows an
        alert.
        """
        try:
            AntiORM.parse_string(self, sql, method_name, include_path, lazy)
        except:
            raise
        else:
            self._cachedmethods += 1
            if self._cachedmethods > 100:
                print >> stderr, ("Surpased APSW cache size (%s > %s)"
                                  % (self._cachedmethods, 100))
