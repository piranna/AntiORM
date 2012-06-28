'''
APSWdb - A DB-API 2.0 wrapper for APSW inspired by MySQLdb module

Created on 18/06/2012

@author: piranna
'''

#from apsw import
import apsw


apilevel = '2.0'
threadsafety =
paramstyle = 'qmark'



def connect():
    pass


class Connection:
    '''
    classdocs
    '''

    def __init__(self):
        '''
        Constructor
        '''

    def close(self):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass

    def cursor(self):
        pass


class Cursor:
    arraysize = 1

    @property
    def description(self):
        pass

    @property
    def rowcount(self):
        pass

    def close(self):
        pass

    def execute(operation, parameters=None):
        pass

    def executemany(operation, seq_of_parameters):
        pass

    def fetchone(self):
        pass

    def fetchmany(size=cursor.arraysize):
        pass

    def fetchall(self):
        pass

    def setinputsizes(sizes):
        pass

    def setoutputsize(size, column=None):
        pass