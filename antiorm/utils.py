# -*- coding: utf-8 -*-

from collections import namedtuple
from re          import sub

# Factory classes
from .backends._apsw   import APSW
from .backends.generic import Generic
from .backends.sqlite  import Sqlite


def Namedtuple_factory(cursor, row):
    "Create a namedtuple from a DB-API 2.0 cursor description and its values"

    try:
        description = cursor.description

    # APSW
    except AttributeError:
        description = cursor.getdescription()

    return namedtuple('namedtuple', (col[0] for col in description))(*row)


def named2pyformat(sql):
    "Convert from 'named' paramstyle format to Python string 'pyformat' format"
    return sub(":\w+", lambda m: "%%(%s)s" % m.group(0)[1:], sql)


def driver_factory(db_conn, *args, **kwargs):
    type_conn = db_conn.__module__

    if type_conn == 'apsw':
        print 'apsw'
        return APSW(db_conn, *args, **kwargs)

    if type_conn == 'sqlite3':
        print 'sqlite3'
        return Sqlite(db_conn, *args, **kwargs)

    print 'generic'
    return Generic(db_conn, *args, **kwargs)
