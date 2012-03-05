# -*- coding: utf-8 -*-

from collections import namedtuple
from re          import sub


def Namedtuple_factory(cursor, row):
    "Create a namedtuple from a DB-API 2.0 cursor description and its values"

    return namedtuple('namedtuple',
                      (col[0] for col in cursor.description))(*row)


def named2pyformat(sql):
    "Convert from 'named' paramstyle format to Python string 'pyformat' format"
    return sub(":\w+", lambda m: "%%(%s)s" % m.group(0)[1:], sql)
