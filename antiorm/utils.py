# -*- coding: utf-8 -*-

from re import sub


def TupleObj_factory(cursor, row):
    "Create a TupleObj from a DB-API 2.0 cursor description and its values"
    t = TupleObj(cell for cell in row)
    for idx, col in enumerate(cursor.description):
        setattr(t, col[0], t[idx])
    return t


def named2pyformat(sql):
    "Convert from 'named' paramstyle format to Python string 'pyformat' format"
    return sub(":\w+", lambda m: "%%(%s)s" % m.group(0)[1:], sql)


class TupleObj(tuple):
    "Tuple that allow access to its elements as object attributes"
    pass

