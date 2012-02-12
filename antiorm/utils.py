# -*- coding: utf-8 -*-

from re import sub


def DictObj_factory(cursor, row):
    d = DictObj()
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def named2pyformat(sql):
    "Convert from 'named' paramstyle format to Python string 'pyformat' format"
    return sub(":\w+", lambda m: "%%(%s)s" % m.group(0)[1:], sql)


class DictObj(dict):
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)

    def __setattr__(self, name, value):
        try:
            self[name] = value
        except KeyError:
            raise AttributeError(name)
