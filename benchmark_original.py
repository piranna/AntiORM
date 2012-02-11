# -*- coding: utf-8 -*-

from antiorm import AntiORM
import sqlite3
import timeit

def time_funct():
    db = sqlite3.connect(":memory:")
    engine = AntiORM(db, "./tests/samples_sql")


number=10000
t = timeit.Timer(time_funct)
total = t.timeit(number=number)

print "Total: %s seconds\nPartial: %.4f usec/pass" % (
    total, 1000000 * total/number
)
