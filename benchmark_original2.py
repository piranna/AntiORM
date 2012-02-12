# -*- coding: utf-8 -*-

from antiorm import AntiORM
import sqlite3
import timeit

db = sqlite3.connect(":memory:")
cursor = db.cursor()
cursor.execute("CREATE TABLE test_table (key TEXT);")

engine = AntiORM(db, "./tests/samples_sql")

def time_funct():
    engine.test_simple_insert(key="hola")

number=10000
t = timeit.Timer(time_funct)
total = t.timeit(number=number)

print "Total: %s seconds\nPartial: %.4f usec/pass" % (
    total, 1000000 * total/number
)
