'''
Created on 13/02/2012

@author: piranna
'''
from unittest import main, TestCase

from sqlparse.filters import Tokens2Unicode
from sqlparse.lexer   import tokenize

import sys
sys.path.insert(0, '..')

from antiorm.sql import Compact, GetColumns, GetLimit


class Test_SQL(TestCase):
    sql = """-- type: script
            -- return: integer

            INCLUDE "Direntry.make.sql";

            INSERT INTO directories(inode)
                            VALUES(:inode)
            LIMIT 1"""

    sql2 = """SELECT child_entry,asdf AS inode, creation
              FROM links
              WHERE parent_dir == :parent_dir AND name == :name
              LIMIT 1"""

    sql3 = """SELECT
    0 AS st_dev,
    0 AS st_uid,
    0 AS st_gid,

    dir_entries.type         AS st_mode,
    dir_entries.inode        AS st_ino,
    COUNT(links.child_entry) AS st_nlink,

    :creation                AS st_ctime,
    dir_entries.access       AS st_atime,
    dir_entries.modification AS st_mtime,
--    :creation                                                AS st_ctime,
--    CAST(STRFTIME('%s',dir_entries.access)       AS INTEGER) AS st_atime,
--    CAST(STRFTIME('%s',dir_entries.modification) AS INTEGER) AS st_mtime,

    COALESCE(files.size,0) AS st_size, -- Python-FUSE
    COALESCE(files.size,0) AS size     -- PyFilesystem

FROM dir_entries
    LEFT JOIN files
        ON dir_entries.inode == files.inode
    LEFT JOIN links
        ON dir_entries.inode == links.child_entry

WHERE dir_entries.inode == :inode

GROUP BY dir_entries.inode
LIMIT 1"""


class Test_Compact(Test_SQL):
    def test_compact1(self):
        self.assertEqual(Tokens2Unicode(Compact(self.sql)),
            'INCLUDE "Direntry.make.sql";INSERT INTO directories(inode) VALUES'
            '(:inode) LIMIT 1')

    def test_compact2(self):
        self.assertEqual(Tokens2Unicode(Compact(self.sql2)),
            'SELECT child_entry,asdf AS inode,creation FROM links WHERE '
            'parent_dir == :parent_dir AND name == :name LIMIT 1')

    def test_compact3(self):
        self.assertEqual(Tokens2Unicode(Compact(self.sql3)),
            'SELECT 0 AS st_dev,0 AS st_uid,0 AS st_gid,dir_entries.type AS '
            'st_mode,dir_entries.inode AS st_ino,COUNT(links.child_entry) AS '
            'st_nlink,:creation AS st_ctime,dir_entries.access AS st_atime,'
            'dir_entries.modification AS st_mtime,COALESCE(files.size,0) AS '
            'st_size,COALESCE(files.size,0) AS size FROM dir_entries LEFT JOIN'
            ' files ON dir_entries.inode == files.inode LEFT JOIN links ON '
            'dir_entries.inode == links.child_entry WHERE dir_entries.inode =='
            ' :inode GROUP BY dir_entries.inode LIMIT 1')


class Test_GetColumns(Test_SQL):
    def test_getlimit1(self):
        self.assertEqual(Tokens2Unicode(GetColumns(self.sql)), [])

    def test_getlimit2(self):
        self.assertEqual(Tokens2Unicode(GetColumns(self.sql2)),
                         ('child_entry', 'inode', 'creation'))

    def test_getlimit3(self):
        self.assertEqual(Tokens2Unicode(GetColumns(self.sql3)),
                         ('st_dev', 'st_uid', 'st_gid', 'st_mode', 'st_ino',
                          'st_nlink', 'st_ctime', 'st_atime', 'st_mtime',
                          'st_size', 'size'))


class Test_GetLimit(Test_SQL):
    def test_getlimit1(self):
        limit = GetLimit(tokenize(self.sql))
        self.assertEqual(limit, 1)

    def test_getlimit2(self):
        limit = GetLimit(tokenize(self.sql2))
        self.assertEqual(limit, 1)

    def test_getlimit3(self):
        limit = GetLimit(tokenize(self.sql3))
        self.assertEqual(limit, 1)


#class Test_IsType(Test_SQL):
#    def test_getlimit1(self):
#        self.assertEqual(Tokens2Unicode(IsType(self.sql)), 1)
#
#    def test_getlimit2(self):
#        self.assertEqual(Tokens2Unicode(IsType(self.sql2)), 1)
#
#    def test_getlimit3(self):
#        self.assertEqual(Tokens2Unicode(IsType(self.sql3)), 1)


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    main()