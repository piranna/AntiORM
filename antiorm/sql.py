'''
Created on 01/08/2011

@author: piranna
'''


from sqlparse.engine import FilterStack
from sqlparse.filters import ColumnsSelect, IncludeStatement, Limit
from sqlparse.filters import StripComments, SerializerUnicode
from sqlparse.filters import StripWhitespaceFilter
from sqlparse.lexer import tokenize
from sqlparse.pipeline import Pipeline
from sqlparse.tokens import Keyword, Whitespace


def Compact(sql, includePath="sql"):
    """Function that return a compacted version of the input SQL query"""
    stack = FilterStack()

    stack.preprocess.append(IncludeStatement(includePath))
    stack.preprocess.append(StripComments())
    stack.stmtprocess.append(StripWhitespaceFilter())
    stack.postprocess.append(SerializerUnicode())

    return ''.join(stack.run(sql))


def GetLimit(sql):
    """Function that return the LIMIT of a input SQL """
    stack = FilterStack()

    stack.preprocess.append(Limit())

    result = stack.run(sql)
    try:
        return int(result)
    except ValueError:
        return result


def GetColumns(sql):
    """Function that return the colums of a SELECT query"""
    stack = FilterStack()

    stack.preprocess.append(ColumnsSelect())

    return list(stack.run(sql))


class IsType():
    def __init__(self, type):
        self.type = type

    def process(self, stack, stream):
        for token_type, value in stream:
            if token_type in Whitespace: continue
            return token_type in Keyword and value == self.type


def FirstIsInsert(sql):
    """Function that return is the statement is a SELECT query"""
    stack = FilterStack()

    stack.preprocess.append(IsType('INSERT'))

    return stack.run(sql)


if __name__ == '__main__':
    import sqlparse

    from sqlparse.engine import FilterStack
    from sqlparse.filters import KeywordCaseFilter

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

#    print sqlparse.split(sql3)

#    print sqlparse.parse(sql2)[0].tokens
#    print sqlparse.parse(sql)[0].tokens[1].tokens
#    print sqlparse.parse(sql)[0].tokens[1].tokens[0]

#    print sqlparse.parse(sql)[0].tokens[1].tokens[2].tokens
#    print sqlparse.parse(sql)[0].tokens[1].tokens[2].tokens[0]

#    print sqlparse.parse(sql)[1].tokens

    print repr(GetColumns(sql))
    print repr(GetColumns(sql2))
    print repr(GetColumns(sql3))

#    stack = FilterStack()

#    stack.preprocess.append(KeywordCaseFilter('upper')) # uppercase keywords too
#    stack.stmtprocess.append(StripCommentsFilter())
#    stack.preprocess.append(Compact())
#    stack.preprocess.append(Get_Comments())

#    stack.preprocess.append(IncludeStatement("sql"))
#    stack.preprocess.append(StripComments())
#    stack.stmtprocess.append(StripWhitespaceFilter())
#    stack.postprocess.append(SerializerUnicode())
#
#    print ''.join(stack.run(sql))