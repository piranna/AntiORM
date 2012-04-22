'''
Created on 01/08/2011

@author: piranna

Several utility functions and custom filters for sqlparse to help on the parsing
of the SQL sentences
'''


from sqlparse.filters  import ColumnsSelect, IncludeStatement, Limit
from sqlparse.filters  import StripComments, StripWhitespace
from sqlparse.lexer    import tokenize
from sqlparse.pipeline import Pipeline
from sqlparse.tokens   import Keyword, Whitespace


def Compact(sql, includePath="sql"):
    """Function that return a compacted version of the input SQL query"""
    pipe = Pipeline()

    pipe.append(tokenize)
    pipe.append(IncludeStatement(includePath))
    pipe.append(StripComments())
    pipe.append(StripWhitespace)

    return pipe(sql)


def GetLimit(stream):
    """Function that return the LIMIT of a input SQL """
    pipe = Pipeline()

    pipe.append(Limit())

    result = pipe(stream)
    try:
        return int(result)
    except ValueError:
        return result


def GetColumns(stream):
    """Function that return the colums of a SELECT query"""
    pipe = Pipeline()

    pipe.append(ColumnsSelect())

    return pipe(stream)


class IsType():
    """Functor that return is the statement is of a specific type"""
    def __init__(self, type):
        self.type = type

    def __call__(self, stream):
        for token_type, value in stream:
            if token_type in Whitespace: continue
            return token_type in Keyword and value == self.type
