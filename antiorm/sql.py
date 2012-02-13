'''
Created on 01/08/2011

@author: piranna
'''


from sqlparse.filters import ColumnsSelect, IncludeStatement, Limit
from sqlparse.filters import StripComments, SerializerUnicode
from sqlparse.filters import StripWhitespaceFilter
from sqlparse.lexer import tokenize
from sqlparse.pipeline import Pipeline
from sqlparse.tokens import Keyword, Whitespace


def Compact(sql, includePath="sql"):
    """Function that return a compacted version of the input SQL query"""
    pipe = Pipeline()

    pipe.append(tokenize)
    pipe.append(IncludeStatement(includePath))
    pipe.append(StripComments())
#    pipe.append(StripWhitespaceFilter())
#    pipe.append(SerializerUnicode())

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


if __name__ == '__main__':
    import sqlparse

    from sqlparse.engine import FilterStack
    from sqlparse.filters import KeywordCaseFilter


#    print sqlparse.split(sql3)

#    print sqlparse.parse(sql2)[0].tokens
#    print sqlparse.parse(sql)[0].tokens[1].tokens
#    print sqlparse.parse(sql)[0].tokens[1].tokens[0]

#    print sqlparse.parse(sql)[0].tokens[1].tokens[2].tokens
#    print sqlparse.parse(sql)[0].tokens[1].tokens[2].tokens[0]

#    print sqlparse.parse(sql)[1].tokens


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