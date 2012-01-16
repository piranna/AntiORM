from AntiORM import AntiORM


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


def DictObj_factory(cursor, row):
    d = DictObj()
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def ChunkConverted(chunk):
    """[Hack] None objects get converted to 'None' while SQLite queries
    expect 'NULL' instead. This function return a newly dict with
    all None objects converted to 'NULL' valued strings.
    """
    d = {}
    for key, value in chunk.iteritems():
        d[key] = "NULL" if value == None else value
    return d
