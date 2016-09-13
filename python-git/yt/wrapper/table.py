from .ypath import TablePath

from contextlib import contextmanager

@contextmanager
def TempTable(path=None, prefix=None, client=None):
    """Create temporary table in given path with given prefix on scope enter and remove it on scope exit.
       .. seealso:: :py:func:`yt.wrapper.table_commands.create_temp_table`
    """
    from .cypress_commands import remove
    from .table_commands import create_temp_table

    table = create_temp_table(path, prefix, client=client)
    try:
        yield table
    finally:
        remove(table, force=True, client=client)

def to_table(object, client=None):
    """ DEPRECATED """
    """Return `TablePath` object"""
    return TablePath(object, client=client)

def to_name(object, client=None):
    """ DEPRECATED """
    """Return `YsonString` name of path"""
    return to_table(object, client=client).to_yson_type()

