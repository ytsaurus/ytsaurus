from contextlib import contextmanager


@contextmanager
def TempTable(path=None, prefix=None, attributes=None, expiration_timeout=None, client=None):
    """Creates temporary table in given path with given prefix on scope enter and \
       removes it on scope exit.

       .. seealso:: :func:`create_temp_table <yt.wrapper.table_commands.create_temp_table>`
    """
    from .cypress_commands import remove
    from .table_commands import create_temp_table

    table = create_temp_table(path, prefix, attributes, expiration_timeout, client=client)
    try:
        yield table
    finally:
        remove(table, force=True, client=client)
