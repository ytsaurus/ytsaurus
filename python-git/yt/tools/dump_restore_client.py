from yt.wrapper.common import run_with_retries
from yt.tools.dynamic_tables import DynamicTablesClient, \
    get_schema_and_key_columns, get_dynamic_table_attributes, \
    split_in_groups, log_exception

# Attribute prefix
ATTRIBUTE_PREFIX = "_yt_dump_restore_"


def save_attributes(dst, schema, key_columns, pivot_keys, yt):
    """ Save dynamic table properties to simple attributes """
    yt.set(dst + "/@" + ATTRIBUTE_PREFIX + "schema", schema)
    yt.set(dst + "/@" + ATTRIBUTE_PREFIX + "key_columns", key_columns)
    yt.set(dst + "/@" + ATTRIBUTE_PREFIX + "pivot_keys", pivot_keys)


def restore_attributes(src, yt):
    """ Reverse of the `save_attributes`: load dynamic table
    properties from simple attributes """
    schema = yt.get(src + "/@" + ATTRIBUTE_PREFIX + "schema")
    key_columns = yt.get(src + "/@" + ATTRIBUTE_PREFIX + "key_columns")
    pivot_keys = yt.get(src + "/@" + ATTRIBUTE_PREFIX + "pivot_keys")
    return schema, key_columns, pivot_keys


def create_destination(dst, yt, force=False, attributes=None):
    """ Create the destination table (force-overwrite logic) """
    if yt.exists(dst):
        if force:
            yt.remove(dst)
        else:
            raise Exception("Destination table exists. Use --force")
    yt.create_table(dst, attributes=attributes)


class DumpRestoreClient(DynamicTablesClient):
    def dump_table(self, src_table, dst_table, force=False, predicate=None):
        """ Dump dynamic table rows matching the predicate into a static table """
        schema, key_columns = get_schema_and_key_columns(self.yt, src_table)
        tablets = self.yt.get(src_table + "/@tablets")
        pivot_keys = sorted([tablet["pivot_key"] for tablet in tablets])

        create_destination(dst_table, yt=self.yt, force=force)
        save_attributes(dst_table, schema, key_columns, pivot_keys, yt=self.yt)

        self.run_map_over_dynamic(
            None,  # empty mapper, i.e. dump not modified rows
            src_table,
            dst_table,
            predicate=predicate)

    def restore_table(self, src_table, dst_table, force=False):
        """ Insert static table rows into a new dynamic table """
        schema, key_columns, pivot_keys = restore_attributes(src_table, yt=self.yt)

        create_destination(
            dst_table,
            yt=self.yt,
            force=force,
            attributes=get_dynamic_table_attributes(self.yt, schema, key_columns))
        self.yt.reshard_table(dst_table, pivot_keys)
        self.mount_table(dst_table)

        self.run_map_dynamic(
            None,  # empty mapper, i.e. restore not modified rows
            src_table,
            dst_table)

    def erase_table(self, table, predicate=None):
        """ Delete all rows matching the predicate from a dynamic table """
        _, key_columns = get_schema_and_key_columns(self.yt, table)

        def erase_mapper(rows):
            client = self.make_driver_yt_client()

            def make_deleter(rowset):
                def do_delete():
                    client.delete_rows(table, rowset, raw=False)

                return do_delete

            for rowset in split_in_groups(rows, self.batch_size):
                do_delete_these = make_deleter(rowset)
                run_with_retries(do_delete_these, except_action=log_exception)

            if False:  # make this function into a generator
                yield

        with self.yt.TempTable() as out_table:
            self.run_map_over_dynamic(
                erase_mapper,
                table,
                out_table,
                columns=key_columns,
                predicate=predicate)
