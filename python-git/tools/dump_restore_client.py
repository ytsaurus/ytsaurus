from datetime import datetime

import yt.wrapper.common as yt_common
import yt.tools.dynamic_tables as dt_module

from yt.packages.six import iteritems

# Attribute prefix
ATTRIBUTE_PREFIX = "_yt_dump_restore_"


def _drop_sort_order(schema):
    """
    Drops sort_order from columns to make schema comparision
    simplier (but key columns must be then compared separately)
    """
    result = []
    for column in schema:
        result_column = dict(column)  # copies it
        if "sort_order" in column:
            del result_column["sort_order"]
        result.append(result_column)
    return result


def save_attributes(dst, yt, **attributes):
    """ Save table properties to simple attributes """
    for attr, value in iteritems(attributes):
        yt.set(dst + "/@" + ATTRIBUTE_PREFIX + attr, value)


def restore_attributes(src, yt, attributes):
    """ Reverse of the `save_attributes`: load requested table
    properties from simple attributes """
    result = []
    for attr in attributes:
        result.append(yt.get(src + "/@" + ATTRIBUTE_PREFIX + attr))
    return tuple(result)


def create_destination(dst, yt, force=False, attributes=None):
    """ Create the destination table (force-overwrite logic) """
    if yt.exists(dst):
        if force:
            yt.remove(dst)
        else:
            raise Exception("Destination table exists. Use --force")
    yt.create_table(dst, attributes=attributes)


def check_table_schema(dst, schema, key_columns, optimize_for, yt):
    """ Check that table exists and has specified schema """
    if not yt.exists(dst):
        raise Exception("Destination table does not exists")
    real_schema, real_key_columns, real_optimize_for = dt_module.get_dynamic_table_attributes(yt, dst)
    if _drop_sort_order(real_schema) != _drop_sort_order(schema):
        raise Exception("Destination table schema does not match")
    if real_key_columns != key_columns:
        raise Exception("Destination table key columns does not match")
    if optimize_for != real_optimize_for:
        raise Exception("Destination table optimize_for mode does not match")


class DumpRestoreClient(dt_module.DynamicTablesClient):
    def dump_table(self, src_table, dst_table, force=False, predicate=None):
        """ Dump dynamic table rows matching the predicate into a static table """
        schema, key_columns, optimize_for = dt_module.get_dynamic_table_attributes(self.yt, src_table)
        tablets = self.yt.get(src_table + "/@tablets")
        pivot_keys = sorted([tablet["pivot_key"] for tablet in tablets])

        static_schema = []
        for column in schema:
            if "expression" in column:
                continue
            static_column = {"name": column["name"], "type": column["type"]}
            if "group" in column:
                static_column["group"] = column["group"]
            static_schema.append(static_column)

        attributes = {
            "schema": static_schema,
            "optimize_for": optimize_for
        }
        create_destination(dst_table, yt=self.yt, force=force, attributes=attributes)
        save_attributes(
            dst_table,
            origin=src_table,
            schema=schema,
            key_columns=key_columns,
            pivot_keys=pivot_keys,
            time=yt_common.datetime_to_string(datetime.utcnow()),
            predicate=predicate,
            yt=self.yt)

        self.run_map_over_dynamic(
            None,  # empty mapper, i.e. dump not modified rows
            src_table,
            dst_table,
            predicate=predicate)

    def restore_table(self, src_table, dst_table, force=False, to_existing=False):
        """ Insert static table rows into a new dynamic table """
        schema, key_columns, pivot_keys = restore_attributes(
            src_table,
            yt=self.yt,
            attributes=("schema", "key_columns", "pivot_keys"))
        optimize_for = self.yt.get_attribute(src_table, "optimize_for", default="lookup")

        if to_existing:
            check_table_schema(dst_table, schema, key_columns, optimize_for, yt=self.yt)
        else:
            create_destination(
                dst_table,
                yt=self.yt,
                force=force,
                attributes=dt_module.make_dynamic_table_attributes(self.yt, schema, key_columns, optimize_for))
            self.yt.reshard_table(dst_table, pivot_keys)
            self.mount_table(dst_table)

        self.run_map_dynamic(
            None,  # empty mapper, i.e. restore not modified rows
            src_table,
            dst_table)

    def erase_table(self, table, predicate=None):
        """ Delete all rows matching the predicate from a dynamic table """
        _, key_columns, _ = dt_module.get_dynamic_table_attributes(self.yt, table)

        def erase_mapper(rows):
            client = self.make_driver_yt_client()

            def make_deleter(rowset):
                def do_delete():
                    client.delete_rows(table, rowset, raw=False)

                return do_delete

            for rowset in dt_module.split_in_groups(rows, self.batch_size):
                do_delete_these = make_deleter(rowset)
                yt_common.run_with_retries(do_delete_these, except_action=dt_module.log_exception)

            if False:  # make this function into a generator
                yield

        with self.yt.TempTable() as out_table:
            self.run_map_over_dynamic(
                erase_mapper,
                table,
                out_table,
                columns=key_columns,
                predicate=predicate)

    def get_schema_and_key_columns(self, table):
        """
        Unified way to get schema and key columns info (either from a real
        dynamic table or from attributes saved with table dump)
        """
        if self.yt.get_attribute(table, "dynamic"):
            schema, key_columns, optimize_for = dt_module.get_dynamic_table_attributes(self.yt, table)
        else:
            schema, key_columns = restore_attributes(
                table,
                yt=self.yt,
                attributes=("schema", "key_columns"))
        return _drop_sort_order(schema), key_columns
