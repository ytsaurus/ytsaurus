# coding: utf8

# NOTE: uppercase to distinguish the global one
import yt.wrapper as yt_module
import yt.yson as yson
import sys
import time
import inspect
import logging
import itertools as it

from yt.common import YtError, update, set_pdeathsig
from yt.wrapper.common import run_with_retries
from yt.wrapper.client import Yt
from random import randint, shuffle

# XXXX/TODO: global stuff. Find a way to avoid this.
yt_module.config["pickling"]["module_filter"] = lambda module: not hasattr(module, "__file__") or "yt_driver_bindings" not in module.__file__


class DynamicTablesClient(object):

    # Defaults:

    # Mapper job options.
    job_count = None
    # maximum number of simultaneously running jobs.
    # (supposedly only applied if a pool is used)
    user_slots = None
    # maximum amount of memory allowed for a job
    job_memory_limit = None
    # maximum number of failed jobs which doesn't imply operation failure.
    max_failed_job_count = None
    # data size per job
    data_size_per_job = None
    # maximum number of output rows
    output_row_limit = 100000000
    # maximum number of input rows
    input_row_limit = 100000000
    # Pool name
    pool = None
    # Workload descriptor (batch/realtime)
    workload_descriptor = None

    default_client_config = {
        "driver_config_path": "/etc/ytdriver.conf",
        "api_version": "v3"
    }

    # Nice yson format
    yson_format = yt_module.YsonFormat(
        boolean_as_string=False, process_table_index=False)

    proxy = None
    token = None
    yt = None

    def __init__(self, **options):
        self.set_options(**options)

    def set_options(self, **options):
        for name, val in options.items():
            if not hasattr(self, name):
                raise Exception("Unknown option: %s" % (name,))
            if val is None:
                continue
            setattr(self, name, val)
        # Rebuild the yt client in case the options changed (or when instaitiating)
        self.yt = self.make_proxy_yt_client()
        self.yt.config["pickling"]["module_filter"] = lambda module: not hasattr(module, "__file__") or "yt_driver_bindings" not in module.__file__

    @property
    def overridable_proxy(self):
        """ A more conveniently-overridable yt proxy value (includes the global client fallback) """
        return self.proxy or yt_module.config["proxy"]["url"]

    @property
    def overridable_token(self):
        """ A more conveniently-overridable yt token value (includes the global client fallback) """
        return self.token or yt_module.config["token"]

    def make_proxy_yt_client(self, **kwargs):
        kwargs["config"] = dict(
            self.default_client_config.items() +
            (kwargs.get("config") or {}).items())
        kwargs.setdefault("proxy", self.overridable_proxy)
        kwargs.setdefault("token", self.overridable_token)
        return Yt(**kwargs)

    def make_driver_yt_client(self):
        """ Make an Yt instnatiated client with no proxy/token options
        (to go from a job to the same cluster) """
        return Yt(config=self.default_client_config)

    @staticmethod
    def log_exception(ex):
        msg = (
            "Execution failed with error: \n"
            "%(ex)s\n"
            "Retrying..."
        ) % dict(ex=ex)
        sys.stderr.write(msg)
        sys.stderr.flush()

    def build_spec_from_options(self):
        """ Build map operation spec, e.g. from command line args """
        spec = {
            "enable_job_proxy_memory_control": False,
            "job_proxy_memory_control": False}

        if self.job_count:
            spec["job_count"] = self.job_count
        if self.user_slots:
            spec["resource_limits"] = {"user_slots": self.user_slots}
        if self.job_memory_limit:
            spec["mapper"] = {"memory_limit": self.job_memory_limit}
        if self.max_failed_job_count:
            spec["max_failed_job_count"] = self.max_failed_job_count
        if self.data_size_per_job:
            spec["data_size_per_job"] = self.data_size_per_job
        if self.pool is not None:
            spec['pool'] = self.pool
        return spec

    def _collect_pivot_keys_mapper(self, tablet):
        """ Mapper: get tablet partition pivot keys """
        yt = self.make_driver_yt_client()
        for pivot_key in self.get_pivot_keys(tablet, yt=yt):
            yield {"pivot_key": pivot_key}

    def get_pivot_keys(self, tablet, yt=None):
        yt = yt if yt is not None else self.yt
        pivot_keys = [tablet["pivot_key"]]
        tablet_id = tablet["tablet_id"]
        cell_id = tablet["cell_id"]
        node = yt.get("#{}/@peers/0/address".format(cell_id))
        partitions_path = "//sys/nodes/{}/orchid/tablet_cells/{}/tablets/{}/partitions".format(
            node, cell_id, tablet_id)
        partitions = yt.get(partitions_path)
        for partition in partitions:
            pivot_keys.append(partition["pivot_key"])
        return pivot_keys


    def _wait_for_predicate(self, predicate, message, timeout, pause):
        start = time.time()
        while not predicate():
            if time.time() - start > timeout:
                error = "Timeout while waiting for \"%s\"" % message
                logging.info(error)
                raise YtError(error)
            logging.info("Waiting for \"%s\"" % message)
            time.sleep(pause)

    def _make_tablets_state_checker(self, table, possible_states):
        def state_checker():
            tablets = {tablet["tablet_id"]: tablet["state"] for tablet in self.yt.get(table + "/@tablets")}
            logging.info("Table %s tablets: %s" % (table, str(tablets)))
            return all(state in possible_states for state in tablets.itervalues())
        return state_checker

    def _wait_for_table_consistency(self, table, timeout, pause):
        self._wait_for_predicate(
            self._make_tablets_state_checker(table, ["mounted", "unmounted"]),
            "All %s tablets are either mounted or unmounted" % table,
            timeout,
            pause)

    def _mount_unmount_table(self, action, table, timeout, pause):
        start = time.time()
        if action == "mount":
            state = "mounted"
            mount_unmount = self.yt.mount_table
        else:
            state = "unmounted"
            mount_unmount = self.yt.unmount_table

        self._wait_for_table_consistency(table, timeout, pause)

        check_state = self._make_tablets_state_checker(table, [state])
        if check_state():
            return

        mount_unmount(table)

        self._wait_for_predicate(
            check_state,
            "All %s tablets are %s" % (table, state),
            timeout - (time.time() - start),
            pause)

    def wait_for_state(self, table, state, timeout=300, pause=1):
        self._wait_for_predicate(
            self._make_tablets_state_checker(table, [state]),
            "All %s tablets are %s" % (table, state),
            timeout,
            pause)

    def mount_table(self, table, timeout=300, pause=1):
        self._mount_unmount_table("mount", table, timeout, pause)

    def unmount_table(self, table, timeout=300, pause=1):
        self._mount_unmount_table("unmount", table, timeout, pause)


    # Write source table partition bounds into partition_bounds_table
    def extract_partition_bounds(self, table, partition_bounds_table):
        # Get pivot keys. For a large number of tablets use map-reduce version.
        # Tablet pivots are merged with partition pivots

        tablets = self.yt.get(table + "/@tablets")

        logging.info("Preparing partition keys for %d tablets", len(tablets))
        partition_keys = []

        if len(tablets) < 10:
            logging.info("Via get")
            tablet_idx = 0
            for tablet in tablets:
                tablet_idx += 1
                logging.info("Tablet {} of {}".format(tablet_idx, len(tablets)))
                partition_keys.extend(self.get_pivot_keys(tablet))
        else:
            logging.info("Via map")
            # note: unconfigurable.
            job_spec = {
                "job_count": 100, "max_failed_job_count": 10,
                # XXXX: pool?
                "resource_limits": {"user_slots": 50}}
            with self.yt.TempTable() as tablets_table, self.yt.TempTable() as partitions_table:
                self.yt.write_table(tablets_table, tablets, self.yson_format, raw=False)
                self.yt.run_map(
                    self._collect_pivot_keys_mapper,
                    tablets_table,
                    partitions_table,
                    spec=job_spec,
                    format=self.yson_format)
                self.yt.run_merge(partitions_table, partitions_table)
                partition_keys = self.yt.read_table(
                    partitions_table, format=self.yson_format, raw=False)
                partition_keys = [part["pivot_key"] for part in partition_keys]

        partition_keys = [
            # NOTE: using `!= None` because there's some YsonEntity
            # which is `== None` but `is not None`.
            list(it.takewhile(lambda val: not self.is_none(val), key))
            for key in partition_keys]
        partition_keys = [key for key in partition_keys if len(key) > 0]
        partition_keys = sorted(partition_keys)
        logging.info("Total %d partitions", len(partition_keys) + 1)

        # Write partition bounds into partition_bounds_table.

        # Same as `regions = window(regions, 2, fill_left=True, fill_right=True)`:
        regions = zip([None] + partition_keys, partition_keys + [None])

        regions = [{"left": left, "right": right}
                   for left, right in regions]
        shuffle(regions)
        self.yt.write_table(
            partition_bounds_table,
            regions,
            format=self.yson_format,
            raw=False)

    @staticmethod
    def quote_column(name):
        """ Query-someting column name escaping.

        WARNING: unguaranteed.
        """
        name = str(name)
        # return '"%s"' % (name,)
        return "[%s]" % (name,)

    @staticmethod
    def quote_value(val):
        # XXXX: there's some controversy as to what would be a correct
        # method of doing this.
        # https://st.yandex-team.ru/STATFACE-3432#1455787390000
        return yson.dumps(val, yson_format="text")

    def is_none(self, val):
        return (val is None or
                val == None or
                self.quote_value(val) == "#")  # cursed yson stuff

    def get_bound_key(self, columns):
        return ",".join([
            self.quote_column(column)
            for column in columns])

    def get_bound_value(self, bound):
        return ",".join([
            self.quote_value(val)
            for val in bound])

    def schema_to_select_columns_str(self, schema, include_list=None):
        column_names = [
            col["name"] for col in schema
            if "expression" not in col]
        if include_list is not None:
            column_names = [
                val for val in column_names
                if val in include_list]
        result = ",".join(
            self.quote_column(column_name)
            for column_name in column_names)
        return result

    def run_map_over_dynamic(
            self, mapper, src_table, dst_table,
            columns=None, predicate=None):

        input_row_limit = self.input_row_limit
        output_row_limit = self.output_row_limit

        schema = self.yt.get(src_table + "/@schema")
        key_columns = self.yt.get(src_table + "/@key_columns")

        select_columns_str = self.schema_to_select_columns_str(
            schema, include_list=columns)

        def prepare_bounds_query(left, right):
            def expand_bound(bound_values):
                # Get something like ((key1, key2, key3), (bound1, bound2, bound3)) from a bound.
                keys = key_columns[:len(bound_values)]
                vals = bound_values
                return self.get_bound_key(keys), self.get_bound_value(vals)

            left = "(%s) >= (%s)" % expand_bound(left) if left else None
            right = "(%s) < (%s)" % expand_bound(right) if right else None
            bounds = [val for val in [left, right, predicate] if val]
            where = (" where " + " and ".join(bounds)) if bounds else ""
            query = "%s from [%s]%s" % (select_columns_str, src_table, where)

            return query

        # Get records from source table.
        @yt_module.aggregator
        def select_mapper(bounds):
            client = self.make_driver_yt_client()

            for bound in bounds:
                def do_select():
                    return client.select_rows(
                        prepare_bounds_query(bound["left"], bound["right"]),
                        input_row_limit=input_row_limit,
                        output_row_limit=output_row_limit,
                        workload_descriptor=self.workload_descriptor,
                        raw=False)
                rows = run_with_retries(do_select, except_action=self.log_exception))
                for res in mapper(rows):
                    yield res

        map_spec = self.build_spec_from_options()

        with self.yt.TempTable() as partition_bounds_table:
            self.extract_partition_bounds(src_table, partition_bounds_table)

            self.yt.run_map(
                select_mapper,
                partition_bounds_table,
                dst_table,
                spec=map_spec,
                format=self.yson_format)

    def split_in_groups(self, rows, count=10000):
        # Should be the same as
        # https://github.com/HoverHell/pyaux/blob/484536f54311b7678e20ee0e8465f328c1b781c1/pyaux/base.py#L847
        result = []
        for row in rows:
            if len(result) >= count:
                yield result
                result = []
            result.append(row)
        yield result

    def run_map_dynamic(self, mapper, src_table, dst_table, batch_size=50000):

        def insert_mapper(rows):
            client = self.make_driver_yt_client()

            def mapped_iterator():
                for row in rows:
                    for res in mapper(row):
                        yield res

            def make_do_insert(rowset):
                def do_insert():
                    client.insert_rows(dst_table, rowset, raw=False)

                return do_insert

            rows = mapped_iterator()
            rowsets = self.split_in_groups(rows, batch_size)
            for rowset in rowsets:
                # Avoiding a closure inside the loop just in case
                do_insert_these = make_do_insert(rowset)
                run_with_retries(do_insert_these, except_action=log_exception)

            # Make a generator out of this function.
            if False:
                yield

        with self.yt.TempTable() as out_table:
            self.run_map_over_dynamic(insert_mapper, src_table, out_table)

    def convert_to_new_schema(self, schema, key_columns):
        result = []
        for column in schema:
            result_column = dict(column)  # copies it
            if column["name"] in key_columns:
                result_column["sort_order"] = "ascending"
            result.append(result_column)
        return result

# Make a singleton and make the functions accessible in the module
# (same as it was previously)
dynamic_tables_client = DynamicTablesClient()
# convenience aliases
client = dynamic_tables_client
worker = dynamic_tables_client
settings = dynamic_tables_client  # alias for kind-of obviousness

build_spec_from_options = worker.build_spec_from_options
convert_to_new_schema = worker.convert_to_new_schema
extract_partition_bounds = worker.extract_partition_bounds
get_pivot_keys = worker.get_pivot_keys
log_exception = worker.log_exception
mount_table = worker.mount_table
run_map_dynamic = worker.run_map_dynamic
run_map_over_dynamic = worker.run_map_over_dynamic
split_in_groups = worker.split_in_groups
unmount_table = worker.unmount_table
wait_for_state = worker.wait_for_state
