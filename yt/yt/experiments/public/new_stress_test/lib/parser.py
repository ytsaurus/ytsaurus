from .spec import get_spec_preset_names, Variable
from .parser_impl import ArgumentParser, ExpertHelpFormatter, SpecBuilder
from .replication_helpers import parse_replica_spec

import yt.wrapper as yt


def replica_location(argument):
    name = argument.split(":")[0]
    count = int(argument.split(":")[1])
    return name, count

##################################################################

# Please keep below 80 characters.                                             |
#                                                                              |
#                                       NO TEXT BEYOND THIS LINE (INCLUSIVE)   |
DESCRIPTION="""
Dynamic tables stress test runner.
{linebreak}

*** QUICK START ***

Plain old stress test on sorted tables:

    %(prog)s --table //path/to/your/table --preset simple_sorted --small --force

Eternal writer (write data non-stop, no verify):

    %(prog)s --table //path/to/your/table --preset simple_sorted --mode stateless_write --bundle-node-count 1
{linebreak}

*** MODES ***

This runner can operate in several modes. Choose mode with --mode argument.

I. iterative (this is the default)

The basic mode for validating data integrity in many ways. Several iterations
will be executed over the same table, where each iteration looks as following:
    - generate a bunch of keys (if table is sorted) and values;
    - insert data into the dynamic table;
    - run lookups and selects, verify that their result match generated data;
    - run several mapreduce operations, verify their output;
    - reshard the table and alter the schema.

If any integrity check fails, information should be printed to the log. Also
failed checks leave nonempty "result" tables with paths like
    //path/to/table.0.2.select.result
where //path/to/table is the base path provided with --table argument.

II. stateless write

In this mode data is only inserted into the table. No integrity checks, only
steady rate of random insertions. If the table exists and has expected schema,
it will be used, otherwise it will be created from scratch.
{linebreak}


*** SPEC AND PRESETS ***

Each test execution is provided with a spec. Spec defines all "what to run"
stuff. There may still be out-of-spec arguments like --pool, defining "how to
run" stuff. After providing some arguments you can see what would be run using
--show-spec option. To see the completely filled spec, use
    %(prog)s --preset full --show-spec

Some preset must be selected. It can be further modified with CLI options. For
example, --tablet-count 10 sets spec value size.tablet_count to 10. Use -hh to
see all available options.

There are some mixins: shortcuts for setting a frequent set of options
simultaneously. For example, --simple-schema makes a schema with one int64 key
column and one string value column.
{linebreak}


*** VARIABLES ***

While some parameters in the spec are fixed, other may take different values on
different runs: e.g. it makes sense to set random chunk_format every time. This
is achieved with so-called Variables. Variable is a list of values marked with
the mode: "Any" or "Variate". "Any" will result in picking random value.
"Variate" will execute the program for all values. If there are many "Variate"
variables, all their combinations will be iterated through.

"full" preset has all variables set to "Variate" mode with the widest possible
set of values. If you attempt to run anything with this preset, combinatorial
explosion will happen. Thus either narrower preset should be used or almost all
variables should be overridden with "Any" or exact values.

Some CLI arguments expect Variable value. You can provide either an exact value
or a variable with the mode. All of those are valid:
    --chunk-format table_versioned_simple
    --chunk-format 'All(table_versioned_simple, table_versioned_columnar)'
    --chunk-format 'Variate(table_versioned_columnar, table_versioned_slim)'
{linebreak}

""".format(linebreak=" ")

assert all(len(x) < 80 for x in DESCRIPTION.split("\n") if "(prog)" not in x)

##################################################################

def parse_human_int(value):
    suffixes = [
        ("T", 2**40),
        ("G", 2**30),
        ("M", 2**20),
        ("K", 2**10),
        ("", 1)
    ]
    for suffix, multiplier in suffixes:
        if value.endswith(suffix):
            if suffix:
                value = value[:-len(suffix)]
            return int(value) * multiplier

def parse_int_variable(value):
    return Variable.from_string(value, int)

def parse_string_variable(value):
    return Variable.from_string(value, str)

def parse_yson_dict(value):
    dct = yt.yson.loads(value.encode())
    assert isinstance(dct, dict)
    return dct

##################################################################

def parse_args():
    usage = "%(prog)s --preset {{{}}} [options]".format(",".join(get_spec_preset_names()))
    parser = ArgumentParser(usage=usage, description=DESCRIPTION)
    builder = SpecBuilder(parser)
    simple_args = ExpertHelpFormatter.whitelist

    def _raise(ex):
        raise ex

    general = parser.add_argument_group("general arguments")
    general.add_argument("--preset", type=str, choices=get_spec_preset_names(), required=True)
    general.add_argument("--table", type=lambda *args: _raise(RuntimeError("--table is deprecated, use --path and specify the path to the base directory instead")))
    general.add_argument("--path", type=str, required=True, help="base directory")
    general.add_argument("--random-name", action="store_true", help="create a subdirectory with random name")
    builder.add_argument("seed", type=int, help="random seed (applies for schema, pivots etc, but not to data itself)")
    general.add_argument("--repeats", default=1, type=int, help="number of full cycle repeats")
    general.add_argument("--force", action="store_true", default=False, help="overwrite destination table if it exists")
    general.add_argument("--show-spec", action="store_true", default=False, help="show resulting spec without actually running anything")
    builder.add_yesno_argument("ipv4", help="Run in IPv4 network")
    general.add_argument("-h", "--help", action="count", default=0, help="print this message (use -hh for all options)")
    simple_args.update(("preset", "table", "random_name", "seed", "repeats", "force",  "show_spec", "help"))


    yt_args = parser.add_argument_group("yt specifics")
    yt_args.add_argument("--proxy", type=yt.config.set_proxy, help="yt proxy")
    yt_args.add_argument("--pool", type=str, help="yt pool for operations")
    yt_args.add_argument("--max-failed-job-count", type=int, default=1, metavar="N")
    simple_args.update(("proxy", "pool"))


    with builder.group("retries", "retries"):
        builder.add_argument("retry_interval", path="interval", type=int, metavar="N", help="interval between retries of dynamic tables requests")
        builder.add_argument("retry_count", path="count", type=int, metavar="N", help="number of retries of dynamic tables requests")
        with builder.mutex():
            builder.add_bool_mixin_argument("no_retries", help="do not retry requests to dynamic tables")
            builder.add_bool_mixin_argument("full_retries", help="retry requests for 10 minutes")
        simple_args.update(("no_retries", "full_retries"))


    with builder.group("common spec"):
        builder.add_argument("mode", choices=["iterative", "stateless_write", "compare_replicas"])
        builder.add_argument("chunk_format", type=parse_string_variable, metavar="VARIABLE", help="chunk formats (table_versioned_simple/table_versioned_columnar/...)")
        builder.add_argument("erasure_codec", type=parse_string_variable, metavar="VARIABLE", help="erasure codecs (none/lrc_12_2_2/...)")
        builder.add_argument("compression_codec", type=parse_string_variable, metavar="VARIABLE", help="compression codecs (none/lz4/...)")
        builder.add_argument("in_memory_mode", type=parse_string_variable, metavar="VARIABLE", help="in memory modes (none/compressed/uncompressed)")
        builder.add_argument("skip_flush", action="store_true", help="do not flush the table on each iteration before map-reduce")
        builder.add_yesno_argument("reshard", help="reshard the table after each iteration")
        builder.add_yesno_argument("alter", help="alter table schema after each iteration")
        builder.add_yesno_argument("mapreduce", help="run map-reduce operations over the dynamic table")
        builder.add_yesno_argument("prepare_table_via_alter", help="use static-to-dynamic alter at the first iteration instead of insert_rows")
        builder.add_yesno_argument("remote_copy_to_itself", help="clone table via remote copy after each iteration")
        builder.add_argument("extra_attributes", allow_unrecognized=True, type=parse_yson_dict, metavar="YSON_DICT", help="attributes to set on the dynamic table")
        builder.add_enable_disable_argument("tablet_balancer", path="enable_tablet_balancer", help="enable/disable tablet balancer")
        simple_args.update(("mode", "in_memory_mode", "reshard", "alter", "mapreduce"))


    with builder.group("size options", "size"):
        with builder.mutex():
            builder.add_bool_mixin_argument("tiny", help="1000 rows, 1 job, 1 tablet")
            builder.add_bool_mixin_argument("small", help="50M data, 5 jobs, 1 tablet")
            builder.add_bool_mixin_argument("medium", help="1G data, 40 jobs, 2 tablets")
            builder.add_bool_mixin_argument("large", help="20G data, 400 jobs, 4 tablets")
            builder.add_bool_mixin_argument("huge", help="100G data, 1000 jobs, 10 tablets")
        builder.add_argument("job_count", type=int, metavar="N", help="job count for insert/lookup/select operations")
        builder.add_argument("bundle_node_count", type=int, metavar="N", help="number of nodes in the bundle (to determine read/write concurrency)"
            " User slot count is vaguely estimated based on mode, use --read/write_user_slot_count for precise tuning")
        builder.add_argument("write_user_slot_count", type=int, metavar="N", help="maximum number of insertion jobs running at the same time")
        builder.add_argument("read_user_slot_count", type=int, metavar="N", help="maximum number of lookup/select jobs running at the same time")
        builder.add_argument("tablet_count", type=int, metavar="N")
        builder.add_argument("iterations", type=int, metavar="N", help="number of read-write-alter iterations over the same table")
        with builder.mutex():
            builder.add_argument("key_count", type=int, metavar="N", help="number of rows in the dynamic table")
            builder.add_argument("data_weight", type=parse_human_int, metavar="N", help="approximate data weight of the dynamic table (e.g. 20G)")
        with builder.mutex():
            builder.add_argument("data_job_count", type=int, metavar="N", help="job count in auxiliary operations")
            builder.add_argument("data_weight_per_data_job", metavar="N", type=parse_human_int, help="data weight per job in auxiliary operations")
        simple_args.update(("bundle_node_count", "tiny", "small", "medium", "large", "huge"))


    with builder.group("sorted tables specifics", "sorted"):
        builder.add_argument("insertion_probability", type=float, metavar="P", help="probability of row insertion upon each interation (between 0.0 and 1.0)")
        builder.add_argument("deletion_probability", type=float, metavar="P", help="probability of row deletion upon each interation (between 0.0 and 1.0)")
        builder.add_yesno_argument("data_node_lookup", "enable_data_node_lookup", help="enable data node lookup")
        builder.add_yesno_argument("hash_chunk_index_for_lookup", "enable_hash_chunk_index_for_lookup", help="enable lookup with hash chunk index")
        builder.add_yesno_argument("lookup_hash_table", "enable_lookup_hash_table", help="enable lookup hash table")
        builder.add_argument("lookup_cache_rows_per_tablet", type=int, metavar="N", help="lookup cache rows per tablet")
        builder.add_argument("max_inline_hunk_size", metavar="N", type=int, help="limit on value size to not move the value into a hunk")
        builder.add_yesno_argument("value_dictionary_compression", "enable_value_dictionary_compression", help="enable value dictionary compression")


    with builder.group("ordered tables specifics", "ordered"):
        builder.add_argument("rows_per_tablet", metavar="N", type=int)
        builder.add_argument("insertion_batch_size", metavar="N", type=int)
        builder.add_yesno_argument("trim")


    with builder.group("schema", "schema"):
        builder.add_bool_mixin_argument("simple_schema", help="schema with two int64 key columns and one string value column")
        builder.add_argument("key_column_count", metavar="N", type=int)
        builder.add_argument("value_column_count", metavar="N", type=int)
        builder.add_argument("key_column_types", metavar="TYPE", type=str, nargs="*")
        builder.add_argument("value_column_types", metavar="TYPE", type=str, nargs="*")
        builder.add_argument("no_aggregate", "allow_aggregates", action="store_false", help="do not include aggregate columns in schema")


    with builder.group("map-reduce operation options", "mr_options"):
        builder.add_argument("operation_types", nargs="*", type=parse_string_variable, metavar="VARIABLE", help="TBD")
        # TODO: job interrupts


    builder.add_argument("replicas", nargs="*", type=parse_replica_spec, metavar="REPLICA")

    with builder.group("replicated table options", "replicated"):
        builder.add_argument("min_sync_replicas", metavar="N", type=int)
        builder.add_argument("max_sync_replicas", metavar="N", type=int)
        builder.add_yesno_argument("switch_replica_modes")
        builder.add_yesno_argument("enable_replicated_table_tracker")
        builder.add_yesno_argument("mount_unmount_replicas")


    with builder.group("testing options", "testing", description=ExpertHelpFormatter.EXPERT+"Options used to debug the script itself"):
        builder.add_argument("skip_generation", action="store_true")
        builder.add_argument("skip_write", action="store_true")
        builder.add_argument("skip_verify", action="store_true")
        builder.add_argument("skip_lookup", action="store_true")
        builder.add_argument("skip_select", action="store_true")
        builder.add_argument("skip_group_by", action="store_true")
        builder.add_argument("ignore_failed_mr", action="store_true")

    # Leftovers from the previous incarnation.
    """
    features = parser.add_argument_group("features")
    features.add_argument("--bulk-insert", action="store_true", help="use bulk insert")
    features.add_argument("--bulk-insert-probability", type=float, default=0.5,
                          help="probability of insertion shard being bulk inserted")
    features.add_argument("--test-unwritable-replica", action="store_true", default=False,
                          help="make one replica unwritable because of quota overflow")
    features.add_argument("--unwritable-replica-mode", type=str, default="async",
                          help="replication mode of unwritable replica")
    features.add_argument("--switch-bundle-options", nargs="*", default=[],
                          help="switch bundle's changelog and snapshot account options according to the passed list."
                               "also switch changelog and snapshot acls")

    misc = parser.add_argument_group("misc")
    misc.add_argument("--jobs-per-tablet", type=int, default=1,
                      help="number of jobs to insert data in one tablet (for ordered tables)")
    misc.add_argument("--replica-locations", type=replica_location, nargs="*", default=[(get_proxy_url(), 1)],
                      help="clusters to place table replicas with number of replicas per cluster "
                           "in format cluster_name:replica_count")
    misc.add_argument("--low-quota-account", type=str, default=None,
                         help="account with low chunk quota")
    """

    args = parser.parse_args()

    if args.help > 0:
        parser.print_help()
        exit(0)

    return builder.build_spec(args), args


if __name__ == "__main__":
    spec, args = parse_args()
    import pprint
    pprint.pprint(spec)
