from .common import EMPTY_GENERATOR, YtError, get_binary_std_stream, get_value

from yt.packages.six.moves import xrange

import inspect
import os
import sys
import types

class YtStandardStreamAccessError(YtError):
    pass

class StreamWrapper(object):
    # NB: close and flush are added to allowed attributes
    # since multiprocessing uncoditionally calls sys.stdout.close and sys.stdout.flush.
    ALLOWED_ATTRIBUTES = {"fileno", "isatty", "tell", "encoding", "name", "mode", "close", "flush"}

    def __init__(self, original_stream):
        self.original_stream = original_stream

    def __getattr__(self, attr):
        if attr in self.ALLOWED_ATTRIBUTES:
            return self.original_stream.__getattribute__(attr)
        raise YtStandardStreamAccessError("Stdin, stdout are inaccessible for Python operations"
                                          " without raw_io attribute")

class WrappedStreams(object):
    def __init__(self, wrap_stdin=True, wrap_stdout=True):
        self.wrap_stdin = wrap_stdin
        self.wrap_stdout = wrap_stdout

    def __enter__(self):
        self.stdin = sys.stdin
        self.stdout = sys.stdout
        if self.wrap_stdin:
            sys.stdin = StreamWrapper(self.stdin)
        if self.wrap_stdout:
            sys.stdout = StreamWrapper(self.stdout)
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        sys.stdin = self.stdin
        sys.stdout = self.stdout

class Context(object):
    def __init__(self, table_index=None, row_index=None, range_index=None):
        self.table_index = table_index
        self.row_index = row_index
        self.range_index = range_index

def convert_callable_to_generator(func):
    def generator(*args):
        result = func(*args)
        if isinstance(result, types.GeneratorType):
            for item in result:
                yield item
        else:
            if result is not None:
                raise YtError('Non-yielding operation function should return generator or None.'
                              ' Did you mean "yield" instead of "return"?')
    return generator

def extract_operation_methods(operation, context, with_skiff_schemas, skiff_input_schemas, skiff_output_schemas):
    if hasattr(operation, "start") and inspect.ismethod(operation.start):
        start = convert_callable_to_generator(operation.start)
    else:
        start = lambda: EMPTY_GENERATOR

    if hasattr(operation, "finish") and inspect.ismethod(operation.finish):
        finish = convert_callable_to_generator(operation.finish)
    else:
        finish = lambda: EMPTY_GENERATOR


    kwargs = {}

    if context is not None:
        kwargs["context"] = context

    if with_skiff_schemas:
        kwargs["skiff_input_schemas"] = skiff_input_schemas
        kwargs["skiff_output_schemas"] = skiff_output_schemas

    if kwargs:
        operation_func = lambda *args: operation(*args, **kwargs)
    else:
        operation_func = operation
    return start, convert_callable_to_generator(operation_func), finish

def enrich_context(rows, context):
    def generate_rows():
        for row in rows:
            context.table_index = get_value(getattr(rows, "table_index", None), context.table_index)
            context.row_index = getattr(rows, "row_index", None)
            context.range_index = getattr(rows, "range_index", None)
            yield row

    return generate_rows()


def check_job_environment_variables():
    for name in ["YT_OPERATION_ID", "YT_JOB_ID", "YT_JOB_INDEX"]:
        if name not in os.environ:
            sys.stderr.write("Warning! {0} is not set. If this job is not run "
                             "manually for testing purposes then this is a bug.\n".format(name))

class FDOutputStream(object):
    def __init__(self, fd):
        self._fd = fd

    def write(self, str):
        os.write(self._fd, str)

class group_by_key_switch(object):
    def __init__(self, rows, extract_key_by_group_by, context=None):
        self.rows = rows
        self.extract_key_by_group_by = extract_key_by_group_by
        self.row = None
        self.context = context
        self._previous_group = None

    def __iter__(self):
        return self

    def __next__(self):
        if self._previous_group is None:
            self._extract_next_row()
        else:
            for row in self._previous_group:
                pass
            while not self.rows.key_switch:
                self._extract_next_row()
        self._previous_group = self._grouper()
        return (self.extract_key_by_group_by(self.row), self._previous_group)

    def next(self):
        return self.__next__()

    def _extract_next_row(self):
        self.row = next(self.rows)
        if self.context is not None:
            self.context.table_index = get_value(getattr(self.rows, "table_index", None), self.context.table_index)
            self.context.row_index = getattr(self.rows, "row_index", None)
            self.context.range_index = getattr(self.rows, "range_index", None)

    def _grouper(self):
        while True:
            yield self.row
            try:
                self._extract_next_row()
            except StopIteration:
                return
            if self.rows.key_switch:
                break

def process_rows(operation_dump_filename, config_dump_filename, start_time):
    from itertools import chain, groupby, starmap
    try:
        from itertools import imap
    except ImportError:  # Python 3
        imap = map

    import time

    import yt.yson
    import yt.wrapper
    from yt.wrapper.format import YsonFormat, extract_key
    from yt.wrapper.pickling import Unpickler
    from yt.wrapper.mappings import FrozenDict

    def process_frozen_dict(rows):
        for row in rows:
            if type(row) == FrozenDict:
                yield row.as_dict()
            else:
                yield row

    yt.wrapper.config.config = \
        Unpickler(yt.wrapper.config.DEFAULT_PICKLING_FRAMEWORK).load(open(config_dump_filename, "rb"))

    yt.wrapper.py_runner_helpers.check_job_environment_variables()

    unpickler_name = yt.wrapper.config.config["pickling"]["framework"]
    unpickler = Unpickler(unpickler_name)
    if unpickler_name == "dill" and yt.wrapper.config.config["pickling"]["load_additional_dill_types"]:
        unpickler.load_types()

    operation, params = unpickler.load(open(operation_dump_filename, "rb"))

    if yt.wrapper.config["pickling"]["enable_job_statistics"]:
        try:
            import yt.wrapper.user_statistics
            if start_time is not None:
                yt.wrapper.user_statistics.write_statistics({"python_job_preparation_time": int((time.time() - start_time) * 1000)})
        except ImportError:
            pass

    if yt.wrapper.config["pickling"]["check_python_version"] and yt.wrapper.common.get_python_version() != params.python_version:
        sys.stderr.write("Python version on cluster differs from local python version")
        sys.exit(1)

    if params.attributes.get("is_raw_io", False):
        operation()
        return

    raw = params.attributes.get("is_raw", False)

    if not params.is_local_mode and isinstance(params.input_format, YsonFormat):
        params.input_format._check_bindings()

    rows = params.input_format.load_rows(get_binary_std_stream(sys.stdin), raw=raw)

    is_reducer = not params.attributes.get("is_aggregator", False) and not params.job_type == "mapper" and not raw
    extract_key_by_group_by = lambda row: extract_key(row, params.group_by)
    grouped_rows = None

    context = None
    if params.attributes.get("with_context", False):
        set_zero_table_index = params.operation_type in ("reduce", "map") \
            and params.input_table_count == 1
        table_index = 0 if set_zero_table_index else None
        context = Context(table_index=table_index)
        if is_reducer:
            grouped_rows = group_by_key_switch(rows, extract_key_by_group_by, context=context)
            rows = None
        else:
            rows = enrich_context(rows, context)

    skiff_input_schemas = None
    skiff_output_schemas = None
    with_skiff_schemas = params.attributes.get("with_skiff_schemas", False)
    if with_skiff_schemas:
        assert params.input_format.name() == "skiff"
        assert params.output_format.name() == "skiff"
        skiff_input_schemas = params.input_format.get_schemas()
        skiff_output_schemas = params.output_format.get_schemas()

    start, run, finish = yt.wrapper.py_runner_helpers.extract_operation_methods(
        operation, context, with_skiff_schemas, skiff_input_schemas, skiff_output_schemas)
    wrap_stdin = wrap_stdout = yt.wrapper.config["pickling"]["safe_stream_mode"]
    with yt.wrapper.py_runner_helpers.WrappedStreams(wrap_stdin, wrap_stdout):
        if params.attributes.get("is_aggregator", False):
            result = run(rows)
        else:
            if params.job_type == "mapper" or raw:
                result = chain(
                    start(),
                    chain.from_iterable(imap(run, rows)),
                    finish())
            else:
                if grouped_rows is None:
                    if params.should_process_key_switch:
                        grouped_rows = group_by_key_switch(rows, extract_key_by_group_by)
                    else:
                        grouped_rows = groupby(rows, extract_key_by_group_by)
                if params.attributes.get("is_reduce_aggregator"):
                    result = run(grouped_rows)
                else:
                    result = chain(
                        start(),
                        chain.from_iterable(starmap(run, grouped_rows)),
                        finish())

        result = process_frozen_dict(result)

        if params.use_yamr_descriptors:
            output_streams = [FDOutputStream(i + 3) for i in xrange(params.output_table_count)]
        else:
            output_streams = [FDOutputStream(i * 3 + 1) for i in xrange(params.output_table_count)]
        params.output_format.dump_rows(result, output_streams, raw=raw)

    # Read out all input
    if rows is None:
        for key, rows in grouped_rows:
            pass
    else:
        for row in rows:
            pass

