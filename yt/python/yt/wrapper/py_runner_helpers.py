from .common import EMPTY_GENERATOR, YtError, get_binary_std_stream, get_value
from .format import StructuredSkiffFormat
try:
    from yt.python.yt.cpp_wrapper import CppJob, exec_cpp_job
    _CPP_WRAPPER_AVAILABLE = True
except ImportError:
    _CPP_WRAPPER_AVAILABLE = False

import inspect
import os
import sys
import types


class YtStandardStreamAccessError(YtError):
    pass


class StreamWrapper(object):
    # NB: close and flush are added to allowed attributes
    # since multiprocessing unconditionally calls sys.stdout.close and sys.stdout.flush.
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
    def __init__(self, table_index=None, row_index=None, range_index=None, tablet_index=None):
        self.table_index = table_index
        self.row_index = row_index
        self.range_index = range_index
        self.tablet_index = tablet_index


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
        start = lambda: EMPTY_GENERATOR  # noqa

    if hasattr(operation, "finish") and inspect.ismethod(operation.finish):
        finish = convert_callable_to_generator(operation.finish)
    else:
        finish = lambda: EMPTY_GENERATOR  # noqa

    kwargs = {}

    if context is not None:
        kwargs["context"] = context

    if with_skiff_schemas:
        kwargs["skiff_input_schemas"] = skiff_input_schemas
        kwargs["skiff_output_schemas"] = skiff_output_schemas

    if kwargs:
        operation_func = lambda *args: operation(*args, **kwargs)  # noqa
    else:
        operation_func = operation
    return start, convert_callable_to_generator(operation_func), finish


def enrich_context(rows, context):
    def generate_rows():
        for row in rows:
            context.table_index = get_value(getattr(rows, "table_index", None), context.table_index)
            context.row_index = getattr(rows, "row_index", None)
            context.range_index = getattr(rows, "range_index", None)
            context.tablet_index = getattr(rows, "tablet_index", None)
            yield row

    return generate_rows()


def check_job_environment_variables():
    for name in ["YT_OPERATION_ID", "YT_JOB_ID", "YT_JOB_INDEX"]:
        if name not in os.environ:
            sys.stderr.write("Warning! {0} is not set. If this job is not run "
                             "manually for testing purposes then this is a bug.\n".format(name))


class FDOutputStream(object):
    def __init__(self, fd):
        self.fd = fd

    def write(self, str):
        os.write(self.fd, str)

    def close(self):
        os.close(self.fd)

    def dup(self):
        return FDOutputStream(os.dup(self.fd))


class StructuredKeySwitchGroup(object):
    def __init__(self, structured_iterator, first_row):
        self._structured_iterator = structured_iterator
        self._with_context = False
        self._first_row = first_row
        self._outstanding_row = None

    def __iter__(self):
        return self

    def __next__(self):
        if self._first_row is not None:
            row = self._first_row
            self._first_row = None
        else:
            if self._outstanding_row is not None:
                raise StopIteration()
            row = next(self._structured_iterator)
            if self._structured_iterator.get_key_switch():
                self._outstanding_row = row
                raise StopIteration()
        if self._with_context:
            return (row, self._structured_iterator)
        else:
            return row

    def with_context(self):
        self._with_context = True
        return self


def group_structured_rows_by_key_switch(structured_iterator):
    try:
        first_row = next(structured_iterator)
    except StopIteration:
        return
    while True:
        group = StructuredKeySwitchGroup(structured_iterator, first_row)
        yield group
        # Read all the remaining rows.
        for _ in group:
            pass
        first_row = group._outstanding_row
        # Check if there is next row to start group with.
        if first_row is None:
            break


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
            self.context.tablet_index = getattr(self.rows, "tablet_index", None)

    def _grouper(self):
        while True:
            yield self.row
            try:
                self._extract_next_row()
            except StopIteration:
                return
            if self.rows.key_switch:
                break


def apply_stdout_fd_protection(output_streams, protection_type):
    if protection_type == "none":
        return

    assert protection_type in ("redirect_to_stderr", "drop", "close"), \
        "unknown stdout_fd_protection value: {}".format(protection_type)

    stdin_fd = 0
    stdout_fd = 1
    stderr_fd = 2

    # NB: move file descriptors 1 (stdout) and 0 (stdin) so that we don't write operation output
    # to them. In particular, this protects from third-party C/C++ libraries writing to stdout.
    for index in range(len(output_streams)):
        stream = output_streams[index]
        if stream.fd in (stdin_fd, stdout_fd):
            output_streams[index] = stream.dup()
            stream.close()
            assert output_streams[index].fd not in (stdin_fd, stdout_fd)

    if protection_type == "redirect_to_stderr":
        os.dup2(stderr_fd, stdout_fd)
    elif protection_type == "drop":
        fd = os.open("/dev/null", os.O_WRONLY)
        if fd != stdout_fd:
            os.dup2(fd, stdout_fd)
            os.close(fd)


def check_allowed_structured_skiff_attributes(params):
    attributes = params.attributes
    if attributes.get("is_raw_io", False):
        raise YtError("@yt.wrapper.raw_io decorator is not compatible with TypedJob")
    if attributes.get("is_raw", False):
        raise YtError("@yt.wrapper.raw decorator is not compatible with TypedJob")
    with_context = attributes.get("with_context", False)
    if with_context and \
            (attributes.get("is_aggregator", False) or attributes.get("is_reduce_aggregator", False)):
        raise YtError("@yt.wrapper.with_context decorator is not compatible with "
                      "@yt.wrapper.aggregator and @yt.wrapper.reduce_aggregator; "
                      "instead, use `for row, context in rows.with_context()`")
    if with_context and params.job_type != "mapper":
        raise YtError("@yt.wrapper.with_context decorator may be used only with mappers")


def process_rows(operation_dump_filename, config_dump_filename, start_time):
    from itertools import chain, groupby, starmap
    import time

    import yt.yson
    import yt.wrapper
    from yt.wrapper.format import YsonFormat, extract_key
    from yt.wrapper.pickling import Unpickler
    from yt.wrapper.mappings import FrozenDict

    def process_frozen_dict(rows):
        for row in rows:
            if type(row) is FrozenDict:
                yield row.as_dict()
            else:
                yield row

    # TODO: move key to protected section YT-24616
    pickling_encryption_key = os.environ.get(
        "YT_SECURE_VAULT__PICKLING_KEY",
        os.environ.get("_PICKLING_KEY", None)
    )

    with open(config_dump_filename, "rb") as f_config_dump:
        config_unpickler = Unpickler(yt.wrapper.config.DEFAULT_PICKLING_FRAMEWORK)
        config_unpickler.enable_encryption(pickling_encryption_key)
        yt.wrapper.config.config = config_unpickler.load(f_config_dump)

    yt.wrapper.py_runner_helpers.check_job_environment_variables()

    unpickler_name = yt.wrapper.config.config["pickling"]["framework"]
    unpickler = Unpickler(unpickler_name)
    unpickler.enable_encryption(pickling_encryption_key)
    if unpickler_name == "dill" and yt.wrapper.config.config["pickling"]["load_additional_dill_types"]:
        unpickler.load_types()

    with open(operation_dump_filename, "rb") as f_operation_dump:
        operation, params = unpickler.load(f_operation_dump)

    if yt.wrapper.config["pickling"]["enable_job_statistics"]:
        try:
            import yt.wrapper.user_statistics
            if start_time is not None:
                yt.wrapper.user_statistics.write_statistics(
                    {"python_job_preparation_time": int((time.time() - start_time) * 1000)})
        except ImportError:
            pass

    if yt.wrapper.config["pickling"]["check_python_version"] and \
            yt.wrapper.common.get_python_version() != params.python_version:
        sys.stderr.write("Python version on cluster differs from local python version")
        sys.exit(1)

    if _CPP_WRAPPER_AVAILABLE and isinstance(operation, CppJob):
        job_arguments = {
            "job_name": operation._mapper_name,
            "output_table_count": params.output_table_count,
            "has_state": params.has_state,
        }
        exec_cpp_job(job_arguments)
        raise RuntimeError("Returned to Python code after executing CppJob. It's a bug. Contact yt@")

    is_structured_skiff = isinstance(params.input_format, StructuredSkiffFormat)
    if is_structured_skiff:
        check_allowed_structured_skiff_attributes(params)

    if params.attributes.get("is_raw_io", False) or params.job_type == "vanilla":
        operation()
        return

    raw = params.attributes.get("is_raw", False)

    if not params.is_local_mode and isinstance(params.input_format, (YsonFormat, StructuredSkiffFormat)):
        params.input_format._check_bindings()

    rows = params.input_format.load_rows(get_binary_std_stream(sys.stdin), raw=raw)

    is_reducer = not params.attributes.get("is_aggregator", False) and not params.job_type == "mapper" and not raw
    extract_key_by_group_by = lambda row: extract_key(row, params.group_by)  # noqa
    grouped_rows = None

    context = None
    if params.attributes.get("with_context", False):
        if is_structured_skiff:
            context = rows
        else:
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

    if params.use_yamr_descriptors:
        output_streams = [FDOutputStream(i + 3) for i in range(params.output_table_count)]
    else:
        output_streams = [FDOutputStream(i * 3 + int(os.environ.get("YT_FIRST_OUTPUT_TABLE_FD", 1))) for i in range(params.output_table_count)]

    apply_stdout_fd_protection(output_streams, yt.wrapper.config["pickling"]["stdout_fd_protection"])

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
                    chain.from_iterable(map(run, rows)),
                    finish())
            else:
                is_aggregator = params.attributes.get("is_reduce_aggregator", False)
                if is_structured_skiff:
                    assert grouped_rows is None
                    grouped_rows = group_structured_rows_by_key_switch(rows)
                elif grouped_rows is None:
                    if params.should_process_key_switch:
                        grouped_rows = group_by_key_switch(rows, extract_key_by_group_by)
                    else:
                        grouped_rows = groupby(rows, extract_key_by_group_by)

                if is_aggregator:
                    result = run(grouped_rows)
                else:
                    if is_structured_skiff:
                        result = chain(
                            start(),
                            chain.from_iterable(map(run, grouped_rows)),
                            finish(),
                        )
                    else:
                        result = chain(
                            start(),
                            chain.from_iterable(starmap(run, grouped_rows)),
                            finish(),
                        )

        result = process_frozen_dict(result)

        if output_streams:
            params.output_format.dump_rows(result, output_streams, raw=raw)
        else:
            # NB: we need to exhaust generator to run user code.
            for _ in result:
                pass

    # Read out all input
    if rows is None:
        for key, rows in grouped_rows:
            pass
    else:
        for row in rows:
            pass
