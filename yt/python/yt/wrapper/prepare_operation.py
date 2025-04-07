from .batch_helpers import create_batch_client
from .cypress_commands import get
from .errors import YtError, YtResolveError, YtResponseError
from .format import StructuredSkiffFormat
from .schema import (TableSchema, _SchemaRuntimeCtx, _validate_py_schema, is_yt_dataclass,
                     check_schema_module_available, is_schema_module_available)
from .schema.internal_schema import RowSchema  # noqa
from .ypath import TablePath
from .config import get_config

from yt.yson import to_yson_type

from abc import ABCMeta, abstractmethod
from copy import deepcopy

import typing

if is_schema_module_available():
    from .schema import Variant, OutputRow, RowIterator


class TypedJob:
    """
    Interface for jobs that use structured data as input and output.
    The types for input and output rows are specified in :func:`~TypedJob.prepare_operation`.
    """

    def prepare_operation(self, context, preparer):
        """
        Override this method to specify input and output row types
        (they must be classes marked with :func:`yt_dataclass` as decorator).

        :param OperationPreparationContext context: Provides information on input and output tables
        :param OperationPreparer preparer: Used to specify row types and table schemas

        Example:
        ```
            preparer \
                .input(0, type=InRow) \
                .outputs(range(context.get_output_count()), type=OutRow)
        ```
        """
        try:
            _infer_table_schemas(self, context, preparer)
        except (TypeError, YtError) as e:
            raise YtError("Failed to infer table schemas. "
                          "Specify it manually by overriding `job.prepare_operation` method",
                          attributes={"job_python_type": type(self).__qualname__}, inner_errors=e)

        return

    def get_intermediate_stream_count(self):
        """
        Override this method in mapper jobs of MapReduce operation to specify
        the number of intermediate streams.

        :return: Number of intermediate streams
        """
        return None


class OperationPreparationContext:
    """
    Interface used to provide information on input and output tables.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_input_count(self):
        """
        Get number of input streams.
        """
        pass

    @abstractmethod
    def get_output_count(self):
        """
        Get number of output streams.
        """
        pass

    @abstractmethod
    def get_input_schemas(self):
        """
        Get list of input schemas.
        """
        pass

    @abstractmethod
    def get_output_schemas(self):
        """
        Get list of output schemas. Some schemas may be None if a table does not exist.
        """
        pass

    @abstractmethod
    def get_input_paths(self):
        """
        Get list of input table paths.
        """
        pass

    @abstractmethod
    def get_output_paths(self):
        """
        Get list of output table paths.
        """
        pass


class OperationPreparer:
    """
    Class used to specify input and output row types for a job.
    Input column filter, input column renaming and output schemas also can be specified.
    """

    def input(self, index, type=None, **kwargs):
        """
        Single-index version of :func:`~OperationPreparer.inputs`.
        """

        if not isinstance(index, int):
            raise TypeError("Expected index to be of type int")
        kwargs["type"] = type
        return self.inputs([index], **kwargs)

    def inputs(self, indices, type=None, column_filter=None, column_renaming=None):
        """
        Specify row type, column filter and renaming for several input streams.

        :param indices: iterable of indices of input streams to apply modification to.
        :param type: type of input row. Must be declared with @yt_dataclass decorator.
        :param column_filter: iterable of column names to be requested inside job.
        :param column_renaming: dict mapping original (from schema) to new column name (visible in the job).
        """

        index_list = list(indices)
        if type is not None:
            if not is_yt_dataclass(type):
                raise TypeError("Input type must be a class marked with @yt_dataclass")
            self._set_elements("Input type", self._input_types, index_list, type)
        if column_filter is not None:
            self._set_elements("Input column filter", self._input_column_filters, index_list, column_filter)
        if column_renaming is not None:
            self._set_elements("Input column renaming", self._input_column_renamings, index_list, column_renaming)
        return self

    def output(self, index, type=None, **kwargs):
        """
        Single-index version of :func:`~OperationPreparer.outputs`.
        """

        if not isinstance(index, int):
            raise TypeError("Expected index to be of type int")
        kwargs["type"] = type
        return self.outputs([index], **kwargs)

    def outputs(self, indices, type=None, schema=None, infer_strict_schema=True):
        """
        Specify row type and schema for several output streams.

        :param indices: iterable of indices of output streams to apply modification to.
        :param type: type of output row. Must be declared with @yt_dataclass decorator.
        :param schema: schema of output table (for empty or nonexistent output tables). By default it is inferred from ``type``.
        :param infer_strict_schema: whether the schema inferred from ``type`` must be strict.
        """

        index_list = list(indices)
        if type is not None:
            if not is_yt_dataclass(type):
                raise TypeError("Output type must be a class marked with @yt_dataclass")
            self._set_elements("Output type", self._output_types, index_list, type)
            self._set_elements("Output schema", self._infer_strict_output_schemas, index_list, infer_strict_schema)
        if schema is not None:
            self._set_elements("Output schema", self._output_schemas, index_list, schema)
        return self

    def __init__(self, context, input_control_attributes=None):
        self._context = context  # type: IntermediateOperationPreparationContext
        self._input_count = self._context.get_input_count()
        self._output_count = self._context.get_output_count()
        self._input_types = [None] * self._input_count
        self._output_types = [None] * self._output_count
        self._output_schemas = [None] * self._output_count
        self._input_column_filters = [None] * self._input_count
        self._input_column_renamings = [{}] * self._input_count
        self._input_py_schemas = []  # type: list[RowSchema]
        self._output_py_schemas = []  # type: list[RowSchema]
        self._infer_strict_output_schemas = [True] * self._output_count
        if input_control_attributes is None:
            input_control_attributes = {
                "enable_key_switch": True,
            }
        self._input_control_attributes = input_control_attributes

    @staticmethod
    def _set_elements(name, list_, indices, value):
        for index in indices:
            if index < 0 or index >= len(list_):
                raise ValueError("{} index {} out of range [0, {})".format(name, index, len(list_)))
            list_[index] = value

    def _raise_missing(self, direction, index):
        assert direction in ("input", "output")
        if direction == "input":
            path = self._context.get_input_paths()[index]
        else:
            assert direction == "output"
            path = self._context.get_output_paths()[index]
        path_message = ""
        if path is not None:
            path_message = " ({})".format(path)
        raise ValueError("Missing type for {} table no. {}{}".format(direction, index, path_message))

    def _finish(self):
        input_schemas = self._context.get_input_schemas()
        output_schemas = self._context.get_output_schemas()
        for index, type_ in enumerate(self._input_types):
            if type_ is None:
                self._raise_missing("input", index)
            column_renaming = self._input_column_renamings[index]
            py_schema = _SchemaRuntimeCtx() \
                .set_validation_mode_from_config(get_config(client=self._context.get_client())) \
                .set_for_reading_only(True) \
                .create_row_py_schema(type_, input_schemas[index], control_attributes=self._input_control_attributes, column_renaming=column_renaming)
            self._input_py_schemas.append(py_schema)
            _validate_py_schema(py_schema)
        for index, type_ in enumerate(self._output_types):
            if type_ is None:
                self._raise_missing("output", index)
            if self._output_schemas[index] is None:
                self._output_schemas[index] = output_schemas[index]
            py_schema = _SchemaRuntimeCtx() \
                .set_validation_mode_from_config(get_config(client=self._context.get_client())) \
                .set_for_reading_only(False) \
                .create_row_py_schema(type_, self._output_schemas[index])
            _validate_py_schema(py_schema)
            self._output_py_schemas.append(py_schema)
            if self._output_schemas[index] is None or self._output_schemas[index].is_empty_nonstrict():
                self._output_schemas[index] = TableSchema.from_row_type(
                    type_,
                    strict=self._infer_strict_output_schemas[index],
                )


def _request_schemas(paths, client):
    batch_client_by_cluster = dict()

    def get_client_for_cluster(cluster_name=None):
        if cluster_name not in batch_client_by_cluster:
            # NB: assumes that cluster_name == yt_proxy
            if not cluster_name or cluster_name == client.config["proxy"]["url"]:
                batch_client_by_cluster[cluster_name] = create_batch_client(raise_errors=False, client=client)
            else:
                from yt.wrapper import YtClient

                new_client = YtClient(config=deepcopy(get_config(client)))
                new_client.config["proxy"]["url"] = cluster_name
                batch_client_by_cluster[cluster_name] = create_batch_client(raise_errors=False, client=new_client)
        return batch_client_by_cluster[cluster_name]

    batch_responses = []
    for path in paths:
        if path is None:
            batch_responses.append(None)
            continue
        batch_responses.append(get(path + "/@", attributes=["schema"], client=get_client_for_cluster(path.attributes.get("cluster", ""))))

    for batch_client in batch_client_by_cluster.values():
        batch_client.commit_batch()

    def get_schema(batch_response):
        if batch_response is None:
            return None
        if batch_response.is_ok():
            return TableSchema.from_yson_type(batch_response.get_result()["schema"])
        error = YtResponseError(batch_response.get_error())
        if isinstance(error, YtResolveError):
            return None
        raise error
    return list(map(get_schema, batch_responses))


class SimpleOperationPreparationContext(OperationPreparationContext):
    def __init__(self, input_paths, output_paths, client):
        self._input_paths = input_paths
        self._output_paths = output_paths
        self._client = client
        self._input_schemas = None
        self._output_schemas = None
        self._pickling_encryption_key = None

    def get_input_count(self):
        return len(self._input_paths)

    def get_output_count(self):
        return len(self._output_paths)

    def get_input_schemas(self):
        if self._input_schemas is None:
            self._input_schemas = _request_schemas(self._input_paths, self._client)
        return self._input_schemas

    def get_output_schemas(self):
        if self._output_schemas is None:
            self._output_schemas = _request_schemas(self._output_paths, self._client)
        return self._output_schemas

    def get_input_paths(self):
        return self._input_paths

    def get_output_paths(self):
        return self._output_paths

    def get_client(self):
        return self._client


class IntermediateOperationPreparationContext(OperationPreparationContext):
    def __init__(self, input_schemas, output_paths, client):
        self._input_schemas = input_schemas
        self._input_paths = [None] * len(self._input_schemas)
        self._output_paths = output_paths
        self._client = client
        self._output_schemas = None
        self._pickling_encryption_key = None

    def get_input_count(self):
        return len(self._input_schemas)

    def get_output_count(self):
        return len(self._output_paths)

    def get_input_schemas(self):
        return self._input_schemas

    def get_output_schemas(self):
        if self._output_schemas is None:
            self._output_schemas = _request_schemas(self._output_paths, self._client)
        return self._output_schemas

    def get_input_paths(self):
        return self._input_paths

    def get_output_paths(self):
        return self._output_paths

    def get_client(self):
        return self._client


def run_operation_preparation(job, context, input_control_attributes):
    assert isinstance(job, TypedJob)
    preparer = OperationPreparer(context, input_control_attributes=input_control_attributes)
    job.prepare_operation(context, preparer)
    output_schemas = context.get_output_schemas()
    preparer._finish()
    input_paths = deepcopy(context.get_input_paths())
    output_paths = deepcopy(context.get_output_paths())
    for index, path in enumerate(input_paths):
        if path is None:
            # Case of intermediate data in MapReduce.
            continue
        input_py_schema = preparer._input_py_schemas[index]
        assert isinstance(path, TablePath)
        column_filter = preparer._input_column_filters[index]
        column_renaming = preparer._input_column_renamings[index]
        if column_filter is not None:
            path.columns = column_filter
        if path.columns is None:
            column_filter = input_py_schema.get_columns_for_reading()
            if column_filter is not None:
                path.columns = column_filter
        if len(column_renaming) > 0:
            path.rename_columns = column_renaming
        input_paths[index] = path
    for index, path in enumerate(output_paths):
        if path is None:
            path = to_yson_type(None)
        else:
            assert isinstance(path, TablePath)
        if output_schemas[index] is None or output_schemas[index].is_empty_nonstrict():
            path.attributes["schema"] = preparer._output_schemas[index]
        output_paths[index] = path
    input_format = StructuredSkiffFormat(preparer._input_py_schemas, for_reading=True)
    output_format = StructuredSkiffFormat(preparer._output_py_schemas, for_reading=False)
    return input_format, output_format, input_paths, output_paths


def _infer_table_schemas(job, context, preparer):
    """
    Try to infer table schemas from :func:`job.__call__`'s type hints.
    The number of input and output types must match the number of input and output tables respectively.

    Let `T` be any class decorated with `@yt_dataclass`, and `Ts` is a list of `T` types.

    Restrictions:
    1. First annotated argument should be annotated as either `InputType` or `RowIterator[InputType]`,
    where `InputType` could be either `T` or `yt.schema.Variant[Ts]`,
    where `Ts` is a list of `T` types

    2. Return type should be annotated as any of `typing.Iterable[OutputType]`, `typing.Iterator[OutputType]`
    or `typing.Generator[OutputType, ...]`,
    where `OutputType` could be equal to `T`, `OutputRow[T]` or `OutputRow[yt.schema.Variant[Ts]]`

    See examples in the tests *schema_inferring_from_type_hints in the test_typed_api.py
    """
    check_schema_module_available()

    def extract_row_type(tp):
        if typing.get_origin(tp) is OutputRow:
            args = typing.get_args(tp)
            if not args:
                raise TypeError("OutputRow expected to be annotated with row type")
            return args[0]

        return tp

    def extract_type_list(variant):
        if typing.get_origin(variant) is not Variant:
            return [variant]

        return list(typing.get_args(variant))

    def get_tables_types(tp, expected_type_count, table_type):
        type_list = extract_type_list(tp)
        if len(type_list) != expected_type_count:
            # if the user has specified only one row type,
            # consider that all tables have the same row type
            if len(type_list) == 1:
                type_list *= expected_type_count
            else:
                raise TypeError("Wrong number of {} types {}, {} expected".format(
                    table_type, len(type_list), expected_type_count))

        return type_list

    type_hints = typing.get_type_hints(job.__call__)
    if len(type_hints) < 2:
        raise TypeError("`job.__call__` has wrong number of annotations. At least 2 expected")

    if "return" not in type_hints:
        raise TypeError("`job.__call__` return type is not annotated")

    argument_type = next(iter(type_hints.values()))
    return_type = type_hints["return"]

    iterable = [typing.get_origin(tp) for tp in [typing.Generator, typing.Iterator, typing.Iterable]]
    if typing.get_origin(argument_type) in iterable:
        args = typing.get_args(argument_type)
        if not args:
            raise TypeError("Argument annotation `Iterable` must be parametrized")
        argument_type = args[0]

    if typing.get_origin(argument_type) is RowIterator:
        args = typing.get_args(argument_type)
        if len(args) != 1:
            raise TypeError("Argument annotation `RowIterator` must be parametrized")
        argument_type = args[0]

    input_tables_types = get_tables_types(argument_type, context.get_input_count(), "input")
    for i, tp in enumerate(input_tables_types):
        preparer.input(i, type=tp)

    if typing.get_origin(return_type) not in iterable or not typing.get_args(return_type):
        raise TypeError("Return type expected to be annotated as Generator[T]")

    iterable_item_type = extract_row_type(typing.get_args(return_type)[0])
    output_tables_types = get_tables_types(iterable_item_type, context.get_output_count(), "output")
    for i, tp in enumerate(output_tables_types):
        preparer.output(i, type=tp)
