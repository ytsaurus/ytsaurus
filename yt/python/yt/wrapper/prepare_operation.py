from .batch_helpers import create_batch_client
from .cypress_commands import get
from .errors import YtResolveError, YtResponseError
from .format import StructuredSkiffFormat
from .schema import TableSchema, _create_row_py_schema, _validate_py_schema, is_yt_dataclass
from .ypath import TablePath

from yt.yson import to_yson_type

from abc import ABCMeta, abstractmethod
from copy import deepcopy


class TypedJob:
    """
    Interface for jobs that use structured data as input and output.
    The types for input and output rows are specified in :func:`~TypedJob.prepare_operation`.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
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
        return

    @abstractmethod
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
        pass

    @abstractmethod
    def get_output_count(self):
        pass

    @abstractmethod
    def get_input_schemas(self):
        pass

    @abstractmethod
    def get_output_schemas(self):
        pass

    @abstractmethod
    def get_input_paths(self):
        pass

    @abstractmethod
    def get_output_paths(self):
        pass


class OperationPreparer:
    """
    Class used to specify input and output row types for a job.
    Also input column filter, input column renaming and output schemas can be specified.
    """

    def input(self, index, **kwargs):
        if not isinstance(index, int):
            raise TypeError("Expected index to be of type int")
        return self.inputs([index], **kwargs)

    def inputs(self, indices, type=None, column_filter=None, column_renaming=None):
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

    def output(self, index, **kwargs):
        if not isinstance(index, int):
            raise TypeError("Expected index to be of type int")
        return self.outputs([index], **kwargs)

    def outputs(self, indices, type=None, schema=None, infer_strict_schema=True):
        index_list = list(indices)
        if type is not None:
            if not is_yt_dataclass(type):
                raise TypeError("Output type must be a class marked with @yt_dataclass")
            self._set_elements("Output type", self._output_types, index_list, type)
            self._set_elements("Output schema", self._infer_strict_output_schemas, index_list, infer_strict_schema)
        if schema is not None:
            self._set_elements("Output schema", self._output_schemas, index_list, schema)
        return self

    def __init__(self, context):
        self._context = context
        self._input_count = self._context.get_input_count()
        self._output_count = self._context.get_output_count()
        self._input_types = [None] * self._input_count
        self._output_types = [None] * self._output_count
        self._output_schemas = [None] * self._output_count
        self._input_column_filters = [None] * self._input_count
        self._input_column_renamings = [{}] * self._input_count
        self._input_py_schemas = []
        self._output_py_schemas = []
        self._infer_strict_output_schemas = [True] * self._output_count

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
            py_schema = _create_row_py_schema(type_, input_schemas[index], column_renaming=column_renaming)
            self._input_py_schemas.append(py_schema)
            _validate_py_schema(py_schema, for_reading=True)
        for index, type_ in enumerate(self._output_types):
            if type_ is None:
                self._raise_missing("output", index)
            if self._output_schemas[index] is None:
                self._output_schemas[index] = output_schemas[index]
            py_schema = _create_row_py_schema(type_, self._output_schemas[index])
            _validate_py_schema(py_schema, for_reading=False)
            self._output_py_schemas.append(py_schema)
            if self._output_schemas[index] is None or self._output_schemas[index].is_empty_nonstrict():
                self._output_schemas[index] = TableSchema.from_row_type(
                    type_,
                    strict=self._infer_strict_output_schemas[index],
                )


def _request_schemas(paths, client):
    batch_client = create_batch_client(raise_errors=False, client=client)
    batch_responses = []
    for path in paths:
        if path is None:
            batch_responses.append(None)
            continue
        batch_responses.append(get(path + "/@", attributes=["schema"], client=batch_client))
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


class IntermediateOperationPreparationContext(OperationPreparationContext):
    def __init__(self, input_schemas, output_paths, client):
        self._input_schemas = input_schemas
        self._input_paths = [None] * len(self._input_schemas)
        self._output_paths = output_paths
        self._client = client
        self._output_schemas = None

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


def run_operation_preparation(job, context):
    assert isinstance(job, TypedJob)
    preparer = OperationPreparer(context)
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
