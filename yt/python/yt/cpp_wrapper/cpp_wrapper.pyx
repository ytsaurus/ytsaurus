cimport cython

from libcpp.vector cimport vector

from util.generic.ptr cimport TIntrusiveConstPtr
from util.generic.string cimport TString

from yt.wrapper.config import get_config
from yt.wrapper.transaction import get_current_transaction_id
from yt.wrapper.common import YtError
from yt.wrapper.ypath import TablePath

from yt import yson


cdef extern from "mapreduce/yt/interface/operation.h" namespace "NYT":
    cdef cppclass IStructuredJob:
        IStructuredJob() except +
    ctypedef TIntrusiveConstPtr[IStructuredJob] IStructuredJobPtr


cdef extern from "mapreduce/yt/interface/init.h" namespace "NYT":
    cdef cppclass TInitializeOptions:
        TInitializeOptions() except +
    void Initialize(int, const char**, const TInitializeOptions&) except +


cdef extern from "mapreduce/yt/client/py_helpers.h" namespace "NYT":
    IStructuredJobPtr ConstructJob(const TString& jobName, const TString& state) except +
    TString GetJobStateString(const IStructuredJob&) except +
    TString GetIOInfo(
        const IStructuredJob&,
        const TString&,
        const TString&,
        const TString&,
        const TString&,
        const TString&,
    ) except +


class _ConstructorArgsSentinelClass(object):
    __instance = None
    def __new__(cls):
        if cls.__instance is None:
            cls.__instance = object.__new__(cls)
            cls.__instance.name = "_ConstructorArgsSentinelClass"
        return cls.__instance
_CONSTRUCTOR_ARGS_SENTINEL = _ConstructorArgsSentinelClass()


class CppJob:
    """
    Creates C++ job. Check module doc for details.
    """
    def __init__(self, mapper_name=None, constructor_args=_CONSTRUCTOR_ARGS_SENTINEL):
        self._mapper_name = mapper_name if isinstance(mapper_name, bytes) else mapper_name.encode("utf-8")
        self._constructor_args_yson = b""
        if constructor_args is not _CONSTRUCTOR_ARGS_SENTINEL:
            self._constructor_args_yson = yson.dumps(constructor_args)

    def prepare_state_and_spec_patch(self, group_by, input_tables, output_tables, client):
        cdef IStructuredJobPtr job_ptr = ConstructJob(self._mapper_name, self._constructor_args_yson)
        cdef bytes state = <bytes> GetJobStateString(cython.operator.dereference(job_ptr.Get()))

        cdef cluster = get_config(client)["proxy"]["url"]
        if not isinstance(cluster, bytes):
            cluster = cluster.encode("utf-8")

        cdef tx_id = get_current_transaction_id(client)
        if not isinstance(tx_id, bytes):
            tx_id = tx_id.encode("utf-8")

        needed_columns = group_by if group_by is not None else []

        cdef bytes patch = <bytes> GetIOInfo(
            cython.operator.dereference(job_ptr.Get()),
            cluster,
            tx_id,
            yson.dumps(input_tables),
            yson.dumps(output_tables),
            yson.dumps(needed_columns),
        )
        spec_patch = yson.loads(patch)
        if "input_table_paths" in spec_patch:
            # If a job has proto input, we can get table paths with column filters here.
            input_table_paths = spec_patch["input_table_paths"]
            if len(input_table_paths) != len(input_tables):
                raise YtError(
                    "Length of input_table_paths provided from Python ({0}) differs from C++ patch ({1})."\
                    " It's a bug. Contact yt@".format(
                        len(input_tables),
                        len(input_table_paths),
                    )
                )
            for index, table in enumerate(input_table_paths):
                new_table_path = TablePath(table, client=client)
                old_table_path = TablePath(input_tables[index], client=client)

                new_table_path.attributes.update(old_table_path.attributes)
                input_tables[index] = new_table_path

            del spec_patch["input_table_paths"]

        return state, spec_patch


def exec_cpp_job(args):
    cdef vector[char*] argv
    for i in xrange(len(args)):
        argv.push_back(<char*>args[i])
    Initialize(len(args), argv.data(), TInitializeOptions())
