cimport cython

import os

from libcpp.vector cimport vector

from util.generic.ptr cimport TIntrusiveConstPtr
from util.generic.string cimport TString

from yt.wrapper.config import get_config
from yt.wrapper.transaction import get_current_transaction_id
from yt.wrapper.common import YtError
from yt.wrapper.ypath import TablePath

from yt import yson


cdef extern from "yt/cpp/mapreduce/interface/client_method_options.h" namespace "NYT":
    cdef cppclass TCreateClientOptions:
        TCreateClientOptions& Token(const TString&)
        TCreateClientOptions& TokenPath(const TString&)


cdef extern from "yt/cpp/mapreduce/interface/operation.h" namespace "NYT":
    cdef cppclass IStructuredJob:
        IStructuredJob() except +
    ctypedef TIntrusiveConstPtr[IStructuredJob] IStructuredJobPtr


cdef extern from "yt/cpp/mapreduce/interface/init.h" namespace "NYT":
    cdef cppclass TInitializeOptions:
        TInitializeOptions() except +
    void Initialize(const TInitializeOptions&) except +


cdef extern from "yt/cpp/mapreduce/client/py_helpers.h" namespace "NYT":
    IStructuredJobPtr ConstructJob(const TString& jobName, const TString& state) except +
    TString GetJobStateString(const IStructuredJob&) except +
    TString GetIOInfo(
        const IStructuredJob&,
        const TCreateClientOptions&,
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
        self.__name__ = "CppJob[{}]".format(self._mapper_name.decode("utf-8"))
        self._constructor_args_yson = b""
        if constructor_args is not _CONSTRUCTOR_ARGS_SENTINEL:
            self._constructor_args_yson = yson.dumps(constructor_args)

    def prepare_state_and_spec_patch(self, group_by, input_tables, output_tables, client):
        cdef IStructuredJobPtr job_ptr = ConstructJob(self._mapper_name, self._constructor_args_yson)
        cdef bytes state = <bytes> GetJobStateString(cython.operator.dereference(job_ptr.Get()))

        cdef config = get_config(client)

        cdef TCreateClientOptions create_options
        if config.get("token") is not None:
            create_options.Token(config["token"].encode("utf-8"))
        if config.get("token_path") is not None:
            create_options.TokenPath(config["token_path"].encode("utf-8"))

        cdef cluster = config["proxy"]["url"]
        if not isinstance(cluster, bytes):
            cluster = cluster.encode("utf-8")

        cdef tx_id = get_current_transaction_id(client)
        if not isinstance(tx_id, bytes):
            tx_id = tx_id.encode("utf-8")

        needed_columns = group_by if group_by is not None else []
        cdef bytes patch = <bytes> GetIOInfo(
            cython.operator.dereference(job_ptr.Get()),
            create_options,
            cluster,
            tx_id,
            yson.dumps(input_tables),
            yson.dumps(output_tables),
            yson.dumps(needed_columns),
        )
        spec_patch = yson.loads(patch)

        def count_intermediate(tables):
            intermediate_count = 0
            for table in tables:
                if table is not None:
                    break
                intermediate_count += 1
            return intermediate_count

        def update_paths(old_paths, new_paths, type):
            intermediate_count = count_intermediate(old_paths)
            if len(old_paths) - intermediate_count != len(new_paths):
                raise YtError(
                    "Length of {0}_table_paths provided from Python ({1}) differs from C++ patch ({2})."\
                    " It's a bug. Contact yt@".format(
                        type,
                        len(old_paths) - intermediate_count,
                        len(new_paths),
                    )
                )
            for index, table in enumerate(new_paths):
                new_table_path = TablePath(table, client=client)
                old_table_path = TablePath(old_paths[intermediate_count + index], client=client)

                new_table_path.attributes.update(old_table_path.attributes)
                old_paths[intermediate_count + index] = new_table_path

        if "input_table_paths" in spec_patch:
            # If a job has proto input, we can get table paths with column filters here.
            update_paths(input_tables, spec_patch["input_table_paths"], "input")
            del spec_patch["input_table_paths"]

        if "output_table_paths" in spec_patch:
            # We can get inferred schemas here
            update_paths(output_tables, spec_patch["output_table_paths"], "output")
            del spec_patch["output_table_paths"]

        return state, spec_patch


def exec_cpp_job(job_arguments):
    os.environ["YT_JOB_ARGUMENTS"] = yson.dumps(job_arguments).decode("UTF-8")
    Initialize(TInitializeOptions())
