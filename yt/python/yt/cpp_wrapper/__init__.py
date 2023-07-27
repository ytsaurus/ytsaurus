"""
Library for launching C++ jobs from YT Python wrapper.

To run a C++ job from Python you need to:
1. Write C++ job. Don't forget to register it with 'REGISTER_MAPPER'/'REGISTER_REDUCER' macro.
2. In Python pass 'CppJob(your_job_class_name)' to 'run_map'/'run_reduce'. Job will be initialized by default constructor.

Don't forget to use 'Y_SAVELOAD_JOB' in stateful jobs.
Such jobs will be initialized locally first.
It is possible to pass arguments for job initialization. To do this:
1. Define static 'TIntrusivePtr<IMapper/IReducer> FromNode(const TNode& node)' method in your job class. The method must return your job constructed from node.
2. Pass any YSON serializable object to CppJob: 'CppJob(your_job_class_name, {"arg1": val1, "arg2", val2})'.
It's also possible to pass None as an argument, it will be treated as null node.

Jobs with protobuf io are also supported. For such jobs you need to override 'PrepareOperation()' method and add needed .proto files.
Check mapreduce/yt/examples/python-tutorial/cpp_simple_map_protobuf and 'IJob::PrepareOperation' documentation.

Check mapreduce/yt/examples/python-tutorial/cpp_* for examples.
"""

from .cpp_wrapper import CppJob, exec_cpp_job  # noqa
