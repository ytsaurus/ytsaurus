from libcpp.string cimport string

import yt.yson

# reuse driver shutdown
import yt_driver_rpc_bindings


cdef extern from "yt/yt/library/tracing/py/init.h" namespace "NYT::NTracing":
    cdef void InitializeGlobalTracer(const string& )


def initialize_tracer(config):
    InitializeGlobalTracer(yt.yson.dumps(config))
