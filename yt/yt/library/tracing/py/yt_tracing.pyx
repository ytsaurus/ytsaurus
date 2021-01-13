from util.generic.string cimport TString

import yt.yson

# reuse driver shutdown
import yt_driver_rpc_bindings


cdef extern from "yt/yt/library/tracing/py/init.h" namespace "NYT::NTracing":
    cdef void InitializeGlobalTracer(const TString& )


def initialize_tracer(config):
    InitializeGlobalTracer(yt.yson.dumps(config))
