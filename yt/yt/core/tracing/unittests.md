# Unit testing

Unit tests for `tracing` are located in the following places:
* Some tests are located in `yt/yt/core/concurrency/unittests/scheduler_ut.cpp`. These tests verify tracing inside a single process. In particular, they test that `TraceContext` is passed between asynchronous calls.
* Some tests are located in `yt/yt/core/rpc/unittests/rpc_ut.cpp`. These tests verify that `TraceContext` is passed between RPC calls correctly.
