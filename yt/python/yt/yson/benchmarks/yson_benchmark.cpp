#include <iostream>

#include <library/cpp/testing/common/env.h>

#include <library/cpp/yt/string/format.h>

#include <Python.h>

#include <benchmark/benchmark.h>

bool isInitialized = false;

template <class ...ExtraArgs>
void BenchmarkYson(benchmark::State& state, ExtraArgs&&... extraArgs)
{
    if (!isInitialized) {
        static const char* importStr = R"raw(
import yt.yson as yson
)raw";

        Py_Initialize();
        PyRun_SimpleString(importStr);

        isInitialized = true;
    }

    auto t = std::tuple<ExtraArgs...>(extraArgs...);
    auto alwaysCreateAttributesStr = std::get<0>(t);
    auto lazyStr = std::get<1>(t);
    auto filename = std::get<2>(t);

    auto setupArgsString = NYT::Format(
        R"raw(
create_attributes = %v
lazy = %v
with open('%v', 'rb') as fin:
    raw_data = fin.read()
)raw",
        alwaysCreateAttributesStr,
        lazyStr,
        filename);

    PyRun_SimpleString(setupArgsString.Data());

    static const char* codeStr = R"raw(
for _ in yson.loads(raw_data, yson_type="list_fragment", always_create_attributes=create_attributes, lazy=lazy):
    pass
)raw";

    for (auto _ : state) {
        PyRun_SimpleString(codeStr);
    }
}

static void SetUp(benchmark::internal::Benchmark* b)
{
    b->Unit(benchmark::kMillisecond)->MinTime(5);
}

BENCHMARK_CAPTURE(BenchmarkYson, yt_bench_scan, std::string("False"), std::string("False"), GetWorkPath() + "/data/yt_bench_scan.yson")->Apply(SetUp);
BENCHMARK_CAPTURE(BenchmarkYson, yt_bench_scan_attr, std::string("True"), std::string("False"), GetWorkPath() + "/data/yt_bench_scan.yson")->Apply(SetUp);
BENCHMARK_CAPTURE(BenchmarkYson, yt_bench_scan_lazy, std::string("False"), std::string("True"), GetWorkPath() + "/data/yt_bench_scan.yson")->Apply(SetUp);

BENCHMARK_CAPTURE(BenchmarkYson, skiff_bench, std::string("False"), std::string("False"), GetWorkPath() + "/data/skiff_bench.yson")->Apply(SetUp);
BENCHMARK_CAPTURE(BenchmarkYson, skiff_bench_attr, std::string("True"), std::string("False"), GetWorkPath() + "/data/skiff_bench.yson")->Apply(SetUp);
BENCHMARK_CAPTURE(BenchmarkYson, skiff_bench_lazy, std::string("False"), std::string("True"), GetWorkPath() + "/data/skiff_bench.yson")->Apply(SetUp);

BENCHMARK_CAPTURE(BenchmarkYson, event_log, std::string("False"), std::string("False"), GetWorkPath() + "/data/event_log_small.yson")->Apply(SetUp);
BENCHMARK_CAPTURE(BenchmarkYson, event_log_attr, std::string("True"), std::string("False"), GetWorkPath() + "/data/event_log_small.yson")->Apply(SetUp);
BENCHMARK_CAPTURE(BenchmarkYson, event_log_lazy, std::string("False"), std::string("True"), GetWorkPath() + "/data/event_log_small.yson")->Apply(SetUp);

template <class ...ExtraArgs>
void LazyBenchEventLogFullParse(benchmark::State& state, ExtraArgs&&... extraArgs)
{
    if (!isInitialized) {
        static const char* importStr = R"raw(
import yt.yson as yson
)raw";

        Py_Initialize();
        PyRun_SimpleString(importStr);

        isInitialized = true;
    }
    auto t = std::tuple<ExtraArgs...>(extraArgs...);
    auto alwaysCreateAttributesStr = std::get<0>(t);
    auto lazyStr = std::get<1>(t);
    auto filename = std::get<2>(t);

    auto setupArgsString = NYT::Format(
        R"raw(
create_attributes = %v
lazy = %v
with open('%v', 'rb') as fin:
    raw_data = fin.read()
)raw",
        alwaysCreateAttributesStr,
        lazyStr,
        filename);

    PyRun_SimpleString(setupArgsString.Data());

    static const char* codeStr = R"raw(
for el in yson.loads(raw_data, yson_type="list_fragment", always_create_attributes=create_attributes, lazy=lazy):
    if "resource_limits" in el:
        a = el.get("resource_limits")
    if "statistics" in el:
        a = el.get("statistics")
)raw";

    for (auto _ : state) {
        PyRun_SimpleString(codeStr);
    }
}

BENCHMARK_CAPTURE(LazyBenchEventLogFullParse, event_log, std::string("False"), std::string("False"), GetWorkPath() + "/data/event_log_small.yson")->Apply(SetUp);
BENCHMARK_CAPTURE(LazyBenchEventLogFullParse, event_log_lazy, std::string("False"), std::string("True"), GetWorkPath() + "/data/event_log_small.yson")->Apply(SetUp);
BENCHMARK_CAPTURE(LazyBenchEventLogFullParse, event_log_attr, std::string("True"), std::string("False"), GetWorkPath() + "/data/event_log_small.yson")->Apply(SetUp);
BENCHMARK_CAPTURE(LazyBenchEventLogFullParse, event_log_attr_lazy, std::string("True"), std::string("True"), GetWorkPath() + "/data/event_log_small.yson")->Apply(SetUp);
