#include <benchmark/benchmark.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/ytlib/scheduler/config.h>

namespace NYT::NYTree {
namespace {

using namespace NYson;
using namespace NScheduler;

using TConfig = TMapReduceOperationSpec;

////////////////////////////////////////////////////////////////////////////////

TIntrusivePtr<TConfig> CreateValidConfig()
{
    auto config = New<TConfig>();
    config->InputTablePaths = {"//tmp/input"};
    config->OutputTablePaths = {"//tmp/output"};
    config->ReduceBy = {"key"};
    return config;
}

void BM_YsonStruct_Save(benchmark::State& state)
{
    auto config = CreateValidConfig();
    auto sourceNode = ConvertToNode(config);
    auto configTarget = New<TConfig>();
    Y_UNUSED(ConvertToYsonString(config));
    while (state.KeepRunning()) {
        Y_UNUSED(ConvertToYsonString(config));
    }
}

BENCHMARK(BM_YsonStruct_Save)
    ->Threads(1)
    ->Threads(2)
    ->Threads(10);

////////////////////////////////////////////////////////////////////////////////

void BM_YsonStruct_LoadFromNode(benchmark::State& state)
{
    auto config = CreateValidConfig();
    auto sourceYson = ConvertToYsonString(config);
    auto configTarget = New<TConfig>();
    while (state.KeepRunning()) {
        auto sourceNode = ConvertToNode(sourceYson);
        Deserialize(configTarget, sourceNode);
    }
}

BENCHMARK(BM_YsonStruct_LoadFromNode)
    ->Threads(1)
    ->Threads(2)
    ->Threads(10);

////////////////////////////////////////////////////////////////////////////////

void BM_YsonStruct_LoadFromCursor(benchmark::State& state)
{
    auto config = CreateValidConfig();
    auto sourceYson = ConvertToYsonString(config).ToString();
    auto configTarget = New<TConfig>();
    while (state.KeepRunning()) {
        TStringInput input(sourceYson);
        TYsonPullParser parser(&input, EYsonType::Node);
        TYsonPullParserCursor cursor(&parser);
        Deserialize(*configTarget, &cursor);
    }
}

BENCHMARK(BM_YsonStruct_LoadFromCursor)
    ->Threads(1)
    ->Threads(2)
    ->Threads(10);

////////////////////////////////////////////////////////////////////////////////

void BM_YsonStruct_CreateLoadFromNode(benchmark::State& state)
{
    auto config = CreateValidConfig();
    auto output = ConvertToYsonString(config);
    while (state.KeepRunning()) {
        auto newConfig = New<TConfig>();
        Deserialize(newConfig, ConvertToNode(output));
    }
}

BENCHMARK(BM_YsonStruct_CreateLoadFromNode)
    ->Threads(1)
    ->Threads(2)
    ->Threads(10);

////////////////////////////////////////////////////////////////////////////////

void BM_YsonStruct_CreateLoadFromCursor(benchmark::State& state)
{
    auto config = CreateValidConfig();
    auto sourceYson = ConvertToYsonString(config).ToString();
    while (state.KeepRunning()) {
        TStringInput input(sourceYson);
        TYsonPullParser parser(&input, EYsonType::Node);
        TYsonPullParserCursor cursor(&parser);
        auto configTarget = New<TConfig>();
        Deserialize(*configTarget, &cursor);
    }
}

BENCHMARK(BM_YsonStruct_CreateLoadFromCursor)
    ->Threads(1)
    ->Threads(2)
    ->Threads(10);

////////////////////////////////////////////////////////////////////////////////

void BM_YsonStruct_Create(benchmark::State& state)
{
    while (state.KeepRunning()) {
        Y_UNUSED(New<TConfig>());
    }
}

BENCHMARK(BM_YsonStruct_Create)
    ->Threads(1)
    ->Threads(2)
    ->Threads(10);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYTree
