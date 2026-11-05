#include <benchmark/benchmark.h>

#include <yt/yt/flow/extensions/clickhouse/cpp/sink.h>

#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <yt/yt/flow/library/cpp/misc/lexicographically_serialize.h>

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/schema.h>

namespace NYT::NFlow {
namespace {

using namespace NTableClient;

void RunBatchPreparation(benchmark::State& state, int shardCount)
{
    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("user_id", SimpleLogicalType(ESimpleLogicalValueType::Int64)),
        TColumnSchema("data", SimpleLogicalType(ESimpleLogicalValueType::Int64)),
    });
    auto streamSpec = New<TStreamSpec>();
    streamSpec->Schema = schema;
    THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>> specs;
    specs[TStreamId("test")][TStreamSpecId(1)] = streamSpec;
    auto storage = New<TComputationStreamSpecStorage>(
        New<TStreamSpecs>(specs),
        New<TTableSchema>(),
        /*evaluatorCache*/ nullptr);
    auto parameters = New<TCommonClickHouseSinkParameters>();
    parameters->Table = "events";
    if (shardCount == 1) {
        parameters->Host = "host";
    } else {
        for (int index = 0; index < shardCount; ++index) {
            parameters->ShardHosts.emplace(std::to_string(index), std::vector<std::string>{"host"});
        }
    }
    TClickHouseShardRouter router(ResolveShards(*parameters), {"user_id"}, {schema});
    TClickHouseBlockBuilder blockBuilder(ResolveColumns({
            {.Name = "user_id", .Type = "Int64"},
            {.Name = "data", .Type = "Int64"},
                                                        },
        schema));
    std::vector<TOutputMessageConstPtr> messages;
    messages.reserve(state.range(0));
    for (i64 id = 0; id < state.range(0); ++id) {
        TMessageBuilder builder("test", schema);
        builder.SetMessageId(TMessageId(LexicographicallySerialize(id)));
        builder.SetSystemTimestamp(TSystemTimestamp(1700000000));
        builder.SetAlignmentTimestamp(TSystemTimestamp(1700000000));
        builder.SetEventTimestamp(TSystemTimestamp(1700000000));
        builder.Payload().Set<i64>(id % 997, "user_id");
        builder.Payload().Set<i64>(id, "data");
        messages.push_back(New<TOutputMessage>(builder.Finish(), storage));
    }
    const std::optional<std::string> token = "batch";
    for (auto iteration : state) {
        auto writes = BuildClickHouseShardWrites(router, blockBuilder, token, messages);
        benchmark::DoNotOptimize(writes.data());
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * state.range(0));
}

void BM_ClickHouseUnshardedBuild(benchmark::State& state)
{
    RunBatchPreparation(state, 1);
}

void BM_ClickHouseShardRouting(benchmark::State& state)
{
    RunBatchPreparation(state, state.range(1));
}

BENCHMARK(BM_ClickHouseUnshardedBuild)->Arg(1024)->Arg(8192);
BENCHMARK(BM_ClickHouseShardRouting)
    ->Args({1024, 2})
    ->Args({1024, 8})
    ->Args({1024, 32})
    ->Args({8192, 2})
    ->Args({8192, 8})
    ->Args({8192, 32});

} // namespace
} // namespace NYT::NFlow
