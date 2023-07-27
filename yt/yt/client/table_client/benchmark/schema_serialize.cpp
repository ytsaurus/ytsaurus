#include <benchmark/benchmark.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/resource/resource.h>

using namespace NYT::NTableClient;
using namespace NYT::NYTree;
using namespace NYT::NYson;

////////////////////////////////////////////////////////////////////////////////

static void PullParser_TableSchema(benchmark::State& state)
{
    auto schemaYson = TYsonString(NResource::Find("schema.yson"));

    for (auto _ : state) {
        Y_DO_NOT_OPTIMIZE_AWAY(ConvertTo<TTableSchema>(schemaYson));
    }
}
BENCHMARK(PullParser_TableSchema);

static void NodeDeserialize_TableSchema(benchmark::State& state)
{
    auto schemaYson = TYsonString(NResource::Find("schema.yson"));

    for (auto _ : state) {
        auto schemaNode = ConvertTo<INodePtr>(schemaYson);
        Y_DO_NOT_OPTIMIZE_AWAY(ConvertTo<TTableSchema>(schemaNode));
    }
}
BENCHMARK(NodeDeserialize_TableSchema);

////////////////////////////////////////////////////////////////////////////////