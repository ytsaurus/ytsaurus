#include <benchmark/benchmark.h>

#include <yt/yt/flow/library/cpp/misc/indexed_yson_string.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/ypath_resolver.h>

#include <util/stream/file.h>
#include <util/string/printf.h>
#include <util/string/split.h>

#include <map>
#include <random>
#include <utility>

namespace NYT::NFlow {
namespace {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

// A flow-view-shaped document: the big map of |childCount| heavy children sits five map levels deep,
// with scalar siblings and small "uncle" maps at every level so the heavy branch is not a lone chain.
// Each child is itself a deep per-stream subtree. |PartitionPathPrefix| is the ypath down to the map.
struct TBigYson
{
    TYsonString Document;
    std::vector<TString> Keys;
    TString PartitionPathPrefix;
    i64 SizeBytes = 0;
};

TBigYson MakeBigYson(int childCount, int streamsPerChild)
{
    TBigYson result;
    result.Keys.reserve(childCount);

    // clang-format off
    auto document = NYTree::BuildYsonStringFluently()
        .BeginMap()
            .Item("schema_version").Value(3)
            .Item("description").Value("flow view snapshot")
            .Item("summary").BeginMap()
                .Item("total").Value(childCount)
                .Item("failed").Value(0)
            .EndMap()
            .Item("root").BeginMap()
                .Item("kind").Value("flow")
                .Item("shards").BeginMap()
                    .Item("shard_meta").BeginMap()
                        .Item("count").Value(1)
                    .EndMap()
                    .Item("graph").BeginMap()
                        .Item("name").Value("swift-map")
                        .Item("partitions").DoMapFor(0, childCount, [&] (NYTree::TFluentMap fluent, int i) {
                            auto key = Sprintf("partition-%08d-aaaa-bbbb-cccc-000000000000", i);
                            result.Keys.push_back(key);
                            fluent.Item(key).BeginMap()
                                .Item("job_id").Value(Sprintf("job-%08d-dddd-eeee-ffff-000000000000", i))
                                .Item("is_finished").Value(false)
                                .Item("start_time").Value(1720000000000000ull + i)
                                .Item("detail").BeginMap()
                                    .Item("attempt").Value(1)
                                    .Item("host").Value("host-0000.example.net")
                                    .Item("streams").DoMapFor(0, streamsPerChild, [&] (NYTree::TFluentMap streamFluent, int s) {
                                        streamFluent.Item(Sprintf("stream-%05d", s)).BeginMap()
                                            .Item("epoch").Value(1000 + s)
                                            .Item("event_watermark").Value(1720000000000000ull + s)
                                            .Item("system_watermark").Value(1720000000000000ull + s)
                                            .Item("state").Value("Executing")
                                        .EndMap();
                                    })
                                .EndMap()
                            .EndMap();
                        })
                    .EndMap()
                .EndMap()
                .Item("epoch").Value(42)
            .EndMap()
        .EndMap();
    // clang-format on

    result.SizeBytes = std::ssize(document.AsStringBuf());
    result.PartitionPathPrefix = "/root/shards/graph/partitions/";
    result.Document = std::move(document);
    return result;
}

i64 ReadRssBytes()
{
    TFileInput in("/proc/self/status");
    TString line;
    while (in.ReadLine(line)) {
        if (line.StartsWith("VmRSS:")) {
            TVector<TString> tokens;
            StringSplitter(line).SplitBySet(" \t").SkipEmpty().Collect(&tokens);
            return FromString<i64>(tokens[1]) * 1024;
        }
    }
    return 0;
}

// The document and the index are expensive to build, so we memoize them across the capture cases
// that share the same shape/threshold.
const TBigYson& GetBigYson(int childCount, int streamsPerChild)
{
    static std::map<std::pair<int, int>, TBigYson> cache;
    auto key = std::pair(childCount, streamsPerChild);
    auto it = cache.find(key);
    if (it == cache.end()) {
        it = cache.emplace(key, MakeBigYson(childCount, streamsPerChild)).first;
    }
    return it->second;
}

const TIndexedYsonStringPtr& GetIndex(const TBigYson& big, i64 leafSizeThreshold)
{
    static std::map<i64, TIndexedYsonStringPtr> cache;
    auto it = cache.find(leafSizeThreshold);
    if (it == cache.end()) {
        i64 rssBefore = ReadRssBytes();
        auto index = TIndexedYsonString::Build(big.Document, leafSizeThreshold);
        i64 rssAfter = ReadRssBytes();
        auto stats = index->ComputeStats();
        Cerr << "INDEX_STATS threshold_KB=" << leafSizeThreshold / 1024
             << " nodes=" << stats.NodeCount << " leaves=" << stats.LeafCount
             << " maxLeaf_B=" << stats.MaxLeafBytes
             << " leafBytes_MB=" << stats.LeafBytes / (1024.0 * 1024.0)
             << " indexRSS_MB=" << (rssAfter - rssBefore) / (1024.0 * 1024.0) << Endl;
        it = cache.emplace(leafSizeThreshold, std::move(index)).first;
    }
    return it->second;
}

////////////////////////////////////////////////////////////////////////////////

// Baselines: constructing the document, and a full DOM parse+reserialize round-trip.
void BM_MakeBigYson(benchmark::State& state, int childCount, int streamsPerChild)
{
    for (auto _ : state) {
        auto big = MakeBigYson(childCount, streamsPerChild);
        benchmark::DoNotOptimize(big);
    }
    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_CAPTURE(BM_MakeBigYson, p3000_s330, 3000, 330)->Unit(benchmark::kMillisecond);

void BM_DomRoundTrip(benchmark::State& state, int childCount, int streamsPerChild)
{
    const auto& big = GetBigYson(childCount, streamsPerChild);
    for (auto _ : state) {
        auto s = ConvertToYsonString(NYTree::ConvertToNode(big.Document));
        benchmark::DoNotOptimize(s);
    }
    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_CAPTURE(BM_DomRoundTrip, p3000_s330, 3000, 330)->Unit(benchmark::kMillisecond);

// Flat streaming extraction: cost is proportional to the child's position, so a random child scans
// ~half the document.
void BM_ExtractRandomChild(benchmark::State& state, int childCount, int streamsPerChild)
{
    const auto& big = GetBigYson(childCount, streamsPerChild);
    std::minstd_rand rng(12345);
    std::uniform_int_distribution<int> pick(0, std::ssize(big.Keys) - 1);
    for (auto _ : state) {
        int idx = pick(rng);
        TString path = big.PartitionPathPrefix + big.Keys[idx];
        auto sub = NYTree::TryGetAny(big.Document.AsStringBuf(), path);
        benchmark::DoNotOptimize(sub);
    }
    state.SetItemsProcessed(state.iterations());
    state.counters["doc_MB"] = big.SizeBytes / (1024.0 * 1024.0);
}

BENCHMARK_CAPTURE(BM_ExtractRandomChild, p3000_s330, 3000, 330)->Unit(benchmark::kMillisecond);

// Random `/partition_job_statuses/<key>` lookup: O(1) map navigation, no document scan.
void BM_IndexRandomChild(benchmark::State& state, int childCount, int streamsPerChild, i64 leafSizeThreshold)
{
    const auto& big = GetBigYson(childCount, streamsPerChild);
    const auto& index = GetIndex(big, leafSizeThreshold);
    std::minstd_rand rng(12345);
    std::uniform_int_distribution<int> pick(0, std::ssize(big.Keys) - 1);
    for (auto _ : state) {
        int idx = pick(rng);
        TString path = big.PartitionPathPrefix + big.Keys[idx];
        auto sub = index->GetByPath(path);
        benchmark::DoNotOptimize(sub);
    }
    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_CAPTURE(BM_IndexRandomChild, leaf10KB, 3000, 330, 10 * 1024)->Unit(benchmark::kMicrosecond);
BENCHMARK_CAPTURE(BM_IndexRandomChild, leaf50KB, 3000, 330, 50 * 1024)->Unit(benchmark::kMicrosecond);
BENCHMARK_CAPTURE(BM_IndexRandomChild, leaf200KB, 3000, 330, 200 * 1024)->Unit(benchmark::kMicrosecond);

// Cost of building the index over the whole document — paid once per flow-view cache rebuild.
void BM_IndexBuild(benchmark::State& state, int childCount, int streamsPerChild, i64 leafSizeThreshold)
{
    const auto& big = GetBigYson(childCount, streamsPerChild);
    for (auto _ : state) {
        auto index = TIndexedYsonString::Build(big.Document, leafSizeThreshold);
        benchmark::DoNotOptimize(index);
    }
    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_CAPTURE(BM_IndexBuild, leaf10KB, 3000, 330, 10 * 1024)->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_IndexBuild, leaf50KB, 3000, 330, 50 * 1024)->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_IndexBuild, leaf200KB, 3000, 330, 200 * 1024)->Unit(benchmark::kMillisecond);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
