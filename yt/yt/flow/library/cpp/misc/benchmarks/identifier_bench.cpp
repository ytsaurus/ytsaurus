#include <benchmark/benchmark.h>

#include <yt/yt/flow/library/cpp/misc/identifier.h>

#include <library/cpp/containers/absl/flat_hash_map.h>

#include <util/string/builder.h>

#include <thread>
#include <vector>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

YT_FLOW_DEFINE_IDENTIFIER_TYPEDEF(TBenchId);

////////////////////////////////////////////////////////////////////////////////
// Helpers.

// A non-empty identifier shared across threads.
const TBenchId SharedId("some-identifier-string");
// An empty identifier shared across threads.
const TBenchId EmptyId;

////////////////////////////////////////////////////////////////////////////////
// Hashing.

std::vector<TBenchId> MakeMessageLikeIds(int count)
{
    std::vector<TBenchId> ids;
    ids.reserve(count);
    for (int i = 0; i < count; ++i) {
        ids.emplace_back(std::string_view(::TStringBuilder() << "000000000004d9e6-event12345678901234567890:" << 1000000000 + i));
    }
    return ids;
}

void BM_BenchIdentifierHash(benchmark::State& state)
{
    const auto ids = MakeMessageLikeIds(1024);
    i64 total = 0;
    size_t index = 0;
    for (auto _ : state) {
        auto hash = THash<TBenchId>{}(ids[index]);
        benchmark::DoNotOptimize(hash);
        index = (index + 1) & 1023;
        ++total;
    }
    state.SetItemsProcessed(total);
}

BENCHMARK(BM_BenchIdentifierHash)->Threads(1);

void BM_BenchIdentifierHashMapFind(benchmark::State& state)
{
    const auto ids = MakeMessageLikeIds(65536);
    absl::flat_hash_map<TBenchId, i64, THash<TBenchId>> map;
    for (const auto& id : ids) {
        map[id] = 1;
    }
    i64 total = 0;
    size_t index = 0;
    for (auto _ : state) {
        auto it = map.find(ids[index]);
        benchmark::DoNotOptimize(it);
        index = (index + 1) & 65535;
        ++total;
    }
    state.SetItemsProcessed(total);
}

BENCHMARK(BM_BenchIdentifierHashMapFind)->Threads(1);

////////////////////////////////////////////////////////////////////////////////
// Construction.

void BM_BenchIdentifierConstructFromStringView(benchmark::State& state)
{
    i64 total = 0;
    for (auto _ : state) {
        TBenchId id("some-identifier-string");
        benchmark::DoNotOptimize(id);
        ++total;
    }
    state.SetItemsProcessed(total);
}

BENCHMARK(BM_BenchIdentifierConstructFromStringView)->Threads(1)->Threads(4)->Threads(16);

void BM_BenchIdentifierDefaultConstruct(benchmark::State& state)
{
    i64 total = 0;
    for (auto _ : state) {
        TBenchId id;
        benchmark::DoNotOptimize(id);
        ++total;
    }
    state.SetItemsProcessed(total);
}

BENCHMARK(BM_BenchIdentifierDefaultConstruct)->Threads(1)->Threads(4)->Threads(16);

////////////////////////////////////////////////////////////////////////////////
// Copy assignment (stresses refcount on shared data).

void BM_BenchIdentifierCopyAssignNonEmpty(benchmark::State& state)
{
    TBenchId dst;
    i64 total = 0;
    for (auto _ : state) {
        dst = SharedId;
        benchmark::DoNotOptimize(dst);
        ++total;
    }
    state.SetItemsProcessed(total);
}

BENCHMARK(BM_BenchIdentifierCopyAssignNonEmpty)->Threads(1)->Threads(4)->Threads(16);

void BM_BenchIdentifierCopyAssignEmpty(benchmark::State& state)
{
    TBenchId dst;
    i64 total = 0;
    for (auto _ : state) {
        dst = EmptyId;
        benchmark::DoNotOptimize(dst);
        ++total;
    }
    state.SetItemsProcessed(total);
}

// Many threads copying the empty singleton stresses EmptyData()'s refcount.
BENCHMARK(BM_BenchIdentifierCopyAssignEmpty)->Threads(1)->Threads(4)->Threads(16);

////////////////////////////////////////////////////////////////////////////////
// Move assignment (no refcount change).

void BM_BenchIdentifierMoveAssign(benchmark::State& state)
{
    i64 total = 0;
    for (auto _ : state) {
        TBenchId src("some-identifier-string");
        TBenchId dst;
        dst = std::move(src);
        benchmark::DoNotOptimize(dst);
        ++total;
    }
    state.SetItemsProcessed(total);
}

BENCHMARK(BM_BenchIdentifierMoveAssign)->Threads(1)->Threads(4)->Threads(16);

////////////////////////////////////////////////////////////////////////////////
// Comparison.

void BM_BenchIdentifierCompareEqual(benchmark::State& state)
{
    TBenchId a("some-identifier-string");
    TBenchId b("some-identifier-string");
    i64 total = 0;
    for (auto _ : state) {
        bool result = (a == b);
        benchmark::DoNotOptimize(result);
        ++total;
    }
    state.SetItemsProcessed(total);
}

BENCHMARK(BM_BenchIdentifierCompareEqual)->Threads(1)->Threads(4)->Threads(16);

void BM_BenchIdentifierCompareNotEqual(benchmark::State& state)
{
    TBenchId a("some-identifier-string");
    TBenchId b("other-identifier-string");
    i64 total = 0;
    for (auto _ : state) {
        bool result = (a == b);
        benchmark::DoNotOptimize(result);
        ++total;
    }
    state.SetItemsProcessed(total);
}

BENCHMARK(BM_BenchIdentifierCompareNotEqual)->Threads(1)->Threads(4)->Threads(16);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
