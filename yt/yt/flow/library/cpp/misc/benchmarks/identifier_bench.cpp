#include <benchmark/benchmark.h>

#include <yt/yt/flow/library/cpp/misc/identifier.h>

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
