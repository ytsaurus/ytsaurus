#include <yt/yt/flow/library/cpp/misc/cow_tree.h>

#include <benchmark/benchmark.h>

#include <optional>
#include <random>
#include <set>
#include <vector>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr i64 TestRangeStart = 100;
constexpr i64 TestRangeEnd = 1'000'000;
constexpr i64 TestRangeMultiplier = 100;

class TTimePauserGuard
{
public:
    TTimePauserGuard(benchmark::State& state, bool pauseTiming)
        : State_(state)
        , PauseTiming_(pauseTiming)
    {
        if (PauseTiming_) {
            State_.PauseTiming();
        }
    }

    ~TTimePauserGuard()
    {
        if (PauseTiming_) {
            State_.ResumeTiming();
        }
    }

private:
    benchmark::State& State_;
    const bool PauseTiming_;
};

static const std::vector<i64>& GetKeys(i64 count)
{
    thread_local std::vector<i64> keys;
    if (std::ssize(keys) >= count) {
        return keys;
    }
    std::mt19937_64 random(42);
    keys.resize(count);
    for (i64 i = 0; i < count; ++i) {
        keys[i] = random();
    }
    return keys;
}

template <class TTested>
class TSetTester
{
public:
    TSetTester(benchmark::State& state)
        : State(state)
        , Size(state.range(0))
        , Keys(GetKeys(state.range(0)))
    { }

    void Fill(bool pauseTiming = false)
    {
        TTimePauserGuard pauser(State, pauseTiming);
        for (i64 i = 0; i < Size; i++) {
            Tested.insert(Keys[i]);
        }
    }

    void Deplete(bool pauseTiming = false)
    {
        TTimePauserGuard pauser(State, pauseTiming);
        for (i64 i = 0; i < Size; i++) {
            Tested.erase(Keys[i]);
        }
    }

    void Clear(bool pauseTiming = false)
    {
        TTimePauserGuard pauser(State, pauseTiming);
        Tested.clear();
    }

    void Find() const
    {
        for (i64 i = 0; i < Size; i++) {
            if (Tested.find(Keys[i]) == Tested.cend()) {
                std::abort();
            }
        }
    }

    void Iterate()
    {
        for (auto it = Tested.begin(); it != Tested.end(); ++it) {
            int value = *it;
            benchmark::DoNotOptimize(value);
        }
    }

    void Snapshot(bool pauseTiming = false)
    {
        TTimePauserGuard pauser(State, pauseTiming);
        auto snapshot = Tested.Snapshot();
        TestedSnapshot.emplace(snapshot);
    }

    void Unsnapshot(bool pauseTiming = false)
    {
        TTimePauserGuard pauser(State, pauseTiming);
        TestedSnapshot.reset();
    }

    i64 SnapshotSize() const
    {
        return TestedSnapshot ? std::ssize(*TestedSnapshot) : 0;
    }

    TTested Tested;
    std::optional<TCowSet<i64>::TSnapshot> TestedSnapshot;
    benchmark::State& State;
    i64 Size;
    const std::vector<i64>& Keys;
};

template <class TTested>
void BenchmarkBenchFill(benchmark::State& state)
{
    TSetTester<TTested> tester(state);
    for (auto _ : state) {
        tester.Fill(/*pauseTiming*/ false);
        tester.Clear(/*pauseTiming*/ true);
    }
    state.SetItemsProcessed(state.range(0) * state.iterations());
}

template <class TTested>
void BenchmarkBenchFind(benchmark::State& state)
{
    TSetTester<TTested> tester(state);
    tester.Fill(/*pauseTiming*/ false);
    for (auto _ : state) {
        tester.Find();
    }
    state.SetItemsProcessed(state.range(0) * state.iterations());
}

template <class TTested>
void BenchmarkBenchDeplete(benchmark::State& state)
{
    TSetTester<TTested> tester(state);
    for (auto _ : state) {
        tester.Fill(/*pauseTiming*/ true);
        tester.Deplete(/*pauseTiming*/ false);
        YT_VERIFY(tester.SnapshotSize() == 0);
    }
    state.SetItemsProcessed(state.range(0) * state.iterations());
}

template <class TTested>
void BenchmarkBenchIterate(benchmark::State& state)
{
    TSetTester<TTested> tester(state);
    tester.Fill(/*pauseTiming*/ false);
    for (auto _ : state) {
        tester.Iterate();
    }
    state.SetItemsProcessed(state.range(0) * state.iterations());
}

template <class TTested>
void BenchmarkBenchIterateWithSnapshot(benchmark::State& state)
{
    TSetTester<TTested> tester(state);
    tester.Fill(/*pauseTiming*/ false);
    for (auto _ : state) {
        tester.Snapshot(/*pauseTiming*/ true);
        tester.Iterate();
        tester.Unsnapshot(/*pauseTiming*/ true);
    }
    state.SetItemsProcessed(state.range(0) * state.iterations());
}

template <class TTested>
void BenchmarkBenchDepleteWithSnapshot(benchmark::State& state)
{
    TSetTester<TTested> tester(state);
    for (auto _ : state) {
        tester.Fill(/*pauseTiming*/ true);
        tester.Snapshot(/*pauseTiming*/ true);
        tester.Deplete(/*pauseTiming*/ false);
        YT_VERIFY(tester.SnapshotSize() == tester.Size);
        tester.Unsnapshot(/*pauseTiming*/ true);
    }
    state.SetItemsProcessed(state.range(0) * state.iterations());
}

using TBenchStdSet = std::set<i64>;
using TBenchCowSet = TCowSet<i64>;

// Keep the names used by existing benchmark filters and result files.
#define REGISTER_TREE_BENCHMARK(name, ...) \
    BENCHMARK_TEMPLATE(Benchmark##name, __VA_ARGS__)->Name("BM_" #name "<" #__VA_ARGS__ ">")

REGISTER_TREE_BENCHMARK(BenchFill, TBenchStdSet)->RangeMultiplier(TestRangeMultiplier)->Range(TestRangeStart, TestRangeEnd);
REGISTER_TREE_BENCHMARK(BenchFill, TBenchCowSet)->RangeMultiplier(TestRangeMultiplier)->Range(TestRangeStart, TestRangeEnd);

REGISTER_TREE_BENCHMARK(BenchFind, TBenchStdSet)->RangeMultiplier(TestRangeMultiplier)->Range(TestRangeStart, TestRangeEnd);
REGISTER_TREE_BENCHMARK(BenchFind, TBenchCowSet)->RangeMultiplier(TestRangeMultiplier)->Range(TestRangeStart, TestRangeEnd);

REGISTER_TREE_BENCHMARK(BenchDeplete, TBenchStdSet)->RangeMultiplier(TestRangeMultiplier)->Range(TestRangeStart, TestRangeEnd);
REGISTER_TREE_BENCHMARK(BenchDeplete, TBenchCowSet)->RangeMultiplier(TestRangeMultiplier)->Range(TestRangeStart, TestRangeEnd);
REGISTER_TREE_BENCHMARK(BenchDepleteWithSnapshot, TBenchCowSet)->RangeMultiplier(TestRangeMultiplier)->Range(TestRangeStart, TestRangeEnd);

REGISTER_TREE_BENCHMARK(BenchIterate, TBenchStdSet)->RangeMultiplier(TestRangeMultiplier)->Range(TestRangeStart, TestRangeEnd);
REGISTER_TREE_BENCHMARK(BenchIterate, TBenchCowSet)->RangeMultiplier(TestRangeMultiplier)->Range(TestRangeStart, TestRangeEnd);
REGISTER_TREE_BENCHMARK(BenchIterateWithSnapshot, TBenchCowSet)->RangeMultiplier(TestRangeMultiplier)->Range(TestRangeStart, TestRangeEnd);

} // namespace

#undef REGISTER_TREE_BENCHMARK

} // namespace NYT::NFlow
