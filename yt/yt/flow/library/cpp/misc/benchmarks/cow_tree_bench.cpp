#include <benchmark/benchmark.h>

#include <yt/yt/flow/library/cpp/misc/cow_tree.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr i64 TestRangeStart = 100;
constexpr i64 TestRangeEnd = 1000000;
constexpr i64 TestRangeMultiplier = 100;

////////////////////////////////////////////////////////////////////////////////

class TTimePauserGuard
{
public:
    TTimePauserGuard(benchmark::State& state, bool pauseTiming)
        : State(state)
        , PauseTiming_(pauseTiming)
    {
        if (PauseTiming_) {
            State.PauseTiming();
        }
    }

    ~TTimePauserGuard()
    {
        if (PauseTiming_) {
            State.ResumeTiming();
        }
    }

private:
    benchmark::State& State;
    bool PauseTiming_;
};

static const std::vector<i64>& GetKeys(i64 count)
{
    thread_local std::vector<i64> keys;
    if (std::ssize(keys) >= count) {
        return keys;
    }
    keys.resize(count);
    for (i64 i = 0; i < count; i++) {
        keys[i] = RandomNumber<ui64>();
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
        TestedSnapshot = Tested;
    }

    void Unsnapshot(bool pauseTiming = false)
    {
        TTimePauserGuard pauser(State, pauseTiming);
        TestedSnapshot.clear();
    }

    TTested Tested;
    TTested TestedSnapshot;
    benchmark::State& State;
    i64 Size;
    const std::vector<i64>& Keys;
};

template <class TTested>
void BM_BenchFill(benchmark::State& state)
{
    TSetTester<TTested> tester(state);
    for (auto _ : state) {
        tester.Fill(/*pauseTiming*/ false);
        tester.Clear(/*pauseTiming*/ true);
    }
    state.SetItemsProcessed(state.range(0) * state.iterations());
}

template <class TTested>
void BM_BenchFind(benchmark::State& state)
{
    TSetTester<TTested> tester(state);
    tester.Fill(/*pauseTiming*/ false);
    for (auto _ : state) {
        tester.Find();
    }
    state.SetItemsProcessed(state.range(0) * state.iterations());
}

template <class TTested>
void BM_BenchDeplete(benchmark::State& state)
{
    TSetTester<TTested> tester(state);
    for (auto _ : state) {
        tester.Fill(/*pauseTiming*/ true);
        tester.Deplete(/*pauseTiming*/ false);
        YT_VERIFY(std::ssize(tester.TestedSnapshot) == 0);
    }
    state.SetItemsProcessed(state.range(0) * state.iterations());
}

template <class TTested>
void BM_BenchIterate(benchmark::State& state)
{
    TSetTester<TTested> tester(state);
    tester.Fill(/*pauseTiming*/ false);
    for (auto _ : state) {
        tester.Iterate();
    }
    state.SetItemsProcessed(state.range(0) * state.iterations());
}

template <class TTested>
void BM_BenchIterateWithSnapshot(benchmark::State& state)
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
void BM_BenchDepleteWithSnapshot(benchmark::State& state)
{
    TSetTester<TTested> tester(state);
    for (auto _ : state) {
        tester.Fill(/*pauseTiming*/ true);
        tester.Snapshot(/*pauseTiming*/ true);
        tester.Deplete(/*pauseTiming*/ false);
        YT_VERIFY(std::ssize(tester.TestedSnapshot) == tester.Size);
        tester.Unsnapshot(/*pauseTiming*/ true);
    }
    state.SetItemsProcessed(state.range(0) * state.iterations());
}

using TStdSet = std::set<i64>;
using TCowSet = TCowTree<i64, void, 8>;

BENCHMARK_TEMPLATE(BM_BenchFill, TStdSet)->RangeMultiplier(TestRangeMultiplier)->Range(TestRangeStart, TestRangeEnd);
BENCHMARK_TEMPLATE(BM_BenchFill, TCowSet)->RangeMultiplier(TestRangeMultiplier)->Range(TestRangeStart, TestRangeEnd);

BENCHMARK_TEMPLATE(BM_BenchFind, TStdSet)->RangeMultiplier(TestRangeMultiplier)->Range(TestRangeStart, TestRangeEnd);
BENCHMARK_TEMPLATE(BM_BenchFind, TCowSet)->RangeMultiplier(TestRangeMultiplier)->Range(TestRangeStart, TestRangeEnd);

BENCHMARK_TEMPLATE(BM_BenchDeplete, TStdSet)->RangeMultiplier(TestRangeMultiplier)->Range(TestRangeStart, TestRangeEnd);
BENCHMARK_TEMPLATE(BM_BenchDeplete, TCowSet)->RangeMultiplier(TestRangeMultiplier)->Range(TestRangeStart, TestRangeEnd);
BENCHMARK_TEMPLATE(BM_BenchDepleteWithSnapshot, TCowSet)->RangeMultiplier(TestRangeMultiplier)->Range(TestRangeStart, TestRangeEnd);

BENCHMARK_TEMPLATE(BM_BenchIterate, TStdSet)->RangeMultiplier(TestRangeMultiplier)->Range(TestRangeStart, TestRangeEnd);
BENCHMARK_TEMPLATE(BM_BenchIterate, TCowSet)->RangeMultiplier(TestRangeMultiplier)->Range(TestRangeStart, TestRangeEnd);
BENCHMARK_TEMPLATE(BM_BenchIterateWithSnapshot, TCowSet)->RangeMultiplier(TestRangeMultiplier)->Range(TestRangeStart, TestRangeEnd);

} // namespace
} // namespace NYT::NFlow
