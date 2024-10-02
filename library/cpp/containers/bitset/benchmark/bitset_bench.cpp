#include <library/cpp/containers/bitset/bitset.h>
#include <util/generic/hash_set.h>
#include <util/generic/xrange.h>

#include <library/cpp/testing/gbenchmark/benchmark.h>


template <typename TSet>
class TIntializedSetFixture
    : public ::benchmark::Fixture
{
public:
    void SetUp(const ::benchmark::State& state) override
    {
        for (ui32 i : xrange(state.range(0))) {
            Set.insert(i);
        }
    }
    void TearDown(const ::benchmark::State&) override
    {
        Set.clear();
    }

    void Contains(::benchmark::State& state)
    {
        size_t i = 0;
        for (auto _ : state) {
            benchmark::DoNotOptimize(Set.contains(i++));
        }
    }

protected:
    TSet Set;
};

template <typename TSet>
class TCombineFixture
    : public ::benchmark::Fixture
{
public:
    void SetUp(const ::benchmark::State& state) override
    {
        for (ui32 i : xrange(state.range(0))) {
            if (0 != i % 2) {
                OddSet.insert(i);
            } else {
                EvenSet.insert(i);
            }
        }
    }
    void TearDown(const ::benchmark::State&) override
    {
        OddSet.clear();
        EvenSet.clear();
    }

    void Or(::benchmark::State& state)
    {
        for (auto _ : state) {
            benchmark::DoNotOptimize(OddSet | EvenSet);
        }
    }

    void And(::benchmark::State& state)
    {
        for (auto _ : state) {
            benchmark::DoNotOptimize(OddSet & EvenSet);
        }
    }

    void Xor(::benchmark::State& state)
    {
        for (auto _ : state) {
            benchmark::DoNotOptimize(OddSet ^ EvenSet);
        }
    }

protected:
    TSet OddSet;
    TSet EvenSet;
};

template <typename TSet>
static void BMInsert(benchmark::State& state)
{
    TSet set;
    for (auto _ : state) {
        for (ui32 i : xrange(state.range(0))) {
            set.insert(i);
        }
    }
}

BENCHMARK_TEMPLATE_DEFINE_F(TIntializedSetFixture, BMContainsBitSetSigned, TBitSet<i32>)(benchmark::State& state)
{
    Contains(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(TIntializedSetFixture, BMContainsBitSetUnsigned, TBitSet<ui32>)(benchmark::State& state)
{
    Contains(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(TIntializedSetFixture, BMContainsHashSet, THashSet<i32>)(benchmark::State& state)
{
    Contains(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(TCombineFixture, BMBitSetOrSigned, TBitSet<i32>)(benchmark::State& state)
{
    Or(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(TCombineFixture, BMBitSetAndSigned, TBitSet<i32>)(benchmark::State& state)
{
    And(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(TCombineFixture, BMBitSetXorSigned, TBitSet<i32>)(benchmark::State& state)
{
    Xor(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(TCombineFixture, BMBitSetOrUnsigned, TBitSet<ui32>)(benchmark::State& state)
{
    Or(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(TCombineFixture, BMBitSetAndUnsigned, TBitSet<ui32>)(benchmark::State& state)
{
    And(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(TCombineFixture, BMBitSetXorUnsigned, TBitSet<ui32>)(benchmark::State& state)
{
    Xor(state);
}

BENCHMARK_TEMPLATE(BMInsert, TBitSet<i32>)->Range(8, 10 << 10);
BENCHMARK_TEMPLATE(BMInsert, TBitSet<ui32>)->Range(8, 10 << 10);
BENCHMARK_TEMPLATE(BMInsert, THashSet<i32>)->Range(8, 10 << 10);

BENCHMARK_REGISTER_F(TIntializedSetFixture, BMContainsBitSetSigned)->Range(8, 10 << 10);
BENCHMARK_REGISTER_F(TIntializedSetFixture, BMContainsBitSetUnsigned)->Range(8, 10 << 10);
BENCHMARK_REGISTER_F(TIntializedSetFixture, BMContainsHashSet)->Range(8, 10 << 10);

BENCHMARK_REGISTER_F(TCombineFixture, BMBitSetOrSigned)->Range(8, 10 << 10);
BENCHMARK_REGISTER_F(TCombineFixture, BMBitSetAndSigned)->Range(8, 10 << 10);
BENCHMARK_REGISTER_F(TCombineFixture, BMBitSetXorSigned)->Range(8, 10 << 10);

BENCHMARK_REGISTER_F(TCombineFixture, BMBitSetOrUnsigned)->Range(8, 10 << 10);
BENCHMARK_REGISTER_F(TCombineFixture, BMBitSetAndUnsigned)->Range(8, 10 << 10);
BENCHMARK_REGISTER_F(TCombineFixture, BMBitSetXorUnsigned)->Range(8, 10 << 10);
