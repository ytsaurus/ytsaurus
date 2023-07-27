#include <benchmark/benchmark.h>

#include <library/cpp/yt/coding/varint.h>

#include <util/generic/size_literals.h>

#include <array>
#include <vector>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr int IterationCount = 100;

template <class F>
auto GenerateValues(F&& func) -> std::vector<decltype(func(0))>
{
    std::vector<decltype(func(0))> result;
    for (int i = 0; i < IterationCount; ++i) {
        result.push_back(func(i));
    }
    return result;
}

const auto SmallValues = GenerateValues([] (int i) -> ui8 {
    return i & 0xf;
});

const auto MediumValues = GenerateValues([] (int i) -> ui32 {
    return (static_cast<ui32>(i) << 16) | i;
});

const auto LargeValues = GenerateValues([] (int i) -> ui64 {
    return (static_cast<ui64>(i) << 32) |  (static_cast<ui32>(i) << 16) | i;
});

template <class TWriter, class TReader>
void DoBenchmark(
    benchmark::State& state,
    TWriter&& writer,
    TReader&& reader)
{
    constexpr int BufferSize = MaxVarUint64Size * IterationCount;
    std::array<char, BufferSize> buffer;
    for (auto _ : state) {
        {
            char* ptr = buffer.data();
            for (int i = 0; i < IterationCount; ++i) {
                writer(ptr, i);
            }
        }
        {
            const char* ptr = buffer.data();
            ui64 sum = 0;
            for (int i = 0; i < IterationCount; ++i) {
                sum += reader(ptr);
            }
            DoNotOptimizeAway(sum);
        }
    }
}

Y_FORCE_INLINE ui64 DoReadVarUint32(const char*& ptr)
{
    ui32 value;
    ptr += ReadVarUint32(ptr, &value);
    return value;
}

Y_FORCE_INLINE ui64 DoReadVarUint64(const char*& ptr)
{
    ui64 value;
    ptr += ReadVarUint64(ptr, &value);
    return value;
}

void BM_SmallVarUint(benchmark::State& state)
{
    DoBenchmark(
        state,
        [] (char*& ptr, int i) {
            ptr += WriteVarUint32(ptr, SmallValues[i]);
        },
        DoReadVarUint32);
}

void BM_MediumVarUint(benchmark::State& state)
{
    DoBenchmark(
        state,
        [] (char*& ptr, int i) {
            ptr += WriteVarUint32(ptr, MediumValues[i]);
        },
        DoReadVarUint32);
}

void BM_LargeVarUint(benchmark::State& state)
{
    DoBenchmark(
        state,
        [] (char*& ptr, int i) {
            ptr += WriteVarUint64(ptr, LargeValues[i]);
        },
        DoReadVarUint64);
}

BENCHMARK(BM_SmallVarUint);
BENCHMARK(BM_MediumVarUint);
BENCHMARK(BM_LargeVarUint);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
