#include <yt/yt/core/misc/bit_packed_unsigned_vector.h>
#include <yt/yt/core/misc/bit_packing.h>

#include <benchmark/benchmark.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

template <int Width, class = void>
struct TValueTraits
{ };

template <int Width>
struct TValueTraits<Width, typename std::enable_if<(Width >= 1) && (Width <= 8)>::type>
{
    using T = ui8;
};

template <int Width>
struct TValueTraits<Width, typename std::enable_if<(Width > 8) && (Width <= 16)>::type>
{
    using T = ui16;
};

template <int Width>
struct TValueTraits<Width, typename std::enable_if<(Width > 16) && (Width <= 32)>::type>
{
    using T = ui32;
};

template <int Width>
struct TValueTraits<Width, typename std::enable_if<(Width > 32) && (Width <= 64)>::type>
{
    using T = ui64;
};

template <class T>
struct TDoTest
    : public TBitPackedUnsignedVectorReader<T>
{
    explicit TDoTest(const ui64* data)
        : TBitPackedUnsignedVectorReader<T>(data)
    { }
};

template <class T>
struct TDoTestNew
    : public std::unique_ptr<T[]>
{
    explicit TDoTestNew(TCompressedVectorView view)
        : std::unique_ptr<T[]>(new T[view.GetSize()])
    {
        view.UnpackTo(this->get());
    }

    explicit TDoTestNew(const ui64* data)
        : TDoTestNew(TCompressedVectorView(data))
    { }
};

template <class T>
struct TDoTestNewPartial
    : public std::unique_ptr<T[]>
{
    explicit TDoTestNewPartial(TCompressedVectorView view)
        : std::unique_ptr<T[]>(new T[view.GetSize()])
    {
        view.UnpackTo(this->get(), 0, view.GetSize());
    }

    explicit TDoTestNewPartial(const ui64* data)
        : TDoTestNewPartial(TCompressedVectorView(data))
    { }
};

template <int Width, template <class> class TTest>
void PackedVector_Unpack(benchmark::State& state)
{
    using T = typename TValueTraits<Width>::T;

    const size_t N = 100000;
    ui64 maxValue = (Width == 64) ? std::numeric_limits<ui64>::max() : ((1ULL << Width) - 1);

    std::vector<T> values(N);
    for (size_t i = 0; i < N - 1; ++i) {
        values[i] = rand() % maxValue;
    }
    values[N - 1] = maxValue;

    auto size = CompressedUnsignedVectorSizeInWords(maxValue, N);
    std::vector<ui64> buffer(size, 0);

    BitPackUnsignedVector(MakeRange(values), maxValue, buffer.data());

    int iterationCount = 0;
    while (state.KeepRunning()) {
        TTest<T> unpacked(buffer.data());

        if (!iterationCount) {
            for (size_t index = 0; index < N; ++index) {
                YT_VERIFY(values[index] == unpacked[index]);
            }
        }
        ++iterationCount;
    }

    state.SetBytesProcessed(iterationCount * CompressedUnsignedVectorSizeInBytes(maxValue, N));
}

#define TEST(width, name, test) \
    void PackedVector_Unpack_##width##_##name(benchmark::State& state) { \
    PackedVector_Unpack<width, test>(state); } \
    BENCHMARK(PackedVector_Unpack_##width##_##name);

#define XX(width) \
    TEST(width, Old, TDoTest) \
    TEST(width, New, TDoTestNew) \
    TEST(width, NewPartial, TDoTestNewPartial)

XX( 1)
XX( 2)
XX( 3)
XX( 4)
XX( 5)
XX( 6)
XX( 7)
XX( 8)
XX( 9)
XX(10)
XX(11)
XX(12)
XX(13)
XX(14)
XX(15)
XX(16)
XX(17)
XX(18)
XX(19)
XX(20)
XX(30)
XX(31)
XX(32)
XX(36)
XX(40)
XX(47)
XX(50)
XX(60)
XX(64)

#undef XX
#undef TEST

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT