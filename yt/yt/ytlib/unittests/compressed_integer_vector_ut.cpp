#include <yt/core/test_framework/framework.h>

#include <yt/ytlib/table_chunk_format/compressed_integer_vector.h>

namespace NYT::NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

template <class T>
size_t Compress(const std::vector<T> &data, std::vector<ui64> *buffer)
{
    T maxValue = 0;
    if (!data.empty()) {
        auto it = std::max_element(data.begin(), data.end());
        maxValue = *it;
    }

    auto size = CompressedUnsignedVectorSizeInWords(maxValue, data.size());
    // NB: initialize with zeros!
    buffer->resize(size, 0);

    return CompressUnsignedVector(MakeRange(data), maxValue, buffer->data());
}

template <class T>
void Validate(const std::vector<T>& data, const TCompressedUnsignedVectorReader<T>& reader)
{
    EXPECT_EQ(data.size(), reader.GetSize());

    for (int i = 0; i < data.size(); ++i) {
        EXPECT_EQ(data[i], reader[i]);
    }
}

template <class T>
void DoTest(T value, size_t count)
{
    std::vector<T> data(count, value);
    std::vector<ui64> buffer;

    size_t size = Compress(data, &buffer);
    EXPECT_EQ(CompressedUnsignedVectorSizeInWords(value, count), size);

    auto reader = TCompressedUnsignedVectorReader<T>(buffer.data());

    Validate(data, reader);
}

template <class T>
void DoTestZero()
{
    T value = 0;
    DoTest(value, 0);
    DoTest(value, 10);
}

TEST(TCompressedIntegerVectorTest, TestZero)
{
    DoTestZero<ui8>();
    DoTestZero<ui16>();
    DoTestZero<ui32>();
    DoTestZero<ui64>();
}

template <class T>
void DoTestMax()
{
    T value = std::numeric_limits<T>::max();
    DoTest(value, 10);
}

TEST(TCompressedIntegerVectorTest, TestMax)
{
    DoTestMax<ui8>();
    DoTestMax<ui16>();
    DoTestMax<ui32>();
    DoTestMax<ui64>();
}

template <class T>
void DoTestPowerOfTwo()
{
    // Set half of the bits.
    T value = MaskLowerBits(1, sizeof(T) * 4);
    DoTest(value, 1000);
}

TEST(TCompressedIntegerVectorTest, TestPowerOfTwo)
{
    DoTestPowerOfTwo<ui8>();
    DoTestPowerOfTwo<ui16>();
    DoTestPowerOfTwo<ui32>();
    DoTestPowerOfTwo<ui64>();
}

template <class T>
void DoTestOdd()
{
    // 00 ... 010 ... 01
    T value = (1ULL << (sizeof(T) * 4)) + 1;
    DoTest(value, 1000);
}

TEST(TCompressedIntegerVectorTest, TestOdd)
{
    DoTestOdd<ui8>();
    DoTestOdd<ui16>();
    DoTestOdd<ui32>();
    DoTestOdd<ui64>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
