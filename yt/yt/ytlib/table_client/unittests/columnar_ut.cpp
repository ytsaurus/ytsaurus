#include "helpers.h"

#include <yt/ytlib/table_client/columnar.h>

#include <yt/client/table_client/logical_type.h>

#include <yt/core/test_framework/framework.h>

#include <array>

namespace NYT::NTableClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TBuildValidityBitmapFromDictionaryIndexesWithZeroNullTest, Empty)
{
    BuildValidityBitmapFromDictionaryIndexesWithZeroNull(
        TRange<ui32>(),
        TMutableRef());
}

TEST(TBuildValidityBitmapFromDictionaryIndexesWithZeroNullTest, LessThanByte)
{
    std::array<ui32, 6> indexes{0, 0, 1, 3, 4, 0};
    ui8 result;
    BuildValidityBitmapFromDictionaryIndexesWithZeroNull(
        MakeRange(indexes),
        TMutableRef(&result, 1));
    EXPECT_EQ(0x1c, result);
}

TEST(TBuildValidityBitmapFromDictionaryIndexesWithZeroNullTest, TenBytes)
{
    std::array<ui32, 80> indexes;
    for (int i = 0; i < indexes.size(); ++i) {
        indexes[i] = i % 2;
    }
    std::array<ui8, indexes.size() / 8> result;
    BuildValidityBitmapFromDictionaryIndexesWithZeroNull(
        MakeRange(indexes),
        TMutableRef(&result, result.size()));
    for (int i = 0; i < result.size(); ++i) {
        EXPECT_EQ(0xaa, result[i]);
    }
}

TEST(TBuildValidityBitmapFromDictionaryIndexesWithZeroNullTest, ManyBits)
{
    std::array<ui32, 8001> indexes;
    for (int i = 0; i < indexes.size(); ++i) {
        indexes[i] = i % 2;
    }
    std::array<ui8, indexes.size() / 8 + 1> result;
    BuildValidityBitmapFromDictionaryIndexesWithZeroNull(
        MakeRange(indexes),
        TMutableRef(&result, result.size()));
    for (int i = 0; i < result.size() - 1; ++i) {
        EXPECT_EQ(0xaa, result[i]);
    }
    EXPECT_EQ(0, result[result.size() - 1]);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TBuildDictionaryIndexesFromDictionaryIndexesWithZeroNullTest, Empty)
{
    BuildDictionaryIndexesFromDictionaryIndexesWithZeroNull(
        TRange<ui32>(),
        TMutableRange<ui32>());
}

TEST(TBuildDictionaryIndexesFromDictionaryIndexesWithZeroNullTest, NonEmpty)
{
    std::array<ui32, 6> indexes{0, 1, 2, 3, 4, 5};
    std::array<ui32, indexes.size()> result;
    BuildDictionaryIndexesFromDictionaryIndexesWithZeroNull(
        MakeRange(indexes),
        MakeMutableRange(result));
    for (int i = 1; i < result.size(); ++i) {
        EXPECT_EQ(indexes[i] - 1, result[i]);
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TCountNullsInDictionaryIndexesWithZeroNullTest, Empty)
{
    EXPECT_EQ(0, CountNullsInDictionaryIndexesWithZeroNull(TRange<ui32>()));
}

TEST(TCountNullsInDictionaryIndexesWithZeroNullTest, NonEmpty)
{
    std::array<ui32, 6> indexes{0, 1, 2, 0, 4, 5};
    EXPECT_EQ(2, CountNullsInDictionaryIndexesWithZeroNull(MakeRange(indexes)));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TCountOnesInBitmapTest, Empty)
{
    EXPECT_EQ(0, CountOnesInBitmap(TRef(), 0, 0));
}

TEST(TCountOnesInBitmapTest, SingleByte)
{
    ui8 byte = 0xFF;
    for (int i = 0; i < 8; ++i) {
        for (int j = i; j < 8; ++j) {
            EXPECT_EQ(j - i, CountOnesInBitmap(TRef(&byte, 1), i, j))
                << "i = " << i << " j = " << j;
        }
    }
}

TEST(TCountOnesInBitmapTest, SevenBytes)
{
    std::array<ui8, 7> bytes = {0, 0xff, 0xff, 0xff, 0, 0, 1};
    auto bitmap = TRef(bytes.data(), bytes.size());
    EXPECT_EQ(25, CountOnesInBitmap(bitmap, 0, 56));
}

TEST(TCountOnesInBitmapTest, ThreeQwords)
{
    std::array<ui64, 3> qwords = {1, 1, 1};
    auto bitmap = TRef(qwords.data(), 8 * qwords.size());
    EXPECT_EQ(3, CountOnesInBitmap(bitmap, 0, 64 * 3));
}

TEST(TCountOnesInBitmapTest, QwordBorder)
{
    std::array<ui64, 2> qwords = {0xffffffffffffffffULL, 0xffffffffffffffffULL};
    auto bitmap = TRef(qwords.data(), 8 * qwords.size());
    for (int i = 0; i < 10; ++i) {
        for (int j = 64; j < 74; ++j) {
            EXPECT_EQ(j - i, CountOnesInBitmap(bitmap, i, j))
                << "i = " << i << " j = " << j;
        }
    }
}

TEST(TCountOnesInBitmapTest, WholeQwordInTheMiddle)
{
    std::array<ui64, 3> qwords = {0xffffffffffffffffULL, 1, 0xffffffffffffffffULL};
    auto bitmap = TRef(qwords.data(), 8 * qwords.size());
    for (int i = 0; i < 10; ++i) {
        for (int j = 128; j < 138; ++j) {
            EXPECT_EQ(j - i - 63, CountOnesInBitmap(bitmap, i, j))
                << "i = " << i << " j = " << j;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

class TCopyBitmapRangeTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<int, int>>
{ };

TEST_P(TCopyBitmapRangeTest, CheckAll)
{
    std::array<ui64, 3> qwords = {0x1234567812345678ULL, 0x1234567812345678ULL, 0xabcdabcdabcdabcdULL};
    auto bitmap = TRef(qwords.data(), 8 * qwords.size());
    std::array<ui8, 24> result;
    std::array<ui8, 24> resultNegated;
    auto [startIndex, endIndex] = GetParam();
    auto byteCount = GetBitmapByteSize(endIndex - startIndex);
    CopyBitmapRange(bitmap, startIndex, endIndex, TMutableRef(result.data(), byteCount));
    CopyBitmapRangeNegated(bitmap, startIndex, endIndex, TMutableRef(resultNegated.data(), byteCount));

    auto getBit = [] (const auto& array, int index) {
        return (reinterpret_cast<const char*>(array.begin())[index / 8] & (1U << (index % 8))) != 0;
    };

    for (int i = startIndex; i < endIndex; ++i) {
        auto srcBit = getBit(bitmap, i);
        auto dstBit = getBit(result, i - startIndex);
        auto inveredDstBit = getBit(resultNegated, i - startIndex);
        EXPECT_EQ(srcBit, dstBit) << "i = " << i;
        EXPECT_NE(srcBit, inveredDstBit) << "i = " << i;
    }
}

INSTANTIATE_TEST_SUITE_P(
    TCopyBitmapRangeTest,
    TCopyBitmapRangeTest,
    ::testing::Values(
        std::make_tuple(  0,   0),
        std::make_tuple(  0,  64),
        std::make_tuple(  0, 192),
        std::make_tuple( 64, 128),
        std::make_tuple(  8,  16),
        std::make_tuple( 10,  13),
        std::make_tuple(  5, 120),
        std::make_tuple( 23,  67),
        std::make_tuple(  1, 191)));

////////////////////////////////////////////////////////////////////////////////

TEST(TTranslateRleIndexTest, CheckAll)
{
    std::array<ui64, 8> rleIndexes{0, 1, 3, 10, 11, 15, 20, 21};
    for (i64 i = 0; i < rleIndexes[rleIndexes.size() - 1] + 10; ++i) {
        auto j = TranslateRleIndex(MakeRange(rleIndexes), i);
        EXPECT_LE(rleIndexes[j], i);
        if (j < rleIndexes.size() - 1) {
            EXPECT_LT(i, rleIndexes[j + 1]);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

class TDecodeRleVectorTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<int, int, std::vector<int>>>
{ };

TEST_P(TDecodeRleVectorTest, CheckAll)
{
    std::vector<int> values{1, 2, 3, 4, 5, 6};
    std::vector<ui64> rleIndexes{0, 1, 10, 11, 20, 25};
    const auto& [startIndex, endIndex, expected] = GetParam();
    std::vector<int> decoded(endIndex - startIndex);
    DecodeRleVector(
        MakeRange(values),
        MakeRange(rleIndexes),
        startIndex,
        endIndex,
        [] (int value) { return value; },
        MakeMutableRange(decoded));
    EXPECT_EQ(expected, decoded);
}

INSTANTIATE_TEST_SUITE_P(
    TDecodeRleVectorTest,
    TDecodeRleVectorTest,
    ::testing::Values(
        std::make_tuple(  0,   0, std::vector<int>{}),
        std::make_tuple(  0,   1, std::vector<int>{1}),
        std::make_tuple(  0,   5, std::vector<int>{1, 2, 2, 2, 2}),
        std::make_tuple(  9,  13, std::vector<int>{2, 3, 4, 4}),
        std::make_tuple( 20,  25, std::vector<int>{5, 5, 5, 5, 5}),
        std::make_tuple( 25,  27, std::vector<int>{6, 6}),
        std::make_tuple( 50,  53, std::vector<int>{6, 6, 6})));

////////////////////////////////////////////////////////////////////////////////

TEST(TDecodeIntegerValueTest, CheckAll)
{
    EXPECT_EQ(100, DecodeIntegerValue<int>(100,  0, false));
    EXPECT_EQ(100, DecodeIntegerValue<int>( 40, 60, false));
    EXPECT_EQ(  9, DecodeIntegerValue<int>( 18,  0, true));
    EXPECT_EQ(-10, DecodeIntegerValue<int>( 19,  0, true));
    EXPECT_EQ(  9, DecodeIntegerValue<int>(  1, 17, true));
    EXPECT_EQ(-10, DecodeIntegerValue<int>(  2, 17, true));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TDecodeIntegerVectorTest, CheckAll)
{
    std::vector<ui64> values     = {100, 40, 18,  19,   1,  2};
    std::vector<int> expected    = { 75, 45, 34, -35, -26, 26};
    std::vector<int> result(values.size());
    DecodeIntegerVector(
        MakeRange(values),
        ESimpleLogicalValueType::Int32,
        50,
        true,
        TMutableRef(result.data(), result.size() * sizeof(int)));
    EXPECT_EQ(expected, result);
}

////////////////////////////////////////////////////////////////////////////////

class TDecodeStringOffsetTest
    : public ::testing::Test
{
protected:
    std::vector<ui32> Offsets = {1, 2, 3, 4, 5};
    static constexpr ui32 AvgLength = 10;
    std::vector<ui32> Expected = {0, 9, 21, 28, 42, 47};
};

TEST_F(TDecodeStringOffsetTest, Single)
{
    std::vector<ui32> result;
    for (int index = 0; index <= Offsets.size(); ++index) {
        result.push_back(DecodeStringOffset(MakeRange(Offsets), AvgLength, index));
    }
    EXPECT_EQ(Expected, result); 
}

TEST_F(TDecodeStringOffsetTest, Multiple)
{
    for (int i = 0; i <= Offsets.size(); ++i) {
        for (int j = i; j <= Offsets.size(); ++j) {
            std::vector<ui32> result(j - i + 1);
            DecodeStringOffsets(MakeRange(Offsets), AvgLength, i, j, MakeMutableRange(result));
            std::vector<ui32> expected;
            for (int k = i; k <= j; ++k) {
                expected.push_back(Expected[k] - DecodeStringOffset(MakeRange(Offsets), AvgLength, i));
            }
            EXPECT_EQ(expected, result); 
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

class TBuildValidityBitmapFromRleDictionaryIndexesWithZeroNullTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<int, int>>
{
protected:
    virtual void SetUp()
    {
        Expected.resize(800);
        std::fill(Expected.begin() +   3, Expected.begin() +   5, true);
        std::fill(Expected.begin() +  20, Expected.begin() + 100, true);
        std::fill(Expected.begin() + 200, Expected.begin() + 800, true);
    }


    std::vector<ui32> DictionaryIndexes = {0, 1, 0,  1,   0,   1};
    std::vector<ui64> RleIndexes        = {0, 3, 5, 20, 100, 200};
    std::vector<bool> Expected;
};

TEST_P(TBuildValidityBitmapFromRleDictionaryIndexesWithZeroNullTest, CheckAll)
{
    auto [startIndex, endIndex] = GetParam();
    std::vector<ui8> bitmap(GetBitmapByteSize(Expected.size()));
    BuildValidityBitmapFromRleDictionaryIndexesWithZeroNull(
        MakeRange(DictionaryIndexes),
        MakeRange(RleIndexes),
        startIndex,
        endIndex,
        TMutableRef(bitmap.data(), bitmap.size()));
    for (int i = startIndex; i < endIndex; ++i) {
        EXPECT_EQ(Expected[i], GetBit(TRef(bitmap.data(), bitmap.size()), i - startIndex))
            << "i = " << i;
    }
}

INSTANTIATE_TEST_SUITE_P(
    TBuildValidityBitmapFromRleDictionaryIndexesWithZeroNullTest,
    TBuildValidityBitmapFromRleDictionaryIndexesWithZeroNullTest,
    ::testing::Values(
        std::make_tuple(  0,   0),
        std::make_tuple(  0, 800),
        std::make_tuple(256, 512),
        std::make_tuple( 10,  20),
        std::make_tuple( 10,  20),
        std::make_tuple( 20, 100),
        std::make_tuple( 90, 110)));

////////////////////////////////////////////////////////////////////////////////

class TBuildDictionaryIndexesFromRleDictionaryIndexesWithZeroNullTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<int, int>>
{
protected:
    std::vector<ui32> DictionaryIndexes = {0, 1, 0,  2,  3};
    std::vector<ui64> RleIndexes        = {0, 3, 5, 10, 12};
    
    static constexpr ui32 Z = static_cast<ui32>(-1); 
    std::vector<ui32> Expected = {Z, Z, Z, 0, 0, Z, Z, Z, Z, Z, 1, 1, 2, 2, 2};
};

TEST_P(TBuildDictionaryIndexesFromRleDictionaryIndexesWithZeroNullTest, CheckAll)
{
    auto [startIndex, endIndex] = GetParam();
    std::vector<ui32> result(endIndex - startIndex);
    BuildDictionaryIndexesFromRleDictionaryIndexesWithZeroNull(
        MakeRange(DictionaryIndexes),
        MakeRange(RleIndexes),
        startIndex,
        endIndex,
        MakeMutableRange(result));
    for (int i = startIndex; i < endIndex; ++i) {
        EXPECT_EQ(Expected[i], result[i - startIndex])
            << "i = " << i;
    }
}

INSTANTIATE_TEST_SUITE_P(
    TBuildDictionaryIndexesFromRleDictionaryIndexesWithZeroNullTest,
    TBuildDictionaryIndexesFromRleDictionaryIndexesWithZeroNullTest,
    ::testing::Values(
        std::make_tuple(  0,   0),
        std::make_tuple(  0,  15),
        std::make_tuple(  3,   5),
        std::make_tuple(  1,  10),
        std::make_tuple( 13,  15)));

////////////////////////////////////////////////////////////////////////////////

class TBuildIotaDictionaryIndexesFromRleIndexesTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<int, int, std::vector<ui32>>>
{
protected:
    std::vector<ui64> RleIndexes = {0, 3, 5, 10, 12};
};

TEST_P(TBuildIotaDictionaryIndexesFromRleIndexesTest, CheckAll)
{
    auto [startIndex, endIndex, expected] = GetParam();
    std::vector<ui32> result(endIndex - startIndex);
    BuildIotaDictionaryIndexesFromRleIndexes(
        MakeRange(RleIndexes),
        startIndex,
        endIndex,
        MakeMutableRange(result));
    EXPECT_EQ(expected, result);
}

INSTANTIATE_TEST_SUITE_P(
    TBuildIotaDictionaryIndexesFromRleIndexesTest,
    TBuildIotaDictionaryIndexesFromRleIndexesTest,
    ::testing::Values(
        std::make_tuple(  0,   0, std::vector<ui32>{}),
        std::make_tuple(  0,  15, std::vector<ui32>{0, 0, 0, 1, 1, 2, 2, 2, 2, 2, 3, 3, 4, 4, 4}),
        std::make_tuple(  3,   5, std::vector<ui32>{0, 0}),
        std::make_tuple(  1,  10, std::vector<ui32>{0, 0, 1, 1, 2, 2, 2, 2, 2}),
        std::make_tuple( 13,  15, std::vector<ui32>{0, 0})));

////////////////////////////////////////////////////////////////////////////////

class TCountNullsInDictionaryIndexesWithZeroNullTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<std::vector<ui32>, int>>
{ };

TEST_P(TCountNullsInDictionaryIndexesWithZeroNullTest, CheckAll)
{
    const auto& [indexes, expected] = GetParam();
    EXPECT_EQ(expected, CountNullsInDictionaryIndexesWithZeroNull(MakeRange(indexes)));
}

INSTANTIATE_TEST_SUITE_P(
    TCountNullsInDictionaryIndexesWithZeroNullTest,
    TCountNullsInDictionaryIndexesWithZeroNullTest,
    ::testing::Values(
        std::make_tuple(std::vector<ui32>{}, 0),
        std::make_tuple(std::vector<ui32>{0, 0, 0}, 3),
        std::make_tuple(std::vector<ui32>{1, 2, 3}, 0),
        std::make_tuple(std::vector<ui32>{1, 0, 3}, 1)));

////////////////////////////////////////////////////////////////////////////////

class TCountOnesInRleBitmapTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<int, int, int>>
{
protected:
    std::vector<ui64> RleIndexes = {0, 3, 5, 20, 50};
    ui8 Bitmap = 0b10101;
};

TEST_P(TCountOnesInRleBitmapTest, CheckAll)
{
    auto [startIndex, endIndex, expected] = GetParam();
    EXPECT_EQ(expected, CountOnesInRleBitmap(TRef(&Bitmap, 1), MakeRange(RleIndexes), startIndex, endIndex));
}

INSTANTIATE_TEST_SUITE_P(
    TCountOnesInRleBitmapTest,
    TCountOnesInRleBitmapTest,
    ::testing::Values(
        std::make_tuple(  0,   0,   0),
        std::make_tuple( 50,  60,  10),
        std::make_tuple( 40,  60,  10),
        std::make_tuple( 60, 100,  40),
        std::make_tuple(  3,   5,   0),
        std::make_tuple(  2,   6,   2)));

////////////////////////////////////////////////////////////////////////////////

class TBuildValidityBitmapFromRleNullBitmapTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<int, int>>
{
protected:
    virtual void SetUp()
    {
        Expected.resize(800);
        std::fill(Expected.begin() +   3, Expected.begin() +   5, true);
        std::fill(Expected.begin() +  20, Expected.begin() + 100, true);
        std::fill(Expected.begin() + 200, Expected.begin() + 800, true);
    }


    std::vector<ui64> RleIndexes = {0, 3, 5, 20, 100, 200};
    ui8 Bitmap = 0b010101;
    std::vector<bool> Expected;
};

TEST_P(TBuildValidityBitmapFromRleNullBitmapTest, CheckAll)
{
    auto [startIndex, endIndex] = GetParam();
    std::vector<ui8> bitmap(GetBitmapByteSize(Expected.size()));
    BuildValidityBitmapFromRleNullBitmap(
        TRef(&Bitmap, 1),
        MakeRange(RleIndexes),
        startIndex,
        endIndex,
        TMutableRef(bitmap.data(), bitmap.size()));
    for (int i = startIndex; i < endIndex; ++i) {
        EXPECT_EQ(Expected[i], GetBit(TRef(bitmap.data(), bitmap.size()), i - startIndex))
            << "i = " << i;
    }
}

INSTANTIATE_TEST_SUITE_P(
    TBuildValidityBitmapFromRleNullBitmapTest,
    TBuildValidityBitmapFromRleNullBitmapTest,
    ::testing::Values(
        std::make_tuple(  0,   0),
        std::make_tuple(  0, 800),
        std::make_tuple(256, 512),
        std::make_tuple( 10,  20),
        std::make_tuple( 10,  20),
        std::make_tuple( 20, 100),
        std::make_tuple( 90, 110)));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
