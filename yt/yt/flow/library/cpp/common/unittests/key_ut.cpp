#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/payload.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/convert.h>

#include <vector>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TKeyInRangeTest, SimpleUint)
{
    auto range = TKeyRange(MakeKey<ui64>(23u), MakeKey<ui64>(35u));
    ASSERT_FALSE(range.Contains(MakeKey<ui64>(22u)));
    ASSERT_TRUE(range.Contains(MakeKey<ui64>(23u)));
    ASSERT_TRUE(range.Contains(MakeKey<ui64, i64, i64>(23u, 4324, 6565)));
    ASSERT_TRUE(range.Contains(MakeKey<ui64>(34u)));
    ASSERT_TRUE(range.Contains(MakeKey<ui64, ui64>(34u, 99999999u)));
    ASSERT_FALSE(range.Contains(MakeKey<ui64>(35u)));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TKeyRangeTest, SplitSmallFive)
{
    std::vector<std::pair<ui64, ui64>> ranges;
    for (const auto& range : SplitUintKeyRange(MakeUintKeyRange(0, 20), 5)) {
        auto [lower, upper] = ExtractUintsFromKeyRange(range);
        ASSERT_TRUE(lower);
        ASSERT_TRUE(upper);
        ranges.push_back(std::pair{*lower, *upper});
    }

    std::vector<std::pair<ui64, ui64>> expected = {
        {0, 4},
        {4, 8},
        {8, 12},
        {12, 16},
        {16, 20},
    };
    ASSERT_EQ(ranges, expected);
}

TEST(TKeyRangeTest, SplitOne)
{
    auto ranges = SplitUintKeyRange(UniversalKeyRange(), 1);
    ASSERT_EQ(ranges.size(), 1ull);
    auto [lower, upper] = ExtractUintsFromKeyRange(ranges[0]);
    ASSERT_EQ(lower, 0ull);
    ASSERT_EQ(upper, std::optional<ui64>());
    std::vector<TKeyRange> expected = {
        MakeUintKeyRange(0, {}),
    };
    ASSERT_EQ(ranges, expected);
}

TEST(TKeyRangeTest, SplitThree)
{
    auto ranges = SplitUintKeyRange(UniversalKeyRange(), 3);
    std::vector<TKeyRange> expected = {
        MakeUintKeyRange(0, 0x5555555555555556),
        MakeUintKeyRange(0x5555555555555556, 0xAAAAAAAAAAAAAAAB),
        MakeUintKeyRange(0xAAAAAAAAAAAAAAAB, {}),
    };
    ASSERT_EQ(ranges, expected);
}

TEST(TKeyRangeTest, SplitEight)
{
    auto ranges = SplitUintKeyRange(UniversalKeyRange(), 8);
    std::vector<TKeyRange> expected = {
        MakeUintKeyRange(0, 0x2000000000000000),
        MakeUintKeyRange(0x2000000000000000, 0x4000000000000000),
        MakeUintKeyRange(0x4000000000000000, 0x6000000000000000),
        MakeUintKeyRange(0x6000000000000000, 0x8000000000000000),
        MakeUintKeyRange(0x8000000000000000, 0xA000000000000000),
        MakeUintKeyRange(0xA000000000000000, 0xC000000000000000),
        MakeUintKeyRange(0xC000000000000000, 0xE000000000000000),
        MakeUintKeyRange(0xE000000000000000, {}),
    };
    ASSERT_EQ(ranges, expected);
    auto rangesUnion = UniteRanges(ranges);
    ASSERT_EQ(rangesUnion, std::vector{UniversalKeyRange()});
}

TEST(TKeyRangeTest, UnionEmpty)
{
    auto ranges = UniteRanges({});
    std::vector<TKeyRange> expected;
    ASSERT_EQ(ranges, expected);
}

TEST(TKeyRangeTest, UnionFull)
{
    std::vector<TKeyRange> ranges = {
        MakeUintKeyRange(0, 0x2000000000000000),
        MakeUintKeyRange(0x2000000000000000, 0x4000000000000000),
        MakeUintKeyRange(0x4000000000000000, 0x6000000000000000),
        MakeUintKeyRange(0x6000000000000000, 0x8000000000000000),
        MakeUintKeyRange(0x8000000000000000, 0xA000000000000000),
        MakeUintKeyRange(0xA000000000000000, 0xC000000000000000),
        MakeUintKeyRange(0xC000000000000000, 0xE000000000000000),
        MakeUintKeyRange(0xE000000000000000, {}),
    };
    auto got = UniteRanges(ranges);
    std::vector<TKeyRange> expected = {UniversalKeyRange()};
    ASSERT_EQ(got, expected);
}

TEST(TKeyRangeTest, UnionOverlap)
{
    std::vector<TKeyRange> ranges = {
        MakeUintKeyRange(0, 11),
        MakeUintKeyRange(5, 16),
    };
    auto got = UniteRanges(ranges);
    std::vector<TKeyRange> expected = {
        MakeUintKeyRange(0, 16),
    };
    ASSERT_EQ(got, expected);
}

TEST(TKeyRangeTest, UnionConcat)
{
    std::vector<TKeyRange> ranges = {
        MakeUintKeyRange(0, 11),
        MakeUintKeyRange(11, 16),
    };
    auto got = UniteRanges(ranges);
    std::vector<TKeyRange> expected = {
        MakeUintKeyRange(0, 16),
    };
    ASSERT_EQ(got, expected);
}

TEST(TKeyRangeTest, UnionGap)
{
    std::vector<TKeyRange> ranges = {
        MakeUintKeyRange(0, 11),
        MakeUintKeyRange(12, 16),
    };
    auto got = UniteRanges(ranges);
    const std::vector<TKeyRange>& expected = ranges;
    ASSERT_EQ(got, expected);
}

TEST(TKeyRangeTest, UnionNested)
{
    std::vector<TKeyRange> ranges = {
        MakeUintKeyRange(0, 16),
        MakeUintKeyRange(5, 11),
    };
    auto got = UniteRanges(ranges);
    std::vector<TKeyRange> expected = {
        MakeUintKeyRange(0, 16),
    };
    ASSERT_EQ(got, expected);
}

TEST(TKeyRangeTest, UnionNestedChain)
{
    std::vector<TKeyRange> ranges = {
        MakeUintKeyRange(0, 16),
        MakeUintKeyRange(5, 11),
        MakeUintKeyRange(20, 25),
    };
    auto got = UniteRanges(ranges);
    std::vector<TKeyRange> expected = {
        MakeUintKeyRange(0, 16),
        MakeUintKeyRange(20, 25),
    };
    ASSERT_EQ(got, expected);
}

TEST(TKeyRangeTest, UnionNestedEmptyRange)
{
    std::vector<TKeyRange> ranges = {
        MakeUintKeyRange(0, 16),
        MakeUintKeyRange(5, 5),
    };
    auto got = UniteRanges(ranges);
    std::vector<TKeyRange> expected = {
        MakeUintKeyRange(0, 16),
    };
    ASSERT_EQ(got, expected);
}

TEST(TKeyRangeTest, UnionDropsEmptyRanges)
{
    std::vector<TKeyRange> ranges = {
        MakeUintKeyRange(5, 5),
    };
    auto got = UniteRanges(ranges);
    std::vector<TKeyRange> expected;
    ASSERT_EQ(got, expected);
}

TEST(TKeyRangeTest, UnionPartial)
{
    std::vector<TKeyRange> ranges = {
        MakeUintKeyRange(0, 0x2000000000000000),
        MakeUintKeyRange(0x2000000000000000, 0x4000000000000000),
        MakeUintKeyRange(0x4000000000000001, 0x6000000000000000),
        MakeUintKeyRange(0x6000000000000000, 0x8000000000000000),
        MakeUintKeyRange(0x8000000000000000, 0x9FFFFFFFFFFFFFFF),
        MakeUintKeyRange(0xA000000000000000, 0xC000000000000000),
        MakeUintKeyRange(0xC000000000000000, 0xE000000000000000),
        MakeUintKeyRange(0xE000000000000000, {}),
    };
    auto got = UniteRanges(ranges);
    std::vector<TKeyRange> expected = {
        MakeUintKeyRange(0, 0x4000000000000000),
        MakeUintKeyRange(0x4000000000000001, 0x9FFFFFFFFFFFFFFF),
        MakeUintKeyRange(0xA000000000000000, {}),
    };
    ASSERT_EQ(got, expected);
}

TEST(TKeyRangeTest, DifferenceFull)
{
    std::vector<TKeyRange> bigger = {
        UniversalKeyRange()};
    std::vector<TKeyRange> smaller = {
        MakeUintKeyRange(0, 0x2000000000000000),
        MakeUintKeyRange(0x2000000000000001, 0x4000000000000000),
        MakeUintKeyRange(0x4000000030000000, 0x5FFFFFFFB0000000),
        MakeUintKeyRange(0x6000000000000000, 0x8000000000000000),
        MakeUintKeyRange(0x8000000000004000, 0xA000000000000000),
        MakeUintKeyRange(0xA000000000000000, 0xBFFFFFFFFFB00000),
        MakeUintKeyRange(0xC000000500000000, 0xC00F000000000001),
        MakeUintKeyRange(0xD00F000000000000, 0xDFFFFFFFFFFFFB00),
    };

    auto got = SubtractRanges(bigger, smaller);
    std::vector<TKeyRange> expected = {
        MakeUintKeyRange(0x2000000000000000, 0x2000000000000001),
        MakeUintKeyRange(0x4000000000000000, 0x4000000030000000),
        MakeUintKeyRange(0x5FFFFFFFB0000000, 0x6000000000000000),
        MakeUintKeyRange(0x8000000000000000, 0x8000000000004000),
        MakeUintKeyRange(0xBFFFFFFFFFB00000, 0xC000000500000000),
        MakeUintKeyRange(0xC00F000000000001, 0xD00F000000000000),
        MakeUintKeyRange(0xDFFFFFFFFFFFFB00, {}),
    };
    ASSERT_EQ(got, expected);
}

TEST(TKeyRangeTest, DifferenceSimple)
{
    std::vector<TKeyRange> bigger = {
        MakeUintKeyRange(2, 5),
        MakeUintKeyRange(6, 11),
        MakeUintKeyRange(15, 21),

    };
    std::vector<TKeyRange> smaller = {
        MakeUintKeyRange(0, 1),
        MakeUintKeyRange(3, 9),
        MakeUintKeyRange(10, 17),
        MakeUintKeyRange(17, 18),
        MakeUintKeyRange(19, 20),
    };

    auto got = SubtractRanges(bigger, smaller);
    std::vector<TKeyRange> expected = {
        MakeUintKeyRange(2, 3),
        MakeUintKeyRange(9, 10),
        MakeUintKeyRange(18, 19),
        MakeUintKeyRange(20, 21),
    };
    ASSERT_EQ(got, expected);
}

TEST(TKeyRangeTest, DifferenceEmpty)
{
    std::vector<TKeyRange> bigger = {
        MakeUintKeyRange(2, 5),
    };
    std::vector<TKeyRange> smaller = {
        MakeUintKeyRange(0, 0),
        MakeUintKeyRange(3, 9),
    };

    auto got = SubtractRanges(bigger, smaller);
    std::vector<TKeyRange> expected = {
        MakeUintKeyRange(2, 3),
    };
    ASSERT_EQ(got, expected);
}

TEST(TKeyRangeTest, DifferencePartial)
{
    std::vector<TKeyRange> bigger = {
        MakeUintKeyRange(0x0000000000000001, 0x1FFFFFFFFFFFFFFF),
        MakeUintKeyRange(0x2000000000000001, 0x3FFFFFFFFFFFFFFF),
        MakeUintKeyRange(0x4000000000000001, 0x5FFFFFFFFFFFFFFF),
        MakeUintKeyRange(0x6000000000000001, 0x7FFFFFFFFFFFFFFF),
        MakeUintKeyRange(0x8000000000000001, 0x9FFFFFFFFFFFFFFF),
        MakeUintKeyRange(0xA000000000000001, 0xBFFFFFFFFFFFFFFF),
        MakeUintKeyRange(0xC000000000000001, 0xDFFFFFFFFFFFFFFF),
        MakeUintKeyRange(0xE000000000000001, 0xFFFFFFFFFFFFFFFF),
    };
    std::vector<TKeyRange> smaller = {
        MakeUintKeyRange(0, 0x2000000000000000),
        MakeUintKeyRange(0x2000000000000001, 0x4000000000000000),
        MakeUintKeyRange(0x4000000030000000, 0x5FFFFFFFB0000000),
        MakeUintKeyRange(0x6000000000000002, 0x8000000000000000),
        MakeUintKeyRange(0x8000000000004000, 0xA000000000000000),
        MakeUintKeyRange(0xA000000000000000, 0xBFFFFFFFFFB00000),
        MakeUintKeyRange(0xC000000500000000, 0xC00F000000000001),
        MakeUintKeyRange(0xD00F000000000000, 0xDFFFFFFFFFFFFB00),
    };

    auto got = SubtractRanges(bigger, smaller);
    std::vector<TKeyRange> expected = {
        MakeUintKeyRange(0x4000000000000001, 0x4000000030000000),
        MakeUintKeyRange(0x5FFFFFFFB0000000, 0x5FFFFFFFFFFFFFFF),
        MakeUintKeyRange(0x6000000000000001, 0x6000000000000002),
        MakeUintKeyRange(0x8000000000000001, 0x8000000000004000),
        MakeUintKeyRange(0xBFFFFFFFFFB00000, 0xBFFFFFFFFFFFFFFF),
        MakeUintKeyRange(0xC000000000000001, 0xC000000500000000),
        MakeUintKeyRange(0xC00F000000000001, 0xD00F000000000000),
        MakeUintKeyRange(0xDFFFFFFFFFFFFB00, 0xDFFFFFFFFFFFFFFF),
        MakeUintKeyRange(0xE000000000000001, 0xFFFFFFFFFFFFFFFF),
    };
    ASSERT_EQ(got, expected);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TKeyTest, ConcatenateKeys)
{
    auto key = MakeKey(1, "abra", 3u);
    auto subKey = MakeKey("dc", 5u);
    auto fullKey = ConcatenateKeys(key, subKey);
    EXPECT_EQ(fullKey, MakeKey(1, "abra", 3u, "dc", 5u));
}

////////////////////////////////////////////////////////////////////////////////

NTableClient::TTableSchemaPtr ParseSchema(TStringBuf yson)
{
    return NYTree::ConvertTo<NTableClient::TTableSchemaPtr>(NYson::TYsonString(yson));
}

TEST(TValidateKeyTest, Ok)
{
    auto schema = ParseSchema(R"""(
        [
            {name = "Hash"; type = "uint64"; expression = "farm_hash(BankId)"; required = %true; sort_order = "ascending";};
            {name = "BankId"; type = "uint64"; required = %true; sort_order = "ascending";};
        ]
    )""");
    TPayloadBuilder builder(schema);
    builder.Set<ui64>(123u, "Hash");
    builder.Set<ui64>(42u, "BankId");
    EXPECT_NO_THROW(ValidateKey(TKey(builder.Finish().Underlying()), schema));
}

TEST(TValidateKeyTest, NullInRequiredColumn)
{
    auto schema = ParseSchema(R"""(
        [
            {name = "Hash"; type = "uint64"; expression = "farm_hash(BankId)"; required = %true; sort_order = "ascending";};
            {name = "BankId"; type = "uint64"; required = %true; sort_order = "ascending";};
        ]
    )""");
    // BankId is required but left null.
    TPayloadBuilder builder(schema);
    builder.Set<ui64>(123u, "Hash");
    EXPECT_THROW_WITH_SUBSTRING(
        ValidateKey(TKey(builder.Finish().Underlying()), schema),
        "cannot have");
}

TEST(TValidateKeyTest, NullInOptionalIsOk)
{
    auto schema = ParseSchema(R"""(
        [
            {name = "BankId"; type = "uint64"; sort_order = "ascending";};
        ]
    )""");
    TPayloadBuilder builder(schema);
    // BankId left null (optional).
    EXPECT_NO_THROW(ValidateKey(TKey(builder.Finish().Underlying()), schema));
}

TEST(TValidateKeyTest, ColumnCountMismatch)
{
    auto schema = ParseSchema(R"""(
        [
            {name = "BankId"; type = "uint64"; required = %true; sort_order = "ascending";};
        ]
    )""");
    auto biggerSchema = ParseSchema(R"""(
        [
            {name = "BankId"; type = "uint64"; required = %true; sort_order = "ascending";};
            {name = "Extra"; type = "uint64"; required = %true;};
        ]
    )""");
    TPayloadBuilder builder(biggerSchema);
    builder.Set<ui64>(1u, "BankId");
    builder.Set<ui64>(2u, "Extra");
    EXPECT_THROW_WITH_SUBSTRING(
        ValidateKey(TKey(builder.Finish().Underlying()), schema),
        "count mismatch");
}

////////////////////////////////////////////////////////////////////////////////

TEST(TKeyTest, SplitPartitionRangeIntoBuckets_HashOnlyQuartiles)
{
    auto buckets = SplitPartitionRangeIntoBuckets(UniversalKeyRange(), 4);
    std::vector<TKeyRange> expected = {
        MakeUintKeyRange(0, 1ull << 62),
        MakeUintKeyRange(1ull << 62, 2ull << 62),
        MakeUintKeyRange(2ull << 62, 3ull << 62),
        MakeUintKeyRange(3ull << 62, {}),
    };
    ASSERT_EQ(buckets, expected);
}

TEST(TKeyTest, SplitPartitionRangeIntoBuckets_SingleBucketReturnsRange)
{
    auto range = MakeUintKeyRange(10, 20);
    auto buckets = SplitPartitionRangeIntoBuckets(range, 1);
    ASSERT_EQ(buckets.size(), 1u);
    EXPECT_EQ(buckets[0], range);
}

TEST(TKeyTest, SplitPartitionRangeIntoBuckets_DegenerateFirstColumnReturnsSingleBucket)
{
    auto lower = MakeKey(ui64{42}, "tail-a");
    auto upper = MakeKey(ui64{42}, "tail-z");
    auto buckets = SplitPartitionRangeIntoBuckets({lower, upper}, 8);
    ASSERT_EQ(buckets.size(), 1u);
    EXPECT_EQ(buckets[0].Lower, lower);
    EXPECT_EQ(buckets[0].Upper, upper);
}

TEST(TKeyTest, SplitPartitionRangeIntoBuckets_PreservesMultiColumnTail)
{
    auto lower = MakeKey(ui64{42}, "tail-a");
    auto upper = MakeKey(ui64{142}, "tail-b");
    auto buckets = SplitPartitionRangeIntoBuckets({lower, upper}, 2);
    ASSERT_EQ(buckets.size(), 2u);
    EXPECT_EQ(buckets[0].Lower, lower);
    EXPECT_EQ(buckets[1].Upper, upper);
    // The shared inner boundary is single-column.
    EXPECT_EQ(buckets[0].Upper, MakeUintKey(92));
    EXPECT_EQ(buckets[1].Lower, MakeUintKey(92));
}

TEST(TKeyTest, SplitPartitionRangeIntoBuckets_FewerHashesThanBuckets)
{
    // Span is 3 hash values but 8 buckets are requested: cap at 3, no empty buckets.
    auto buckets = SplitPartitionRangeIntoBuckets(MakeUintKeyRange(10, 13), 8);
    std::vector<TKeyRange> expected = {
        MakeUintKeyRange(10, 11),
        MakeUintKeyRange(11, 12),
        MakeUintKeyRange(12, 13),
    };
    ASSERT_EQ(buckets, expected);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
