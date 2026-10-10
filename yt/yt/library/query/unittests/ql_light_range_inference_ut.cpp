#include <yt/yt/library/query/engine_api/new_range_inferrer.h>

#include <yt/yt/library/query/unittests/evaluate/ql_helpers.h>

namespace NYT::NQueryClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

TSharedRange<TRowRange> InferLightRanges(
    const char* predicate,
    const TTableSchemaPtr& schema)
{
    return CreateNewRangeInferrer(
        ParseAndPrepareExpression(predicate, *schema),
        schema,
        schema->GetKeyColumns(),
        /*evaluatorCache*/ nullptr,
        GetBuiltinConstraintExtractors(),
        TQueryOptions{.RangeExpansionLimit = 1000},
        GetDefaultMemoryChunkProvider(),
        /*forceLightRangeInference*/ true);
}

bool CoversKey(
    const TSharedRange<TRowRange>& ranges,
    const TComparator& comparator,
    TUnversionedRow row)
{
    auto key = TKey::FromRow(row);
    for (const auto& [lower, upper] : ranges) {
        if (comparator.TestKey(key, KeyBoundFromLegacyRow(lower, /*isUpper*/ false, comparator.GetLength())) &&
            comparator.TestKey(key, KeyBoundFromLegacyRow(upper, /*isUpper*/ true, comparator.GetLength())))
        {
            return true;
        }
    }
    return false;
}

TEST(TLightRangeInferrerTest, DescendingBounds)
{
    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("k", EValueType::Int64).SetSortOrder(ESortOrder::Descending),
    });
    auto ranges = InferLightRanges("k >= 1 AND k < 4", schema);
    ASSERT_EQ(1u, ranges.Size());
    auto comparator = schema->ToComparator();
    for (int value = 0; value <= 5; ++value) {
        auto row = YsonToKey(ToString(value));
        EXPECT_EQ(value >= 1 && value < 4, CoversKey(ranges, comparator, row));
    }
}

TEST(TLightRangeInferrerTest, MixedSortOrders)
{
    const std::vector<std::pair<const char*, std::function<bool(int, int)>>> predicates = {
        {"a = 2 AND b >= 2 AND b < 4", [] (int a, int b) { return a == 2 && b >= 2 && b < 4; }},
        {"a IN (1, 3) AND b IN (0, 4)", [] (int a, int b) { return (a == 1 || a == 3) && (b == 0 || b == 4); }},
        {"a > 1 OR (a = 1 AND b >= 2)", [] (int a, int b) { return a > 1 || (a == 1 && b >= 2); }},
        {"a = 1 AND (b < 3 OR b > 1)", [] (int a, int /*b*/) { return a == 1; }},
        {"a >= 1 AND a < 3", [] (int a, int /*b*/) { return a >= 1 && a < 3; }},
        {"a = 2 AND b != 3", [] (int a, int b) { return a == 2 && b != 3; }},
    };
    for (int directions = 1; directions < 4; ++directions) {
        auto schema = New<TTableSchema>(std::vector{
            TColumnSchema("a", EValueType::Int64).SetSortOrder(directions & 1 ? ESortOrder::Descending : ESortOrder::Ascending),
            TColumnSchema("b", EValueType::Int64).SetSortOrder(directions & 2 ? ESortOrder::Descending : ESortOrder::Ascending),
        });
        auto comparator = schema->ToComparator();
        for (const auto& [predicate, matches] : predicates) {
            SCOPED_TRACE(Format("Directions: %v, predicate: %v", directions, predicate));
            auto ranges = InferLightRanges(predicate, schema);
            ASSERT_FALSE(ranges.Empty());
            for (int index = 0; index < std::ssize(ranges); ++index) {
                auto lower = KeyBoundFromLegacyRow(ranges[index].first, /*isUpper*/ false, comparator.GetLength());
                auto upper = KeyBoundFromLegacyRow(ranges[index].second, /*isUpper*/ true, comparator.GetLength());
                EXPECT_FALSE(comparator.IsRangeEmpty(lower, upper));
                if (index > 0) {
                    auto previousUpper = KeyBoundFromLegacyRow(ranges[index - 1].second, /*isUpper*/ true, comparator.GetLength());
                    EXPECT_GT(comparator.CompareKeyBounds(lower, previousUpper), 0);
                }
            }
            for (int a = 0; a <= 4; ++a) {
                for (int b = 0; b <= 4; ++b) {
                    auto row = YsonToKey(Format("%v;%v", a, b));
                    EXPECT_EQ(matches(a, b), CoversKey(ranges, comparator, row)) << "Key: " << a << ", " << b;
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient
