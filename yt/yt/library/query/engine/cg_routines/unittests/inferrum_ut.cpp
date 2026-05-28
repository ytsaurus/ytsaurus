#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/library/query/engine/cg_routines/inferrum.h>

#include <yt/yt/library/query/engine_api/expression_context.h>

#include <yt/yt/client/table_client/unversioned_value.h>
#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/yson/pull_parser.h>
#include <yt/yt/core/yson/writer.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

#include <util/stream/mem.h>

namespace NYT::NQueryClient::NRoutines {

////////////////////////////////////////////////////////////////////////////////

namespace {

using namespace NTableClient;
using namespace NYson;
using namespace NYTree;

using TReplica = TInferrumKVCacheReplica::TUnderlying;

TUnversionedValue MakeDelta(
    const std::vector<TReplica>& added,
    const std::vector<TReplica>& removed,
    std::string* storage)
{
    *storage = BuildYsonStringFluently(EYsonFormat::Binary)
        .BeginList()
            .Item().Value(added)
            .Item().Value(removed)
        .EndList()
        .ToString();
    return MakeUnversionedAnyValue(*storage);
}

std::vector<TReplica> ParseList(TStringBuf yson)
{
    std::vector<TReplica> result;
    TMemoryInput input(yson);
    TYsonPullParser parser(&input, EYsonType::ListFragment);
    TYsonPullParserCursor cursor(&parser);
    YT_VERIFY(cursor.TryConsumeFragmentStart());
    cursor.ParseList([&] (TYsonPullParserCursor* cursor) {
        auto item = cursor->GetCurrent();
        YT_VERIFY(item.GetType() == EYsonItemType::Uint64Value);
        result.push_back(item.UncheckedAsUint64());
        cursor->Next();
    });
    return result;
}

std::pair<std::vector<TReplica>, std::vector<TReplica>> ParseDelta(TStringBuf yson)
{
    std::pair<std::vector<TReplica>, std::vector<TReplica>> result;
    TMemoryInput input(yson);
    TYsonPullParser parser(&input, EYsonType::ListFragment);
    TYsonPullParserCursor cursor(&parser);
    YT_VERIFY(cursor.TryConsumeFragmentStart());
    YT_VERIFY(cursor.GetCurrent().GetType() == EYsonItemType::BeginList);
    cursor.Next();
    auto readInts = [] (TYsonPullParserCursor* cursor, std::vector<TReplica>* out) {
        cursor->ParseList([&] (TYsonPullParserCursor* cursor) {
            auto item = cursor->GetCurrent();
            YT_VERIFY(item.GetType() == EYsonItemType::Uint64Value);
            out->push_back(item.UncheckedAsUint64());
            cursor->Next();
        });
    };
    readInts(&cursor, &result.first);
    readInts(&cursor, &result.second);
    YT_VERIFY(cursor.GetCurrent().GetType() == EYsonItemType::EndList);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TEST(TInferrumKVCacheReplicaSetTest, MergeNullInitialState)
{
    TExpressionContext context;

    std::string storage;
    auto state2 = MakeDelta({1, 2, 3}, {}, &storage);

    auto state1 = MakeUnversionedNullValue();

    TUnversionedValue result;
    InferrumKVCacheReplicaSetMerge(&context, &result, &state1, &state2);

    EXPECT_EQ(result.Type, EValueType::Any);
    EXPECT_EQ(result.AsStringBuf(), state2.AsStringBuf());
}

TEST(TInferrumKVCacheReplicaSetTest, MergeAddsReplicas)
{
    TExpressionContext context;

    std::string s1, s2;
    auto state1 = MakeDelta({1, 2}, {}, &s1);
    auto state2 = MakeDelta({3, 4}, {}, &s2);

    TUnversionedValue result;
    InferrumKVCacheReplicaSetMerge(&context, &result, &state1, &state2);

    auto [added, removed] = ParseDelta(result.AsStringBuf());
    EXPECT_EQ(added, (std::vector<TReplica>{1, 2, 3, 4}));
    EXPECT_TRUE(removed.empty());
}

TEST(TInferrumKVCacheReplicaSetTest, MergeRemovesReplicas)
{
    TExpressionContext context;

    std::string s1, s2;
    auto state1 = MakeDelta({1, 2, 3, 4}, {}, &s1);
    auto state2 = MakeDelta({}, {2, 4}, &s2);

    TUnversionedValue result;
    InferrumKVCacheReplicaSetMerge(&context, &result, &state1, &state2);

    auto [added, removed] = ParseDelta(result.AsStringBuf());
    EXPECT_EQ(added, (std::vector<TReplica>{1, 3}));
    EXPECT_TRUE(removed.empty());
}

TEST(TInferrumKVCacheReplicaSetTest, MergeAddsAndRemoves)
{
    TExpressionContext context;

    std::string s1, s2;
    auto state1 = MakeDelta({1, 2, 3}, {}, &s1);
    auto state2 = MakeDelta({4, 5}, {1, 3}, &s2);

    TUnversionedValue result;
    InferrumKVCacheReplicaSetMerge(&context, &result, &state1, &state2);

    auto [added, removed] = ParseDelta(result.AsStringBuf());
    EXPECT_EQ(added, (std::vector<TReplica>{2, 4, 5}));
    EXPECT_TRUE(removed.empty());
}

TEST(TInferrumKVCacheReplicaSetTest, MergeDuplicatesArePreservedOnce)
{
    TExpressionContext context;

    std::string s1, s2;
    auto state1 = MakeDelta({1, 2, 3}, {}, &s1);
    auto state2 = MakeDelta({2, 3, 4}, {}, &s2);

    TUnversionedValue result;
    InferrumKVCacheReplicaSetMerge(&context, &result, &state1, &state2);

    auto [added, removed] = ParseDelta(result.AsStringBuf());
    EXPECT_EQ(added, (std::vector<TReplica>{1, 2, 3, 4}));
    EXPECT_TRUE(removed.empty());
}

TEST(TInferrumKVCacheReplicaSetTest, FinalizeExtractsAddedAsList)
{
    TExpressionContext context;

    std::string storage;
    auto state = MakeDelta({10, 20, 30}, {}, &storage);

    TUnversionedValue result;
    InferrumKVCacheReplicaSetFinalize(&context, &result, &state);

    EXPECT_EQ(result.Type, EValueType::Any);
    EXPECT_EQ(ParseList(result.AsStringBuf()), (std::vector<TReplica>{10, 20, 30}));
}

TEST(TInferrumKVCacheReplicaSetTest, FinalizeEmpty)
{
    TExpressionContext context;

    std::string storage;
    auto state = MakeDelta({}, {}, &storage);

    TUnversionedValue result;
    InferrumKVCacheReplicaSetFinalize(&context, &result, &state);

    EXPECT_EQ(result.Type, EValueType::Any);
    EXPECT_TRUE(ParseList(result.AsStringBuf()).empty());
}

TEST(TInferrumKVCacheReplicaSetTest, EndToEndChain)
{
    TExpressionContext context;

    std::string s1, s2, s3;
    auto delta1 = MakeDelta({1, 2}, {}, &s1);
    auto delta2 = MakeDelta({3, 4}, {1}, &s2);
    auto delta3 = MakeDelta({5}, {3}, &s3);

    auto accumulator = MakeUnversionedNullValue();

    TUnversionedValue tmp;
    InferrumKVCacheReplicaSetMerge(&context, &tmp, &accumulator, &delta1);
    accumulator = tmp;
    InferrumKVCacheReplicaSetMerge(&context, &tmp, &accumulator, &delta2);
    accumulator = tmp;
    InferrumKVCacheReplicaSetMerge(&context, &tmp, &accumulator, &delta3);
    accumulator = tmp;

    TUnversionedValue finalized;
    InferrumKVCacheReplicaSetFinalize(&context, &finalized, &accumulator);

    EXPECT_EQ(ParseList(finalized.AsStringBuf()), (std::vector<TReplica>{2, 4, 5}));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient
