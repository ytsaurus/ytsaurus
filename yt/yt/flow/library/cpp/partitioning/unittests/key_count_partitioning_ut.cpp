#include <yt/yt/flow/library/cpp/partitioning/key_count_partitioning.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow::NPartitioning {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TKeyCountPartitioningTest, BalancesSevenKeysUnderLimitSixAsFourAndThree)
{
    auto state = New<TKeyCountPartitioningState>();
    for (int key = 1; key <= 7; ++key) {
        state->ObservedKeys[MakeKey(static_cast<ui64>(key))] = TInstant::Seconds(100);
    }
    auto keys = GetKeys(state->ObservedKeys);
    std::sort(keys.begin(), keys.end());
    EXPECT_EQ(BuildKeyCountRanges(keys, UniversalKeyRange(), 6),
        (std::vector<TKeyRange>{{MinKey(), MakeKey(5u)}, {MakeKey(5u), MaxKey()}}));
}

TEST(TKeyCountPartitioningTest, LocalSplitUsesHistoryAndPreservesRangeBoundaries)
{
    auto state = New<TKeyCountPartitioningState>();
    IngestObservedKeys(state, {{MakeKey("a", 1), TInstant::Seconds(100)}});
    IngestObservedKeys(state, {{MakeKey("a", 2), TInstant::Seconds(101)}, {MakeKey("b"), TInstant::Seconds(101)}});
    auto keys = GetKeys(state->ObservedKeys);
    std::sort(keys.begin(), keys.end());
    EXPECT_EQ(BuildKeyCountRanges(keys, {MakeKey("a", 1), MakeKey("b")}, 1),
        (std::vector<TKeyRange>{{MakeKey("a", 1), MakeKey("a", 2)}, {MakeKey("a", 2), MakeKey("b")}}));
}

TEST(TKeyCountPartitioningTest, ExpiryUsesActualEncounterAndRepeatedReportsDoNotRefreshIt)
{
    auto state = New<TKeyCountPartitioningState>();
    const THashMap<TKey, TInstant> old{{MakeKey(1u), TInstant::Seconds(100)}};
    IngestObservedKeys(state, old);
    IngestObservedKeys(state, {{MakeKey(2u), TInstant::Seconds(109)}});
    IngestObservedKeys(state, old);
    ExpireKeyCatalogue(state, TInstant::Seconds(100));
    EXPECT_EQ(state->ObservedKeys.size(), 1u);
    EXPECT_TRUE(state->ObservedKeys.contains(MakeKey(2u)));
    auto keys = GetKeys(state->ObservedKeys);
    std::sort(keys.begin(), keys.end());
    EXPECT_EQ(BuildKeyCountRanges(keys, UniversalKeyRange(), 1), std::vector{UniversalKeyRange()});
    EXPECT_EQ(BuildKeyCountRanges(keys, {MakeKey(3u), MaxKey()}, 1),
        (std::vector<TKeyRange>{{MakeKey(3u), MaxKey()}}));
    auto copy = NYTree::ConvertTo<TKeyCountPartitioningStatePtr>(NYson::ConvertToYsonString(state));
    EXPECT_EQ(copy->ObservedKeys, state->ObservedKeys);
}

TEST(TKeyCountPartitioningTest, ExpiredObservationsStayExpiredAfterStateRecovery)
{
    auto state = New<TKeyCountPartitioningState>();
    const auto oldTime = TInstant::Seconds(100);
    const auto newTime = TInstant::Seconds(101);
    IngestObservedKeys(state, {{MakeKey("expired"), oldTime}, {MakeKey("alive"), newTime}});
    ExpireKeyCatalogue(state, oldTime);
    state = NYTree::ConvertTo<TKeyCountPartitioningStatePtr>(NYson::ConvertToYsonString(state));

    IngestObservedKeys(state, {{MakeKey("expired"), oldTime}, {MakeKey("late"), oldTime}});
    EXPECT_EQ(state->ObservedKeys, (THashMap<TKey, TInstant>{{MakeKey("alive"), newTime}}));

    IngestObservedKeys(state, {{MakeKey("expired"), newTime}});
    EXPECT_EQ(state->ObservedKeys.size(), 2u);
    EXPECT_EQ(GetOrCrash(state->ObservedKeys, MakeKey("expired")), newTime);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NPartitioning
