#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/extensions/kafka/transactional_writer.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

std::optional<TKafkaCommittedOffset> CommittedAt(i64 seqNo)
{
    return TKafkaCommittedOffset{
        .Offset = seqNo,
        .Metadata = std::string(KafkaProgressMarkerMetadata),
    };
}

TKafkaTransactionalRecovery MakeRecovery(i64 maxPersistedSeqNo, i64 maxDistributedSeqNo)
{
    return {
        .MaxPersistedSeqNo = maxPersistedSeqNo,
        .MaxDistributedSeqNo = maxDistributedSeqNo,
    };
}

////////////////////////////////////////////////////////////////////////////////

TEST(TResolveKafkaProgressMarkerTest, TrustsAMarkerWithinTheRecoveredRange)
{
    auto markerOrError = ResolveKafkaProgressMarker(CommittedAt(2), MakeRecovery(1, 3));
    ASSERT_TRUE(markerOrError.IsOK());
    EXPECT_EQ(markerOrError.Value(), 2);

    // Committed up to the persisted frontier and not past it.
    markerOrError = ResolveKafkaProgressMarker(CommittedAt(1), MakeRecovery(1, 3));
    ASSERT_TRUE(markerOrError.IsOK());
    EXPECT_EQ(markerOrError.Value(), 1);
}

TEST(TResolveKafkaProgressMarkerTest, StartsWithoutAMarkerWhenNothingMayBeCommitted)
{
    // A fresh sink, one switched from at-least-once, or an idle one whose offset expired.
    for (auto recovery : {MakeRecovery(0, 0), MakeRecovery(3, 3), MakeRecovery(3, 0)}) {
        auto markerOrError = ResolveKafkaProgressMarker(std::nullopt, recovery);
        ASSERT_TRUE(markerOrError.IsOK());
        EXPECT_FALSE(markerOrError.Value());
    }
}

TEST(TResolveKafkaProgressMarkerTest, RefusesToStartWithoutAMarkerWhenMessagesMayBeCommitted)
{
    // The first epoch's messages may be committed before any acknowledgement is persisted.
    for (auto recovery : {MakeRecovery(0, 3), MakeRecovery(10, 12)}) {
        auto markerOrError = ResolveKafkaProgressMarker(std::nullopt, recovery);
        ASSERT_FALSE(markerOrError.IsOK());
        EXPECT_THAT(ToString(markerOrError), ::testing::HasSubstr("cannot be trusted"));
        EXPECT_THAT(ToString(markerOrError), ::testing::HasSubstr("no committed offset"));
    }
}

TEST(TResolveKafkaProgressMarkerTest, DistrustsAResetOffset)
{
    // An offset reset leaves the metadata empty; other writers may leave anything.
    for (const auto& metadata : std::vector<std::string>{"", "m2", "ytflow/v1/0123456789abcdef", "ytflow/v2"}) {
        auto markerOrError = ResolveKafkaProgressMarker(
            TKafkaCommittedOffset{.Offset = 2, .Metadata = metadata},
            MakeRecovery(0, 3));
        ASSERT_FALSE(markerOrError.IsOK()) << metadata;
        EXPECT_THAT(ToString(markerOrError), ::testing::HasSubstr("not written by the sink"));
    }
    auto reset = std::optional(TKafkaCommittedOffset{.Offset = 2, .Metadata = ""});

    // Ignored when nothing is ambiguous; the next commit overwrites it.
    auto markerOrError = ResolveKafkaProgressMarker(reset, MakeRecovery(3, 3));
    ASSERT_TRUE(markerOrError.IsOK());
    EXPECT_FALSE(markerOrError.Value());
}

TEST(TResolveKafkaProgressMarkerTest, DistrustsAMarkerOutsideTheRecoveredRange)
{
    // Past the bound it would skip messages never committed; behind the frontier it is older than what
    // the acknowledgements prove.
    for (const auto& [committed, recovery] : std::vector{
            std::pair(CommittedAt(5), MakeRecovery(0, 3)),
            std::pair(CommittedAt(1), MakeRecovery(2, 4)),
         })
    {
        auto markerOrError = ResolveKafkaProgressMarker(committed, recovery);
        ASSERT_FALSE(markerOrError.IsOK());
        EXPECT_THAT(ToString(markerOrError), ::testing::HasSubstr("outside the seqNos"));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
