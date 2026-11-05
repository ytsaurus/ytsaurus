#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/extensions/kafka/transactional_writer.h>

#include <util/string/hex.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

TMessageId MakeId(i64 value)
{
    return value == 0 ? TMessageId() : TMessageId("m" + std::to_string(value));
}

std::optional<TKafkaCommittedOffset> CommittedAt(TMessageId messageId)
{
    std::string metadata(KafkaProgressMarkerMetadataPrefix);
    metadata.resize(metadata.size() + 2 * messageId.Underlying().size());
    HexEncode(messageId.Underlying().data(), messageId.Underlying().size(), metadata.data() + KafkaProgressMarkerMetadataPrefix.size());
    return TKafkaCommittedOffset{.Offset = 0, .Metadata = std::move(metadata)};
}

TKafkaTransactionalRecovery MakeRecovery(i64 persisted, i64 distributed)
{
    return {
        .MaxPersistedMessageId = MakeId(persisted),
        .MaxDistributedMessageId = MakeId(distributed),
    };
}

////////////////////////////////////////////////////////////////////////////////

TEST(TResolveKafkaProgressMarkerTest, TrustsAMarkerWithinTheRecoveredRange)
{
    auto markerOrError = ResolveKafkaProgressMarker(CommittedAt(MakeId(2)), MakeRecovery(1, 3));
    ASSERT_TRUE(markerOrError.IsOK());
    EXPECT_EQ(markerOrError.Value(), MakeId(2));

    // Committed up to the persisted frontier and not past it.
    markerOrError = ResolveKafkaProgressMarker(CommittedAt(MakeId(1)), MakeRecovery(1, 3));
    ASSERT_TRUE(markerOrError.IsOK());
    EXPECT_EQ(markerOrError.Value(), MakeId(1));
}

TEST(TResolveKafkaProgressMarkerTest, StartsWithoutAMarkerWhenNothingMayBeCommitted)
{
    // A fresh sink, one switched from at-least-once, or an idle one whose offset expired.
    for (const auto& recovery : {MakeRecovery(0, 0), MakeRecovery(3, 3), MakeRecovery(3, 0)}) {
        auto markerOrError = ResolveKafkaProgressMarker(std::nullopt, recovery);
        ASSERT_TRUE(markerOrError.IsOK());
        EXPECT_FALSE(markerOrError.Value());
    }
}

TEST(TResolveKafkaProgressMarkerTest, RefusesToStartWithoutAMarkerWhenMessagesMayBeCommitted)
{
    // The first epoch's messages may be committed before any acknowledgement is persisted.
    for (const auto& recovery : {MakeRecovery(0, 3), MakeRecovery(10, 12)}) {
        auto markerOrError = ResolveKafkaProgressMarker(std::nullopt, recovery);
        ASSERT_FALSE(markerOrError.IsOK());
        EXPECT_THAT(ToString(markerOrError), ::testing::HasSubstr("cannot be trusted"));
        EXPECT_THAT(ToString(markerOrError), ::testing::HasSubstr("no committed offset"));
    }
}

TEST(TResolveKafkaProgressMarkerTest, DistrustsAResetOffset)
{
    // An offset reset leaves the metadata empty; other writers may leave anything.
    for (const auto& metadata : std::vector<std::string>{"", "m2", "ytflow/v1", "ytflow/v3/"}) {
        auto markerOrError = ResolveKafkaProgressMarker(
            TKafkaCommittedOffset{.Offset = 0, .Metadata = metadata},
            MakeRecovery(0, 3));
        ASSERT_FALSE(markerOrError.IsOK()) << metadata;
        EXPECT_THAT(ToString(markerOrError), ::testing::HasSubstr("not written by the sink"));
    }
    auto reset = std::optional(TKafkaCommittedOffset{.Offset = 0, .Metadata = ""});

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
            std::pair(CommittedAt(MakeId(5)), MakeRecovery(0, 3)),
            std::pair(CommittedAt(MakeId(1)), MakeRecovery(2, 4)),
         })
    {
        auto markerOrError = ResolveKafkaProgressMarker(committed, recovery);
        ASSERT_FALSE(markerOrError.IsOK());
        EXPECT_THAT(ToString(markerOrError), ::testing::HasSubstr("outside the MessageIds"));
    }
}

TEST(TResolveKafkaProgressMarkerTest, RejectsMalformedMarkersAndWrongCarrierOffsets)
{
    for (auto metadata : {"ytflow/v2/0", "ytflow/v2/gg"}) {
        auto markerOrError = ResolveKafkaProgressMarker(TKafkaCommittedOffset{.Offset = 0, .Metadata = metadata}, MakeRecovery(0, 3));
        ASSERT_FALSE(markerOrError.IsOK());
        EXPECT_THAT(ToString(markerOrError), ::testing::HasSubstr("Malformed"));
    }
    auto marker = CommittedAt(MakeId(2));
    marker->Offset = 2;
    auto markerOrError = ResolveKafkaProgressMarker(marker, MakeRecovery(0, 3));
    ASSERT_FALSE(markerOrError.IsOK());
    EXPECT_THAT(ToString(markerOrError), ::testing::HasSubstr("must be zero"));
}

TEST(TResolveKafkaProgressMarkerTest, PreservesBinaryIdsAndTheSupportedLimit)
{
    for (const auto& messageId : {TMessageId(), TMessageId(std::string("m\0\xff", 3)), TMessageId(std::string(2043, 'z'))}) {
        auto markerOrError = ResolveKafkaProgressMarker(CommittedAt(messageId), {.MaxDistributedMessageId = messageId});
        ASSERT_TRUE(markerOrError.IsOK()) << ToString(markerOrError);
        EXPECT_EQ(markerOrError.Value(), messageId);
    }
    auto messageId = TMessageId(std::string(2044, 'z'));
    auto markerOrError = ResolveKafkaProgressMarker(CommittedAt(messageId), {.MaxDistributedMessageId = messageId});
    ASSERT_FALSE(markerOrError.IsOK());
    EXPECT_THAT(ToString(markerOrError), ::testing::HasSubstr("supported metadata limit"));
}

TEST(TResolveKafkaProgressMarkerTest, MigratesOnlyDrainedLegacyState)
{
    for (const auto& marker : {std::optional<TKafkaCommittedOffset>(), CommittedAt(MakeId(2)), std::optional(TKafkaCommittedOffset{.Offset = 2, .Metadata = "ytflow/v1"})}) {
        auto recovery = TKafkaTransactionalRecovery{
            .MaxPersistedMessageId = MakeId(1),
            .MaxPersistedSeqNo = 1,
            .MaxDistributedSeqNo = 1,
        };
        auto markerOrError = ResolveKafkaProgressMarker(marker, recovery);
        ASSERT_TRUE(markerOrError.IsOK());
        EXPECT_FALSE(markerOrError.Value());

        recovery.MaxDistributedSeqNo = 2;
        markerOrError = ResolveKafkaProgressMarker(marker, recovery);
        ASSERT_FALSE(markerOrError.IsOK());
        EXPECT_THAT(ToString(markerOrError), ::testing::HasSubstr("Legacy Kafka sink state"));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
