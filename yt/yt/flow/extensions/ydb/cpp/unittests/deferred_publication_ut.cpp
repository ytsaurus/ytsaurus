#include <yt/yt/flow/extensions/ydb/cpp/deferred_publication.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow {
namespace {

using namespace NDetail;

////////////////////////////////////////////////////////////////////////////////

const std::string PublicWriterIdentityType = "ytflow-ydb-two-phase-commit";
const std::string OtherWriterIdentityType = "ytflow-other-two-phase-commit";

std::string MakeIdentityText(
    const std::string& type,
    int version,
    const std::string& sinkId,
    const TPartitionId& partitionId)
{
    return Format(R"({"generation"=42u;"partition_id"="%v";"preparation_attempt"=17u;"sink_id"="%v";"type"="%v";"version"=%v;})",
        partitionId,
        sinkId,
        type,
        version);
}

TEST(TYdbDeferredPublicationIdentityTest, PublicDiscriminatorRoundtrips)
{
    const auto sinkId = TSinkId("sink");
    const auto partitionId = TPartitionId(TGuid::Create());
    const auto identity = MakeDeferredPublicationWriterIdentity(
        PublicWriterIdentityType,
        TUniqueSeqNo(42),
        17,
        sinkId,
        partitionId);
    EXPECT_EQ(identity, MakeIdentityText(PublicWriterIdentityType, 2, "sink", partitionId));

    const auto parsed = ParseDeferredPublicationWriterIdentity(PublicWriterIdentityType, identity, sinkId, partitionId);
    EXPECT_EQ(parsed.Generation, TUniqueSeqNo(42));
    EXPECT_EQ(parsed.PreparationAttempt, 17u);
}

TEST(TYdbDeferredPublicationIdentityTest, RejectsForeignDiscriminator)
{
    const auto sinkId = TSinkId("sink");
    const auto partitionId = TPartitionId(TGuid::Create());
    const auto identity = MakeIdentityText(OtherWriterIdentityType, 2, "sink", partitionId);
    EXPECT_THROW_WITH_SUBSTRING(
        ParseDeferredPublicationWriterIdentity(PublicWriterIdentityType, identity, sinkId, partitionId),
        "Foreign deferred publication writer identity");
    EXPECT_NO_THROW(ParseDeferredPublicationWriterIdentity(OtherWriterIdentityType, identity, sinkId, partitionId));
}

TEST(TYdbDeferredPublicationIdentityTest, RejectsWrongVersion)
{
    const auto partitionId = TPartitionId(TGuid::Create());
    EXPECT_THROW_WITH_SUBSTRING(
        ParseDeferredPublicationWriterIdentity(
            PublicWriterIdentityType,
            MakeIdentityText(PublicWriterIdentityType, 1, "sink", partitionId),
            TSinkId("sink"),
            partitionId),
        "Foreign deferred publication writer identity");
}

TEST(TYdbDeferredPublicationIdentityTest, RejectsWrongSink)
{
    const auto partitionId = TPartitionId(TGuid::Create());
    EXPECT_THROW_WITH_SUBSTRING(
        ParseDeferredPublicationWriterIdentity(
            PublicWriterIdentityType,
            MakeIdentityText(PublicWriterIdentityType, 2, "other-sink", partitionId),
            TSinkId("sink"),
            partitionId),
        "Foreign deferred publication writer identity");
}

TEST(TYdbDeferredPublicationIdentityTest, RejectsWrongPartition)
{
    EXPECT_THROW_WITH_SUBSTRING(
        ParseDeferredPublicationWriterIdentity(
            PublicWriterIdentityType,
            MakeIdentityText(PublicWriterIdentityType, 2, "sink", TPartitionId(TGuid::Create())),
            TSinkId("sink"),
            TPartitionId(TGuid::Create())),
        "Foreign deferred publication writer identity");
}

TEST(TYdbDeferredPublicationIdentityTest, RejectsMalformedIdentity)
{
    EXPECT_THROW_WITH_SUBSTRING(
        ParseDeferredPublicationWriterIdentity(
            PublicWriterIdentityType,
            "ytflow-attempt/v1/17",
            TSinkId("sink"),
            TPartitionId(TGuid::Create())),
        "Malformed deferred publication writer identity");
}

TEST(TYdbDeferredPublicationIdentityTest, OversizedIdentityIsARegularError)
{
    EXPECT_THROW_WITH_SUBSTRING(
        MakeDeferredPublicationWriterIdentity(
            PublicWriterIdentityType,
            TUniqueSeqNo(42),
            17,
            TSinkId(std::string(3000, 'a')),
            TPartitionId(TGuid::Create())),
        "exceeds the SDK size limit");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
