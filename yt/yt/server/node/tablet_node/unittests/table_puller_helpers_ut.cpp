#include <yt/yt/client/chaos_client/replication_card.h>
#include <yt/yt/client/chaos_client/helpers.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/server/node/tablet_node/table_puller_helpers.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NTabletNode {
namespace {

using namespace NChaosClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

struct TChaosReplicaDescriptor
{
    std::string Cluster;
    std::string Path;
    ETableReplicaMode Mode;
    ETableReplicaContentType ContentType;

    TTimestamp ReplicationProgressTimestamp;
};

TChaosReplicaDescriptor GenerateDefaultReplica(
    const std::string& cluster,
    const std::string& path,
    ETableReplicaMode mode,
    ETableReplicaContentType contentType)
{
    return TChaosReplicaDescriptor{
        .Cluster = cluster,
        .Path = path,
        .Mode = mode,
        .ContentType = contentType,
        .ReplicationProgressTimestamp = MinTimestamp
    };
}

std::vector<TChaosReplicaDescriptor> GenerateDefaultReplicas()
{
    std::vector<TChaosReplicaDescriptor> replicas;
    replicas.reserve(6);
    replicas.push_back(GenerateDefaultReplica(
        "primary",
        "//tmp/pdp",
        ETableReplicaMode::Sync,
        ETableReplicaContentType::Data));

    replicas.push_back(GenerateDefaultReplica(
        "primary",
        "//tmp/pdq",
        ETableReplicaMode::Sync,
        ETableReplicaContentType::Queue));

    replicas.push_back(GenerateDefaultReplica(
        "remote_0",
        "//tmp/r0q",
        ETableReplicaMode::Sync,
        ETableReplicaContentType::Queue));

    replicas.push_back(GenerateDefaultReplica(
        "remote_0",
        "//tmp/r0d",
        ETableReplicaMode::Sync,
        ETableReplicaContentType::Data));

    replicas.push_back(GenerateDefaultReplica(
        "remote_1",
        "//tmp/r1q",
        ETableReplicaMode::Async,
        ETableReplicaContentType::Queue));

    replicas.push_back(GenerateDefaultReplica(
        "remote_1",
        "//tmp/r1d",
        ETableReplicaMode::Async,
        ETableReplicaContentType::Data));

    return replicas;
}

TReplicationCardId GenerateReplicationCardId()
{
    return MakeReplicationCardId(TGuid::Create());
}

TReplicationCardPtr CreateReplicationCard(std::span<TChaosReplicaDescriptor> replicas, TReplicationCardId replicationCardId)
{
    auto replicationCard = New<TReplicationCard>();
    replicationCard->Era = InitialReplicationEra;

    TReplicaIdIndex replicaIndex = 0;
    for (const auto& replica : replicas) {
        auto newReplicaId = MakeReplicaId(replicationCardId, replicaIndex);
        ++replicaIndex;

        auto& replicaInfo = EmplaceOrCrash(replicationCard->Replicas, newReplicaId, TReplicaInfo())->second;
        replicaInfo.ClusterName = replica.Cluster;
        replicaInfo.ReplicaPath = replica.Path;
        replicaInfo.Mode = replica.Mode;
        replicaInfo.ContentType = replica.ContentType;
        replicaInfo.State = ETableReplicaState::Enabled;

        replicaInfo.ReplicationProgress = TReplicationProgress{
            .Segments = {{EmptyKey(), replica.ReplicationProgressTimestamp}},
            .UpperKey = MaxKey()
        };

        replicaInfo.History.push_back(TReplicaHistoryItem{
            .Era = InitialReplicationEra,
            .Timestamp = replica.ReplicationProgressTimestamp,
            .Mode = replica.Mode,
            .State = ETableReplicaState::Enabled,
        });
    }

    return replicationCard;
}

////////////////////////////////////////////////////////////////////////////////

TEST(TQueueReplicaSelectorTest, TestPreferLocal)
{
    TLogger logger;
    TBannedReplicaTracker bannedReplicaTracker(logger, std::nullopt);
    TQueueReplicaSelector queueReplicaSelector(logger, bannedReplicaTracker);

    auto replicationCardId = GenerateReplicationCardId();
    auto replicas = GenerateDefaultReplicas();
    auto replicationCard = CreateReplicationCard(replicas, replicationCardId);

    auto now = TInstant::Now();
    auto nowTs = InstantToTimestamp(now).second;
    TReplicaId asyncQueueReplicaId;
    TReplicaId asyncDataReplicaId;
    for (auto& [replicaId, replicaInfo] : replicationCard->Replicas) {
        if (replicaInfo.Mode == ETableReplicaMode::Async) {
            if (replicaInfo.ContentType == ETableReplicaContentType::Queue) {
                asyncQueueReplicaId = replicaId;
            } else {
                asyncDataReplicaId = replicaId;
                continue;
            }
        }

        replicaInfo.ReplicationProgress = AdvanceReplicationProgress(replicaInfo.ReplicationProgress, nowTs);
    }

    auto result = queueReplicaSelector.PickQueueReplica(
        asyncDataReplicaId,
        replicationCard,
        replicationCard->GetReplicaOrThrow(asyncDataReplicaId, replicationCardId)->ReplicationProgress,
        now);

    ASSERT_TRUE(result.IsOK());
    const auto& value = result.Value();
    EXPECT_EQ(asyncQueueReplicaId, std::get<0>(value));
    EXPECT_EQ(NullTimestamp, std::get<2>(value));
}

TEST(TQueueReplicaSelectorTest, TestBanReplicas)
{
    TLogger logger;
    TBannedReplicaTracker bannedReplicaTracker(logger, 1);
    TQueueReplicaSelector queueReplicaSelector(logger, bannedReplicaTracker);

    auto replicationCardId = GenerateReplicationCardId();
    auto replicas = GenerateDefaultReplicas();
    auto replicationCard = CreateReplicationCard(replicas, replicationCardId);

    auto now = TInstant::Now();
    auto nowTs = InstantToTimestamp(now).second;
    TReplicaId asyncQueueReplicaId;
    TReplicaId asyncDataReplicaId;
    for (auto& [replicaId, replicaInfo] : replicationCard->Replicas) {
        if (replicaInfo.Mode == ETableReplicaMode::Async) {
            if (replicaInfo.ContentType == ETableReplicaContentType::Queue) {
                asyncQueueReplicaId = replicaId;
            } else {
                asyncDataReplicaId = replicaId;
                continue;
            }
        }

        replicaInfo.ReplicationProgress = AdvanceReplicationProgress(replicaInfo.ReplicationProgress, nowTs);
    }

    bannedReplicaTracker.BanReplica(asyncQueueReplicaId, TError());

    auto result = queueReplicaSelector.PickQueueReplica(
        asyncDataReplicaId,
        replicationCard,
        replicationCard->GetReplicaOrThrow(asyncDataReplicaId, replicationCardId)->ReplicationProgress,
        now);

    ASSERT_TRUE(result.IsOK());
    auto selectedReplicaId = std::get<0>(result.Value());
    EXPECT_NE(asyncQueueReplicaId, selectedReplicaId);
    EXPECT_EQ(NullTimestamp, std::get<2>(result.Value()));

    // Check stickiness.
    for (int i = 0; i < 10; ++i) {
        auto repeatedResult = queueReplicaSelector.PickQueueReplica(
            asyncDataReplicaId,
            replicationCard,
            replicationCard->GetReplicaOrThrow(asyncDataReplicaId, replicationCardId)->ReplicationProgress,
            now);

        ASSERT_TRUE(result.IsOK());
        EXPECT_EQ(selectedReplicaId, std::get<0>(result.Value()));
        EXPECT_EQ(NullTimestamp, std::get<2>(result.Value()));
    }

    // Ban selected queue. Only one remains.
    bannedReplicaTracker.BanReplica(selectedReplicaId, TError());
    auto lastReplica = queueReplicaSelector.PickQueueReplica(
        asyncDataReplicaId,
            replicationCard,
            replicationCard->GetReplicaOrThrow(asyncDataReplicaId, replicationCardId)->ReplicationProgress,
            now);

    ASSERT_TRUE(result.IsOK());
    auto lastQueueReplicaId = std::get<0>(lastReplica.Value());
    EXPECT_NE(selectedReplicaId, lastQueueReplicaId);
    EXPECT_EQ(NullTimestamp, std::get<2>(lastReplica.Value()));

    // Ban the last one.
    bannedReplicaTracker.BanReplica(lastQueueReplicaId, TError());
    auto noneReplica = queueReplicaSelector.PickQueueReplica(
        asyncDataReplicaId,
            replicationCard,
            replicationCard->GetReplicaOrThrow(asyncDataReplicaId, replicationCardId)->ReplicationProgress,
            now);

    ASSERT_FALSE(noneReplica.IsOK());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTabletNode

