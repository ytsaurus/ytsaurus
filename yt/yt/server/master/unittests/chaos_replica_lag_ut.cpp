#include "helpers.h"

#include <yt/yt/server/master/table_server/helpers.h>

#include <yt/yt/client/chaos_client/helpers.h>
#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NTableServer {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NChaosClient;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TReplicationCardId GenerateReplicationCardId()
{
    return MakeReplicationCardId(TGuid::Create());
}

TReplicaId AddReplicationCardReplica(
    TReplicationCardId replicationCardId,
    TReplicationCardPtr& replicationCard,
    ETableReplicaMode mode,
    ETableReplicaContentType contentType,
    std::string_view path,
    std::string_view progress)
{
    auto newReplicaId = MakeReplicaId(replicationCardId, replicationCard->Replicas.size());

    replicationCard->Replicas[newReplicaId] = TReplicaInfo{
        .ReplicaPath = TYPath(path),
        .ContentType = contentType,
        .Mode = mode,
        .State = ETableReplicaState::Enabled,
        .ReplicationProgress = ConvertTo<TReplicationProgress>(TYsonStringBuf(progress)),
    };

    return newReplicaId;
}

struct TLagTimeItem
{
    std::string TabletId;
    ETableReplicaMode Mode;
    TDuration LagTime;
};

TLagTimeItem ParseLagTime(const INodePtr& lagNode)
{
    EXPECT_EQ(lagNode->GetType(), ENodeType::Map);
    auto lagNodeMap = lagNode->AsMap();

    auto tabletIdNode = lagNodeMap->GetChildOrThrow("tablet_id");
    EXPECT_EQ(tabletIdNode->GetType(), ENodeType::String);

    auto tabletModeNode = lagNodeMap->GetChildOrThrow("replication_mode");
    EXPECT_EQ(tabletModeNode->GetType(), ENodeType::String);

    auto tabletLagDurationNode = lagNodeMap->GetChildOrThrow("replication_lag_time");
    EXPECT_EQ(tabletLagDurationNode->GetType(), ENodeType::Int64);

    return TLagTimeItem{
        .TabletId = tabletIdNode->AsString()->GetValue(),
        .Mode = ConvertTo<ETableReplicaMode>(tabletModeNode),
        .LagTime = ConvertTo<TDuration>(tabletLagDurationNode),
    };
}

////////////////////////////////////////////////////////////////////////////////

TEST(TChaosReplicaLagTest, TestGetSortedLag)
{
    auto syncProgress = "{"
        "segments=[{lower_key=[];timestamp=107374182400};{lower_key=[\"c\"];timestamp=214748364800};];"
        "upper_key=[<type=max>#]}"sv;
    auto asyncProgress = "{segments=["
            "{lower_key=[];timestamp=10737418240};"
            "{lower_key=[\"a\"];timestamp=107374182400};"
            "{lower_key=[\"d\"];timestamp=10737418240};"
        "];upper_key=[<type=max>#]}"sv;

    auto replicationCardId = GenerateReplicationCardId();
    auto replicationCard = New<TReplicationCard>();
    auto syncQueueReplicaId = AddReplicationCardReplica(
        replicationCardId,
        replicationCard,
        ETableReplicaMode::Sync,
        ETableReplicaContentType::Queue,
        "/sync_q",
        syncProgress);
    replicationCard->Replicas[syncQueueReplicaId].History.emplace_back(TReplicaHistoryItem{
        .Era = 1,
        .Timestamp = 1073741824, // 1s.
        .Mode = ETableReplicaMode::Sync,
        .State = ETableReplicaState::Enabled,
    });

    auto syncDataReplicaId = AddReplicationCardReplica(
        replicationCardId,
        replicationCard,
        ETableReplicaMode::Sync,
        ETableReplicaContentType::Data,
        "/sync_d",
        syncProgress);
    replicationCard->Replicas[syncDataReplicaId].History.emplace_back(TReplicaHistoryItem{
        .Era = 1,
        .Timestamp = 161061273600, // 150s.
        .Mode = ETableReplicaMode::Sync,
        .State = ETableReplicaState::Enabled,
    });

    auto asyncDataReplicaId = AddReplicationCardReplica(
        replicationCardId,
        replicationCard,
        ETableReplicaMode::Async,
        ETableReplicaContentType::Data,
        "/async_d",
        asyncProgress);
    replicationCard->Replicas[asyncDataReplicaId].History.emplace_back(TReplicaHistoryItem{
        .Era = 1,
        .Timestamp = 10737418240, // 10s.
        .Mode = ETableReplicaMode::Async,
        .State = ETableReplicaState::Enabled,
    });

    std::vector<TLegacyKey> pivotKeys;
    std::vector<TLegacyOwningKey> buffer;
    std::vector<TTabletId> tabletIds;

    pivotKeys.push_back(EmptyKey().Get());
    tabletIds.push_back(GenerateTabletId());
    for (char c = 'a'; c < 'f'; ++c) {
        buffer.push_back(MakeUnversionedOwningRow(std::string(1, c)));
        pivotKeys.push_back(buffer.back().Get());
        tabletIds.push_back(GenerateTabletId());
    }

    {
        auto absentReplicaId = MakeReplicaId(replicationCardId, replicationCard->Replicas.size());
        auto lagTimes = GetReplicationLagAttribute(absentReplicaId, replicationCard, pivotKeys, tabletIds);
        auto node = NYTree::ConvertToNode(lagTimes);
        EXPECT_EQ(node->GetType(), ENodeType::Entity);
    }

    {
        auto lagTimes = GetReplicationLagAttribute(syncQueueReplicaId, replicationCard, pivotKeys, tabletIds);
        auto node = NYTree::ConvertToNode(lagTimes);
        EXPECT_EQ(node->GetType(), ENodeType::List);
        auto listNode = node->AsList();
        auto childNodes = listNode->GetChildren();
        EXPECT_EQ(std::ssize(childNodes), std::ssize(tabletIds));

        for (int index = 0; index < std::ssize(tabletIds); ++index) {
            const auto& lagNode = childNodes[index];
            auto lagItem = ParseLagTime(lagNode);
            EXPECT_EQ(lagItem.TabletId, ToString(tabletIds[index]));
            EXPECT_EQ(lagItem.Mode, ETableReplicaMode::Sync);
            EXPECT_EQ(lagItem.LagTime, TDuration::Zero());
        }
    }

    {
        auto lagTimes = GetReplicationLagAttribute(syncDataReplicaId, replicationCard, pivotKeys, tabletIds);
        auto node = NYTree::ConvertToNode(lagTimes);
        EXPECT_EQ(node->GetType(), ENodeType::List);
        auto listNode = node->AsList();
        auto childNodes = listNode->GetChildren();
        EXPECT_EQ(std::ssize(childNodes), std::ssize(tabletIds));
        int index = 0;
        for (; index < 3; ++index) {
            const auto& lagNode = childNodes[index];
            auto lagItem = ParseLagTime(lagNode);
            EXPECT_EQ(lagItem.TabletId, ToString(tabletIds[index]));
            EXPECT_EQ(lagItem.Mode, ETableReplicaMode::Async);
            EXPECT_EQ(lagItem.LagTime, TDuration::Seconds(49));
        }

        for (; index < std::ssize(tabletIds); ++index) {
            const auto& lagNode = childNodes[index];
            auto lagItem = ParseLagTime(lagNode);
            EXPECT_EQ(lagItem.TabletId, ToString(tabletIds[index]));
            EXPECT_EQ(lagItem.Mode, ETableReplicaMode::Sync);
            EXPECT_EQ(lagItem.LagTime, TDuration::Zero());
        }
    }

    {
        auto lagTimes = GetReplicationLagAttribute(asyncDataReplicaId, replicationCard, pivotKeys, tabletIds);
        auto node = NYTree::ConvertToNode(lagTimes);
        EXPECT_EQ(node->GetType(), ENodeType::List);
        auto listNode = node->AsList();
        auto childNodes = listNode->GetChildren();
        EXPECT_EQ(std::ssize(childNodes), std::ssize(tabletIds));
        std::vector<TDuration> durations {
            TDuration::Seconds(139), // [].
            TDuration::Seconds(49),  // a.
            TDuration::Seconds(49),  // b.
            TDuration::Seconds(99), // c.
            TDuration::Seconds(189), // d.
            TDuration::Seconds(189), // e.
        };

        EXPECT_EQ(std::ssize(durations), std::ssize(tabletIds));

        for (int index = 0; index < std::ssize(tabletIds); ++index) {
            const auto& lagNode = childNodes[index];
            auto lagItem = ParseLagTime(lagNode);
            EXPECT_EQ(lagItem.TabletId, ToString(tabletIds[index]));
            EXPECT_EQ(lagItem.Mode, ETableReplicaMode::Async);
            EXPECT_EQ(lagItem.LagTime, durations[index]);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableServer
