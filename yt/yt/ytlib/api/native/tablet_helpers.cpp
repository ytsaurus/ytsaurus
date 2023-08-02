#include "tablet_helpers.h"
#include "config.h"
#include "connection.h"
#include "tablet_sync_replica_cache.h"

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/core/misc/hedging_manager.h>

#include <yt/yt/core/rpc/hedging_channel.h>

namespace NYT::NApi::NNative {

using namespace NObjectClient;
using namespace NTabletClient;
using namespace NTableClient;
using namespace NHiveClient;
using namespace NRpc;
using namespace NHydra;
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

TCompactVector<const TCellPeerDescriptor*, TypicalPeerCount> GetValidPeers(const TCellDescriptor& cellDescriptor)
{
    TCompactVector<const TCellPeerDescriptor*, TypicalPeerCount> peers;
    for (const auto& peer : cellDescriptor.Peers) {
        if (!peer.IsNull()) {
            peers.push_back(&peer);
        }
    }
    return peers;
}

const TCellPeerDescriptor& GetPrimaryTabletPeerDescriptor(
    const TCellDescriptor& cellDescriptor,
    EPeerKind peerKind)
{
    auto peers = GetValidPeers(cellDescriptor);

    if (peers.empty()) {
        THROW_ERROR_EXCEPTION("No alive peers for tablet cell %v",
            cellDescriptor.CellId);
    }

    int leadingPeerIndex = -1;
    for (int index = 0; index < std::ssize(peers); ++index) {
        if (peers[index]->GetVoting()) {
            leadingPeerIndex = index;
            break;
        }
    }

    switch (peerKind) {
        case EPeerKind::Leader: {
            if (leadingPeerIndex < 0) {
                THROW_ERROR_EXCEPTION("No leading peer is known for tablet cell %v",
                    cellDescriptor.CellId);
            }
            return *peers[leadingPeerIndex];
        }

        case EPeerKind::LeaderOrFollower: {
            int randomIndex = RandomNumber(peers.size());
            return *peers[randomIndex];
        }

        case EPeerKind::Follower: {
            if (leadingPeerIndex < 0 || peers.size() == 1) {
                int randomIndex = RandomNumber(peers.size());
                return *peers[randomIndex];
            } else {
                int randomIndex = RandomNumber(peers.size() - 1);
                if (randomIndex >= leadingPeerIndex) {
                    ++randomIndex;
                }
                return *peers[randomIndex];
            }
        }

        default:
            YT_ABORT();
    }
}

const TCellPeerDescriptor& GetBackupTabletPeerDescriptor(
    const TCellDescriptor& cellDescriptor,
    const TCellPeerDescriptor& primaryPeerDescriptor)
{
    auto peers = GetValidPeers(cellDescriptor);

    YT_ASSERT(peers.size() > 1);

    int primaryPeerIndex = -1;
    for (int index = 0; index < std::ssize(peers); ++index) {
        if (peers[index] == &primaryPeerDescriptor) {
            primaryPeerIndex = index;
            break;
        }
    }

    YT_ASSERT(primaryPeerIndex >= 0 && primaryPeerIndex < std::ssize(peers));

    int randomIndex = RandomNumber(peers.size() - 1);
    if (randomIndex >= primaryPeerIndex) {
        ++randomIndex;
    }

    return *peers[randomIndex];
}

IChannelPtr CreateTabletReadChannel(
    const IChannelFactoryPtr& channelFactory,
    const TCellDescriptor& cellDescriptor,
    const TTabletReadOptions& options,
    const TNetworkPreferenceList& networks)
{
    const auto& primaryPeerDescriptor = GetPrimaryTabletPeerDescriptor(cellDescriptor, options.ReadFrom);
    auto primaryChannel = channelFactory->CreateChannel(primaryPeerDescriptor.GetAddressOrThrow(networks));
    if (cellDescriptor.Peers.size() == 1 || !options.RpcHedgingDelay) {
        return primaryChannel;
    }

    const auto& backupPeerDescriptor = GetBackupTabletPeerDescriptor(cellDescriptor, primaryPeerDescriptor);
    auto backupChannel = channelFactory->CreateChannel(backupPeerDescriptor.GetAddressOrThrow(networks));

    return CreateHedgingChannel(
        std::move(primaryChannel),
        std::move(backupChannel),
        THedgingChannelOptions{
            .HedgingManager = CreateSimpleHedgingManager(*options.RpcHedgingDelay),
        });
}

void ValidateTabletMountedOrFrozen(const TTableMountInfoPtr& tableInfo, const TTabletInfoPtr& tabletInfo)
{
    auto state = tabletInfo->State;
    if (state != ETabletState::Mounted &&
        state != ETabletState::Freezing &&
        state != ETabletState::Unfreezing &&
        state != ETabletState::Frozen)
    {
        THROW_ERROR_EXCEPTION(
            NTabletClient::EErrorCode::TabletNotMounted,
            "Cannot read from tablet %v of table %v while it is in %Qlv state",
            tabletInfo->TabletId,
            tableInfo->Path,
            state)
            << TErrorAttribute("tablet_id", tabletInfo->TabletId)
            << TErrorAttribute("is_tablet_unmounted", state == ETabletState::Unmounted);
    }
}

void ValidateTabletMounted(const TTableMountInfoPtr& tableInfo, const TTabletInfoPtr& tabletInfo)
{
    auto state = tabletInfo->State;
    if (state != ETabletState::Mounted) {
        THROW_ERROR_EXCEPTION(
            NTabletClient::EErrorCode::TabletNotMounted,
            "Tablet %v of table %v is in %Qlv state",
            tabletInfo->TabletId,
            tableInfo->Path,
            tabletInfo->State)
            << TErrorAttribute("tablet_id", tabletInfo->TabletId)
            << TErrorAttribute("is_tablet_unmounted", state == ETabletState::Unmounted);
    }
}

void ValidateTabletMounted(
    const TTableMountInfoPtr& tableInfo,
    const TTabletInfoPtr& tabletInfo,
    bool validateWrite)
{
    if (validateWrite) {
        ValidateTabletMounted(tableInfo, tabletInfo);
    } else {
        ValidateTabletMountedOrFrozen(tableInfo, tabletInfo);
    }
}

TNameTableToSchemaIdMapping BuildColumnIdMapping(
    const TTableSchema& schema,
    const TNameTablePtr& nameTable,
    bool allowMissingKeyColumns)
{
    if (!allowMissingKeyColumns) {
        for (const auto& name : schema.GetKeyColumns()) {
            // We shouldn't consider computed columns below because client doesn't send them.
            if (!nameTable->FindId(name) && !schema.GetColumnOrThrow(name).Expression()) {
                THROW_ERROR_EXCEPTION("Missing key column %Qv",
                    name);
            }
        }
    }

    TNameTableToSchemaIdMapping mapping;
    mapping.resize(nameTable->GetSize());
    for (int nameTableId = 0; nameTableId < nameTable->GetSize(); ++nameTableId) {
        const auto& name = nameTable->GetName(nameTableId);
        const auto* columnSchema = schema.FindColumn(name);
        mapping[nameTableId] = columnSchema ? schema.GetColumnIndex(*columnSchema) : -1;
    }
    return mapping;
}

namespace {

template <class TRow>
TTabletInfoPtr GetSortedTabletForRowImpl(
    const TTableMountInfoPtr& tableInfo,
    TRow row,
    bool validateWrite)
{
    YT_ASSERT(tableInfo->IsSorted());

    auto tabletInfo = tableInfo->GetTabletForRow(row);
    ValidateTabletMounted(tableInfo, tabletInfo, validateWrite);
    return tabletInfo;
}

} // namespace

TTabletInfoPtr GetSortedTabletForRow(
    const TTableMountInfoPtr& tableInfo,
    TUnversionedRow row,
    bool validateWrite)
{
    return GetSortedTabletForRowImpl(tableInfo, row, validateWrite);
}

TTabletInfoPtr GetSortedTabletForRow(
    const TTableMountInfoPtr& tableInfo,
    TVersionedRow row,
    bool validateWrite)
{
    return GetSortedTabletForRowImpl(tableInfo, row, validateWrite);
}

TTabletInfoPtr GetOrderedTabletForRow(
    const TTableMountInfoPtr& tableInfo,
    const TTabletInfoPtr& randomTabletInfo,
    std::optional<int> tabletIndexColumnId,
    TUnversionedRow row,
    bool validateWrite)
{
    YT_ASSERT(!tableInfo->IsSorted());

    i64 tabletIndex = -1;
    for (const auto& value : row) {
        if (tabletIndexColumnId && value.Id == *tabletIndexColumnId && value.Type != EValueType::Null) {
            try {
                FromUnversionedValue(&tabletIndex, value);
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Error parsing tablet index from row")
                    << ex;
            }
            // Just checking.
            tableInfo->GetTabletByIndexOrThrow(tabletIndex);
        }
    }

    if (tabletIndex < 0) {
        if (tableInfo->ReplicationCardId) {
            THROW_ERROR_EXCEPTION("Invalid input row for chaos ordered table %v: %Qlv column is not provided",
                tableInfo->Path,
                TabletIndexColumnName);
        }

        return randomTabletInfo;
    }

    auto tabletInfo = tableInfo->Tablets[tabletIndex];
    ValidateTabletMounted(tableInfo, tabletInfo, validateWrite);
    return tabletInfo;
}

////////////////////////////////////////////////////////////////////////////////

bool IsReplicaSync(
    const NQueryClient::NProto::TReplicaInfo& replicaInfo,
    const NQueryClient::NProto::TTabletInfo& tabletInfo)
{
    auto replicaStatus = FromProto<ETableReplicaStatus>(replicaInfo.status());
    if (replicaStatus != ETableReplicaStatus::Unknown) {
        return replicaStatus == ETableReplicaStatus::SyncInSync;
    }

    // COMPAT(akozhikhov).
    return
        FromProto<ETableReplicaMode>(replicaInfo.mode()) == ETableReplicaMode::Sync &&
        replicaInfo.current_replication_row_index() >= tabletInfo.total_row_count() + tabletInfo.delayed_lockless_row_count();
}

TTableReplicaInfoPtrList OnTabletInfosReceived(
    const IConnectionPtr& connection,
    const TTableMountInfoPtr& tableInfo,
    int totalTabletCount,
    std::optional<TInstant> cachedSyncReplicasAt,
    THashMap<TTableReplicaId, int> replicaIdToCount,
    const std::vector<NQueryClient::TQueryServiceProxy::TRspGetTabletInfoPtr>& responses)
{
    const auto& Logger = connection->GetLogger();

    THashMap<TTabletId, TTableReplicaIdList> tabletIdToSyncReplicaIds;

    for (const auto& response : responses) {
        for (const auto& protoTabletInfo : response->tablets()) {
            TTableReplicaIdList* syncReplicaIds = nullptr;
            if (cachedSyncReplicasAt) {
                auto tabletId = FromProto<TTabletId>(protoTabletInfo.tablet_id());
                auto [it, emplaced] = tabletIdToSyncReplicaIds.try_emplace(tabletId);
                YT_VERIFY(emplaced);
                syncReplicaIds = &it->second;
            }

            for (const auto& protoReplicaInfo : protoTabletInfo.replicas()) {
                if (IsReplicaSync(protoReplicaInfo, protoTabletInfo)) {
                    auto replicaId = FromProto<TTableReplicaId>(protoReplicaInfo.replica_id());
                    ++replicaIdToCount[replicaId];
                    if (cachedSyncReplicasAt) {
                        syncReplicaIds->push_back(replicaId);
                    }
                }
            }
        }
    }

    if (cachedSyncReplicasAt && !tabletIdToSyncReplicaIds.empty()) {
        connection->GetTabletSyncReplicaCache()->Put(
            *cachedSyncReplicasAt,
            std::move(tabletIdToSyncReplicaIds));
    }

    TTableReplicaInfoPtrList inSyncReplicaInfos;
    for (const auto& replicaInfo : tableInfo->Replicas) {
        auto it = replicaIdToCount.find(replicaInfo->ReplicaId);
        if (it != replicaIdToCount.end() && it->second == totalTabletCount) {
            YT_LOG_DEBUG("In-sync replica found (Path: %v, ReplicaId: %v, ClusterName: %v)",
                tableInfo->Path,
                replicaInfo->ReplicaId,
                replicaInfo->ClusterName);
            inSyncReplicaInfos.push_back(replicaInfo);
        }
    }

    return inSyncReplicaInfos;
}

TFuture<TTableReplicaInfoPtrList> PickInSyncReplicas(
    const IConnectionPtr& connection,
    const NTabletClient::TTableMountInfoPtr& tableInfo,
    const TTabletReadOptions& options,
    THashMap<NObjectClient::TCellId, std::vector<NTabletClient::TTabletId>> cellIdToTabletIds)
{
    const auto& Logger = connection->GetLogger();

    auto cachedSyncReplicasAt = options.CachedSyncReplicasTimeout
        ? std::make_optional(TInstant::Now())
        : std::nullopt;

    int totalTabletCount = 0;
    for (const auto& [cellId, tabletIds] : cellIdToTabletIds) {
        totalTabletCount += tabletIds.size();
    }

    THashMap<TTableReplicaId, int> replicaIdToCount;

    int cachedTabletCount = 0;
    if (cachedSyncReplicasAt) {
        auto cachedSyncReplicasDeadline = *cachedSyncReplicasAt - *options.CachedSyncReplicasTimeout;
        auto syncReplicaIdLists = connection->GetTabletSyncReplicaCache()->Filter(
            &cellIdToTabletIds,
            cachedSyncReplicasDeadline);
        for (const auto& syncReplicaIds : syncReplicaIdLists) {
            for (auto syncReplicaId : syncReplicaIds) {
                ++replicaIdToCount[syncReplicaId];
            }
            ++cachedTabletCount;
        }
    }

    YT_LOG_DEBUG("Looking for in-sync replicas "
        "(Path: %v, CellCount: %v, TotalTabletCount: %v, CachedTabletCount: %v)",
        tableInfo->Path,
        cellIdToTabletIds.size(),
        totalTabletCount,
        cachedTabletCount);

    const auto& channelFactory = connection->GetChannelFactory();
    const auto& cellDirectory = connection->GetCellDirectory();

    std::vector<TFuture<NQueryClient::TQueryServiceProxy::TRspGetTabletInfoPtr>> asyncResults;
    if (!cachedSyncReplicasAt) {
        asyncResults.reserve(cellIdToTabletIds.size());
    }

    for (const auto& [cellId, tabletIds] : cellIdToTabletIds) {
        if (tabletIds.empty()) {
            continue;
        }

        auto cellDescriptor = cellDirectory->GetDescriptorOrThrow(cellId);
        auto channel = CreateTabletReadChannel(
            channelFactory,
            *cellDescriptor,
            options,
            connection->GetNetworks());

        NQueryClient::TQueryServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(options.Timeout.value_or(connection->GetConfig()->DefaultGetInSyncReplicasTimeout));

        auto req = proxy.GetTabletInfo();
        req->SetResponseHeavy(true);
        ToProto(req->mutable_tablet_ids(), tabletIds);
        for (int index = 0; index < std::ssize(tabletIds); ++index) {
            ToProto(req->add_cell_ids(), cellId);
        }

        asyncResults.push_back(req->Invoke());
    }

    auto asyncResult = AllSucceeded(std::move(asyncResults));
    if (const auto& resultOrError = asyncResult.TryGet()) {
        return MakeFuture(OnTabletInfosReceived(
            connection,
            tableInfo,
            totalTabletCount,
            cachedSyncReplicasAt,
            std::move(replicaIdToCount),
            resultOrError->ValueOrThrow()));
    } else {
        return asyncResult.Apply(BIND(
            &OnTabletInfosReceived,
            connection,
            tableInfo,
            totalTabletCount,
            cachedSyncReplicasAt,
            Passed(std::move(replicaIdToCount)))
            .AsyncVia(GetCurrentInvoker()));
    }
}

TFuture<TTableReplicaInfoPtrList> PickInSyncReplicas(
    const IConnectionPtr& connection,
    const NTabletClient::TTableMountInfoPtr& tableInfo,
    const TTabletReadOptions& options,
    const std::vector<std::pair<NTableClient::TLegacyKey, int>>& keys)
{
    THashMap<TCellId, std::vector<TTabletId>> cellIdToTabletIds;
    THashSet<TTabletId> tabletIds;
    for (const auto& [key, _] : keys) {
        auto tabletInfo = GetSortedTabletForRow(tableInfo, key);
        auto tabletId = tabletInfo->TabletId;
        if (tabletIds.insert(tabletId).second) {
            cellIdToTabletIds[tabletInfo->CellId].push_back(tabletInfo->TabletId);
        }
    }

    return PickInSyncReplicas(
        connection,
        tableInfo,
        options,
        std::move(cellIdToTabletIds));
}

TFuture<TTableReplicaInfoPtrList> PickInSyncReplicas(
    const IConnectionPtr& connection,
    const NTabletClient::TTableMountInfoPtr& tableInfo,
    const TTabletReadOptions& options)
{
    THashMap<TCellId, std::vector<TTabletId>> cellIdToTabletIds;
    for (const auto& tabletInfo : tableInfo->Tablets) {
        cellIdToTabletIds[tabletInfo->CellId].push_back(tabletInfo->TabletId);
    }

    return PickInSyncReplicas(
        connection,
        tableInfo,
        options,
        std::move(cellIdToTabletIds));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
