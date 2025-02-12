#pragma once

#include "public.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/ytlib/hydra/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/query_client/query_service_proxy.h>

#include <yt/yt/ytlib/tablet_client/public.h>

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

TCompactVector<const NHiveClient::TCellPeerDescriptor*, NTabletClient::TypicalPeerCount> GetValidPeers(
    const NHiveClient::TCellDescriptor& cellDescriptor);

const NHiveClient::TCellPeerDescriptor& GetPrimaryTabletPeerDescriptor(
    const NHiveClient::TCellDescriptor& cellDescriptor,
    NHydra::EPeerKind peerKind = NHydra::EPeerKind::Leader);

const NHiveClient::TCellPeerDescriptor& GetBackupTabletPeerDescriptor(
    const NHiveClient::TCellDescriptor& cellDescriptor,
    const NHiveClient::TCellPeerDescriptor& primaryPeerDescriptor);

NRpc::IChannelPtr CreateTabletReadChannel(
    const NRpc::IChannelFactoryPtr& channelFactory,
    const NHiveClient::TCellDescriptor& cellDescriptor,
    const TTabletReadOptions& options,
    const NNodeTrackerClient::TNetworkPreferenceList& networks);

void ValidateTabletMountedOrFrozen(
    const NTabletClient::TTableMountInfoPtr& tableInfo,
    const NTabletClient::TTabletInfoPtr& tabletInfo);

void ValidateTabletMounted(
    const NTabletClient::TTableMountInfoPtr& tableInfo,
    const NTabletClient::TTabletInfoPtr& tabletInfo);

NTableClient::TNameTableToSchemaIdMapping BuildColumnIdMapping(
    const NTableClient::TTableSchema& schema,
    const NTableClient::TNameTablePtr& nameTable,
    bool allowKeyExtension = false);

TSharedRange<NTableClient::TUnversionedRow> PermuteAndEvaluateKeys(
    const NTabletClient::TTableMountInfoPtr& tableInfo,
    const NTableClient::TNameTablePtr& nameTable,
    const TSharedRange<NTableClient::TLegacyKey>& keys,
    const NQueryClient::TColumnEvaluatorPtr& columnEvaluator);

NTabletClient::TTabletInfoPtr GetSortedTabletForRow(
    const NTabletClient::TTableMountInfoPtr& tableInfo,
    NTableClient::TUnversionedRow row,
    bool validateWrite = false);

NTabletClient::TTabletInfoPtr GetSortedTabletForRow(
    const NTabletClient::TTableMountInfoPtr& tableInfo,
    NTableClient::TVersionedRow row,
    bool validateWrite = false);

NTabletClient::TTabletInfoPtr GetOrderedTabletForRow(
    const NTabletClient::TTableMountInfoPtr& tableInfo,
    const NTabletClient::TTabletInfoPtr& randomTabletInfo,
    std::optional<int> tabletIndexColumnId,
    NTableClient::TLegacyKey key,
    bool validateWrite = false);

////////////////////////////////////////////////////////////////////////////////

bool IsTimestampInSync(
    NHiveClient::TTimestamp userTimestamp,
    NHiveClient::TTimestamp replicationTimestamp);

bool IsReplicaSync(
    const NQueryClient::NProto::TReplicaInfo& replicaInfo,
    const NQueryClient::NProto::TTabletInfo& tabletInfo);

TFuture<TTableReplicaInfoPtrList> PickInSyncReplicas(
    const IConnectionPtr& connection,
    const NTabletClient::TTableMountInfoPtr& tableInfo,
    const TTabletReadOptions& options,
    const std::vector<std::pair<NTableClient::TLegacyKey, int>>& keys);

TFuture<TTableReplicaInfoPtrList> PickInSyncReplicas(
    const IConnectionPtr& connection,
    const NTabletClient::TTableMountInfoPtr& tableInfo,
    const TTabletReadOptions& options);

TFuture<TTableReplicaInfoPtrList> PickInSyncReplicas(
    const IConnectionPtr& connection,
    const NTabletClient::TTableMountInfoPtr& tableInfo,
    const TTabletReadOptions& options,
    THashMap<NObjectClient::TCellId, std::vector<NTabletClient::TTabletId>> cellIdToTabletIds);

////////////////////////////////////////////////////////////////////////////////

//! Will group #cellIds by cell peer addresses. Corresponsing cell descriptors will be returned.
//! NB: If a cell has multiple or zero peers, it will be assigned to its own slot in the returned list.
//! This is among other reasons due to various channel picking policies (i.e. EPeerKind values).
std::vector<std::vector<NHiveClient::TCellDescriptorPtr>> GroupCellDescriptorsByPeer(
    const IConnectionPtr& connection,
    const std::vector<NObjectClient::TCellId>& cellIds);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
