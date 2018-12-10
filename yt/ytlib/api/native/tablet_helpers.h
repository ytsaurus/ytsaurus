#pragma once
#include "public.h"

#include <yt/client/api/client.h>

#include <yt/ytlib/hive/public.h>

#include <yt/ytlib/hydra/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/tablet_client/public.h>

#include <yt/client/table_client/unversioned_row.h>
#include <yt/client/table_client/versioned_row.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NApi {
namespace NNative {

////////////////////////////////////////////////////////////////////////////////

SmallVector<const NHiveClient::TCellPeerDescriptor*, NTabletClient::TypicalPeerCount> GetValidPeers(
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
    const NTableClient::TNameTablePtr& nameTable);

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
    NTableClient::TKey key,
    bool validateWrite = false);

////////////////////////////////////////////////////////////////////////////////

} // namespace NNative
} // namespace NApi
} // namespace NYT
