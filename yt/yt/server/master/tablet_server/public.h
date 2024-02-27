#pragma once

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/lib/hydra/public.h>

#include <yt/yt/ytlib/hydra/public.h>

#include <yt/yt/ytlib/tablet_client/public.h>
#include <yt/yt/ytlib/tablet_client/backup.h>

#include <yt/yt/core/misc/arithmetic_formula.h>
#include <yt/yt/core/misc/public.h>

#include <library/cpp/yt/misc/enum.h>

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NTableClient::NProto {

class TRspCheckBackup;

} // namespace NYT::NTableClient::NProto

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TTabletCellStatistics;
class TTabletResources;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

using NHydra::InvalidPeerId;
using NHydra::EPeerState;

using NTabletClient::TTabletCellBundleId;
using NTabletClient::NullTabletCellBundleId;
using NTabletClient::TTabletCellId;
using NTabletClient::NullTabletCellId;
using NTabletClient::TTabletId;
using NTabletClient::NullTabletId;
using NTabletClient::TStoreId;
using NTabletClient::THunkStorageId;
using NTabletClient::ETabletState;
using NTabletClient::ETableReplicaMode;
using NTabletClient::TypicalPeerCount;
using NTabletClient::TTableReplicaId;
using NTabletClient::TTabletActionId;
using NTabletClient::TTabletOwnerId;
using NTabletClient::TTableReplicaId;

using NTabletClient::TTabletCellOptions;
using NTabletClient::TTabletCellOptionsPtr;
using NTabletClient::TDynamicTabletCellOptions;
using NTabletClient::TDynamicTabletCellOptionsPtr;
using NTabletClient::ETabletCellHealth;
using NTabletClient::ETableReplicaState;
using NTabletClient::ETabletActionKind;
using NTabletClient::ETabletActionState;
using NTabletClient::ETableBackupState;
using NTabletClient::ETabletBackupState;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TTabletManager)
DECLARE_REFCOUNTED_CLASS(TTabletService)
DECLARE_REFCOUNTED_CLASS(TTabletBalancer)
DECLARE_REFCOUNTED_CLASS(TTabletCellDecommissioner)
DECLARE_REFCOUNTED_CLASS(TTabletActionManager)
DECLARE_REFCOUNTED_CLASS(TReplicatedTableTracker)
DECLARE_REFCOUNTED_STRUCT(IReplicatedTableTrackerStateProvider)
DECLARE_REFCOUNTED_STRUCT(ITabletCellBalancerProvider)
DECLARE_REFCOUNTED_STRUCT(ITabletNodeTracker)
DECLARE_REFCOUNTED_STRUCT(IBackupManager)
DECLARE_REFCOUNTED_CLASS(TMountConfigStorage)
DECLARE_REFCOUNTED_STRUCT(ITabletChunkManager)

struct ITabletCellBalancer;

DECLARE_REFCOUNTED_CLASS(TTabletBalancerMasterConfig)
DECLARE_REFCOUNTED_CLASS(TTabletCellDecommissionerConfig)
DECLARE_REFCOUNTED_CLASS(TTabletActionManagerMasterConfig)
DECLARE_REFCOUNTED_CLASS(TReplicatedTableTrackerConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicTabletCellBalancerMasterConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicTabletManagerConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicTablesMulticellGossipConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicTabletNodeTrackerConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicCellHydraPersistenceSynchronizerConfig)

class TTableReplica;

DECLARE_ENTITY_TYPE(TTabletCellBundle, TTabletCellBundleId, NObjectClient::TDirectObjectIdHash)
DECLARE_ENTITY_TYPE(TTabletCell, TTabletCellId, NObjectClient::TDirectObjectIdHash)
DECLARE_ENTITY_TYPE(TTablet, TTabletId, NObjectClient::TDirectObjectIdHash)
DECLARE_ENTITY_TYPE(THunkTablet, TTabletId, NObjectClient::TDirectObjectIdHash)
DECLARE_ENTITY_TYPE(TTabletBase, TTabletId, NObjectClient::TDirectObjectIdHash)
DECLARE_ENTITY_TYPE(TTabletOwnerBase, TTabletOwnerId, NObjectClient::TDirectObjectIdHash)
DECLARE_ENTITY_TYPE(TTableReplica, TTableReplicaId, NObjectClient::TDirectObjectIdHash)
DECLARE_ENTITY_TYPE(TTabletAction, TTabletActionId, NObjectClient::TDirectObjectIdHash)
DECLARE_ENTITY_TYPE(THunkStorageNode, THunkStorageId, NObjectClient::TDirectObjectIdHash)

DECLARE_MASTER_OBJECT_TYPE(TTabletCellBundle)
DECLARE_MASTER_OBJECT_TYPE(THunkStorageNode)

struct TTabletStatistics;

extern const TString DefaultTabletCellBundleName;
extern const TString SequoiaTabletCellBundleName;

extern const TTimeFormula DefaultTabletBalancerSchedule;

constexpr i64 EdenStoreIdsSizeLimit = 100;

constexpr auto DefaultSyncTabletActionKeepalivePeriod = TDuration::Minutes(1);

constexpr int DefaultTabletCountLimit = 1000;

constexpr int MaxStoresPerBackupMutation = 10000;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
