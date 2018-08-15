#pragma once

#include <yt/server/hydra/public.h>

#include <yt/ytlib/hydra/public.h>

#include <yt/ytlib/tablet_client/public.h>

#include <yt/core/misc/arithmetic_formula.h>
#include <yt/core/misc/enum.h>
#include <yt/core/misc/public.h>

namespace NYT {
namespace NTabletServer {

////////////////////////////////////////////////////////////////////////////////

using NHydra::TPeerId;
using NHydra::InvalidPeerId;
using NHydra::EPeerState;

using NTabletClient::TTabletCellBundleId;
using NTabletClient::NullTabletCellBundleId;
using NTabletClient::TTabletCellId;
using NTabletClient::NullTabletCellId;
using NTabletClient::TTabletId;
using NTabletClient::NullTabletId;
using NTabletClient::TStoreId;
using NTabletClient::ETabletState;
using NTabletClient::ETableReplicaMode;
using NTabletClient::TypicalPeerCount;
using NTabletClient::TTableReplicaId;
using NTabletClient::TTabletActionId;

using NTabletClient::TTabletCellConfig;
using NTabletClient::TTabletCellConfigPtr;
using NTabletClient::TTabletCellOptions;
using NTabletClient::TTabletCellOptionsPtr;
using NTabletClient::TDynamicTabletCellOptions;
using NTabletClient::TDynamicTabletCellOptionsPtr;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETabletCellHealth,
    (Initializing)
    (Good)
    (Degraded)
    (Failed)
);

DEFINE_ENUM(ETableReplicaState,
    ((None)                     (0))
    ((Disabling)                (1))
    ((Disabled)                 (2))
    ((Enabled)                  (3))
);

DEFINE_ENUM(ETabletActionKind,
    ((Move)                     (0))
    ((Reshard)                  (1))
);

DEFINE_ENUM(ETabletActionState,
    ((Preparing)                (0))
    ((Freezing)                 (1))
    ((Frozen)                   (2))
    ((Unmounting)               (3))
    ((Unmounted)                (4))
    ((Orphaned)                (10))
    ((Mounting)                 (5))
    ((Mounted)                  (6))
    ((Completed)                (7))
    ((Failing)                  (8))
    ((Failed)                   (9))
);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TTabletManager)
DECLARE_REFCOUNTED_CLASS(TTabletBalancer)
DECLARE_REFCOUNTED_CLASS(TBundleNodeTracker)
DECLARE_REFCOUNTED_CLASS(TTabletCellDecommissioner)
DECLARE_REFCOUNTED_CLASS(TReplicatedTableManager)

DECLARE_REFCOUNTED_CLASS(TTabletManagerConfig)
DECLARE_REFCOUNTED_CLASS(TTabletBalancerConfig)
DECLARE_REFCOUNTED_CLASS(TTabletBalancerMasterConfig)
DECLARE_REFCOUNTED_CLASS(TTabletCellDecommissionerConfig)
DECLARE_REFCOUNTED_CLASS(TReplicatedTableManagerConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicTabletBalancerMasterConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicTabletCellBalancerMasterConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicTabletManagerConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicReplicatedTableManagerConfig)

class TTableReplica;

using TTableReplicaId = NObjectClient::TObjectId;

DECLARE_ENTITY_TYPE(TTabletCellBundle, TTabletCellBundleId, NObjectClient::TDirectObjectIdHash)
DECLARE_ENTITY_TYPE(TTabletCell, TTabletCellId, NObjectClient::TDirectObjectIdHash)
DECLARE_ENTITY_TYPE(TTablet, TTabletId, NObjectClient::TDirectObjectIdHash)
DECLARE_ENTITY_TYPE(TTableReplica, TTableReplicaId, NObjectClient::TDirectObjectIdHash)
DECLARE_ENTITY_TYPE(TTabletAction, TTabletActionId, NObjectClient::TDirectObjectIdHash)

struct TTabletStatistics;
struct TTabletPerformanceCounter;
struct TTabletPerformanceCounters;

extern const TString DefaultTabletCellBundleName;

extern const TTimeFormula DefaultTabletBalancerSchedule;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
