#pragma once

#include <yt/yt/server/master/tablet_server/public.h>

#include <yt/yt/server/lib/hydra/public.h>

#include <yt/yt/ytlib/hydra/public.h>

#include <yt/yt/ytlib/cellar_client/public.h>

#include <yt/yt/ytlib/tablet_client/public.h>

#include <library/cpp/yt/misc/enum.h>

#include <library/cpp/yt/compact_containers/compact_vector.h>

namespace NYT::NCellServer {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TCellStatus;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

using NHydra::InvalidPeerId;
using NHydra::EPeerState;

using NCellarClient::CellShardCount;

using TCellBundleId = NTabletClient::TTabletCellBundleId;
using NTabletClient::NullTabletCellBundleId;
using TTamedCellId = NTabletClient::TTabletCellId;
using NTabletClient::NullTabletCellId;
using NTabletClient::TypicalPeerCount;
using TAreaId = NObjectClient::TObjectId;

using NTabletClient::TTabletCellOptions;
using NTabletClient::TTabletCellOptionsPtr;
using NTabletClient::TDynamicTabletCellOptions;
using NTabletClient::TDynamicTabletCellOptionsPtr;
using ECellHealth = NTabletClient::ETabletCellHealth;
using ECellLifeStage = NTabletClient::ETabletCellLifeStage;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TCellBaseDecommissioner)

DECLARE_REFCOUNTED_STRUCT(IBundleNodeTracker)
DECLARE_REFCOUNTED_STRUCT(ICellBalancerProvider)
DECLARE_REFCOUNTED_STRUCT(ICellHydraJanitor)
DECLARE_REFCOUNTED_STRUCT(ICellHydraPersistenceSynchronizer)
DECLARE_REFCOUNTED_STRUCT(ICellTracker)
DECLARE_REFCOUNTED_STRUCT(ICellarNodeTracker)
DECLARE_REFCOUNTED_STRUCT(ITamedCellManager)

DECLARE_REFCOUNTED_CLASS(TCellBalancerBootstrapConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicCellarNodeTrackerConfig)
DECLARE_REFCOUNTED_CLASS(TCellManagerConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicCellManagerConfig)

struct ICellBalancer;

DECLARE_ENTITY_TYPE(TCellBundle, TCellBundleId, NObjectClient::TObjectIdEntropyHash)
DECLARE_ENTITY_TYPE(TCellBase, TTamedCellId, NObjectClient::TObjectIdEntropyHash)
DECLARE_ENTITY_TYPE(TArea, TAreaId, NObjectClient::TObjectIdEntropyHash)

DECLARE_MASTER_OBJECT_TYPE(TArea)
DECLARE_MASTER_OBJECT_TYPE(TCellBase)
DECLARE_MASTER_OBJECT_TYPE(TCellBundle)

extern const std::string DefaultCellBundleName;
extern const std::string DefaultAreaName;

using TCellSet = TCompactVector<std::pair<const TCellBase*, int>, NCellarClient::TypicalCellarSize>;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_ERROR_ENUM(
    ((NodeDecommissioned)           (1401))
    ((NodeBanned)                   (1402))
    ((NodeTabletSlotsDisabled)      (1403))
    ((NodeFilterMismatch)           (1404))
    ((CellDidNotAppearWithinTimeout)(1405))
    ((MasterCellNotReady)           (1406))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
