#pragma once

#include <yt/server/master/tablet_server/public.h>

#include <yt/server/lib/hydra/public.h>

#include <yt/ytlib/hydra/public.h>

#include <yt/ytlib/tablet_client/public.h>

#include <yt/core/misc/enum.h>
#include <yt/core/misc/public.h>

namespace NYT::NCellServer {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TCellStatus;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

using NHydra::TPeerId;
using NHydra::InvalidPeerId;
using NHydra::EPeerState;

using TCellBundleId = NTabletClient::TTabletCellBundleId;
using NTabletClient::NullTabletCellBundleId;
using TTamedCellId = NTabletClient::TTabletCellId;
using NTabletClient::NullTabletCellId;
using NTabletClient::TypicalPeerCount;

using TTamedCellConfig = NTabletClient::TTabletCellConfig;
using TTamedCellConfigPtr = NTabletClient::TTabletCellConfigPtr;
using NTabletClient::TTabletCellOptions;
using NTabletClient::TTabletCellOptionsPtr;
using NTabletClient::TDynamicTabletCellOptions;
using NTabletClient::TDynamicTabletCellOptionsPtr;
using ECellHealth = NTabletClient::ETabletCellHealth;
using ECellLifeStage = NTabletClient::ETabletCellLifeStage;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TTamedCellManager)
DECLARE_REFCOUNTED_CLASS(TBundleNodeTracker)
DECLARE_REFCOUNTED_CLASS(TCellBaseDecommissioner)
DECLARE_REFCOUNTED_CLASS(TCellHydraJanitor)

DECLARE_REFCOUNTED_STRUCT(ICellBalancerProvider)

DECLARE_REFCOUNTED_CLASS(TCellBalancerConfig)

struct ICellBalancer;

using TDynamicCellManagerConfig = NTabletServer::TDynamicTabletManagerConfig;
using TDynamicCellManagerConfigPtr = NTabletServer::TDynamicTabletManagerConfigPtr;

DECLARE_ENTITY_TYPE(TCellBundle, TCellBundleId, NObjectClient::TDirectObjectIdHash)
DECLARE_ENTITY_TYPE(TCellBase, TTamedCellId, NObjectClient::TDirectObjectIdHash)

extern const TString DefaultCellBundleName;

using TCellSet = SmallVector<std::pair<const TCellBase*, int>, NTabletClient::TypicalTabletSlotCount>;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EErrorCode,
    ((NodeDecommissioned)           (1401))
    ((NodeBanned)                   (1402))
    ((NodeTabletSlotsDisabled)      (1403))
    ((NodeFilterMismatch)           (1404))
    ((CellDidNotAppearWithinTimeout)(1405))
)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
