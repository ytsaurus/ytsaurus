#pragma once

#include <yt/yt/server/master/cell_server/public.h>

#include <yt/yt/server/lib/hydra/public.h>

#include <yt/yt/ytlib/hydra/public.h>

#include <yt/yt/core/misc/enum.h>
#include <yt/yt/core/misc/public.h>

namespace NYT::NChaosServer {

////////////////////////////////////////////////////////////////////////////////

using NHydra::TPeerId;
using NHydra::InvalidPeerId;
using NHydra::EPeerState;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IChaosManager)
DECLARE_REFCOUNTED_CLASS(TDynamicChaosManagerConfig)

DECLARE_REFCOUNTED_CLASS(TChaosPeerConfig)
DECLARE_REFCOUNTED_CLASS(TChaosHydraConfig)

using TChaosCellBundleId = NCellServer::TCellBundleId;
using TChaosCellId = NCellServer::TTamedCellId;

DECLARE_ENTITY_TYPE(TChaosCellBundle, TChaosCellBundleId, NObjectClient::TDirectObjectIdHash)
DECLARE_ENTITY_TYPE(TChaosCell, TChaosCellId, NObjectClient::TDirectObjectIdHash)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
