#pragma once

#include <yt/yt/server/master/cell_server/public.h>

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/lib/hydra/public.h>

#include <yt/yt/ytlib/hydra/public.h>

#include <yt/yt/client/chaos_client/public.h>

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NChaosServer {

////////////////////////////////////////////////////////////////////////////////

using NHydra::TPeerId;
using NHydra::InvalidPeerId;
using NHydra::EPeerState;

using NChaosClient::TReplicationCardId;
using NChaosClient::TReplicaId;

////////////////////////////////////////////////////////////////////////////////

class TChaosReplicatedTableNode;

DECLARE_REFCOUNTED_CLASS(TAlienCellSynchronizerConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicChaosManagerConfig)

DECLARE_REFCOUNTED_CLASS(TChaosPeerConfig)
DECLARE_REFCOUNTED_CLASS(TChaosHydraConfig)

DECLARE_REFCOUNTED_STRUCT(IChaosManager)
DECLARE_REFCOUNTED_STRUCT(IAlienCellSynchronizer)
DECLARE_REFCOUNTED_CLASS(TAlienClusterRegistry)

using TChaosCellBundleId = NCellServer::TCellBundleId;
using TChaosCellId = NCellServer::TTamedCellId;

DECLARE_ENTITY_TYPE(TChaosCellBundle, TChaosCellBundleId, NObjectClient::TDirectObjectIdHash)
DECLARE_ENTITY_TYPE(TChaosCell, TChaosCellId, NObjectClient::TDirectObjectIdHash)

DECLARE_MASTER_OBJECT_TYPE(TChaosCellBundle)
DECLARE_MASTER_OBJECT_TYPE(TChaosCell)

constexpr int TypicalAlienPeerCount = 2;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
