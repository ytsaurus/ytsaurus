#pragma once

#include "public.h"

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/server/master/cell_master/public.h>

namespace NYT::NChaosServer {

////////////////////////////////////////////////////////////////////////////////

struct IChaosManager
    : public virtual TRefCounted
{
    virtual void Initialize() = 0;

    virtual void ReplicateAlienClusterRegistryToSecondaryMaster(NObjectClient::TCellTag cellTag) const = 0;

    virtual const TAlienClusterRegistryPtr& GetAlienClusterRegistry() const = 0;
    virtual TChaosCell* GetBundleMetadataCell(const TChaosCellBundle* cellBundle) const = 0;
    virtual void SetBundleMetadataCells(
        TChaosCellBundle* cellBundle,
        const std::vector<TChaosCellId>& metadataCellIds) const = 0;

    virtual TChaosCell* FindChaosCellById(TChaosCellId cellId) const = 0;
    virtual TChaosCell* GetChaosCellByIdOrThrow(TChaosCellId cellId) const = 0;

    virtual TChaosCell* FindChaosCellByTag(NObjectClient::TCellTag cellTag) const = 0;
    virtual TChaosCell* GetChaosCellByTagOrThrow(NObjectClient::TCellTag cellTag) const = 0;

    virtual TChaosCellBundle* FindChaosCellBundle(TChaosCellId id) = 0;
    virtual TChaosCellBundle* GetChaosCellBundleOrThrow(TChaosCellBundleId id) = 0;
    virtual TChaosCellBundle* GetChaosCellBundleByNameOrThrow(const TString& name, bool activeLifeStageOnly) const = 0;

    virtual void SetChaosCellBundle(TChaosReplicatedTableNode* node, TChaosCellBundle* cellBundle) = 0;
};

DEFINE_REFCOUNTED_TYPE(IChaosManager)

IChaosManagerPtr CreateChaosManager(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
