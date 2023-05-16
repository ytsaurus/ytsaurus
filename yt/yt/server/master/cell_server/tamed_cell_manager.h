#pragma once

#include "public.h"
#include "config.h"
#include "cell_base.h"
#include "cell_bundle.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/cypress_server/public.h>

#include <yt/yt/server/lib/hydra_common/entity_map.h>

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/client/object_client/public.h>

namespace NYT::NCellServer {

////////////////////////////////////////////////////////////////////////////////

struct ITamedCellManager
    : public virtual TRefCounted
{
public:
    virtual void Initialize() = 0;

    virtual const TCellSet* FindAssignedCells(const TString& address) const = 0;

    virtual const IBundleNodeTrackerPtr& GetBundleNodeTracker() = 0;

    DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS(CellBundle, TCellBundle);
    virtual const THashSet<TCellBundle*>& CellBundles(NCellarClient::ECellarType cellarType) = 0;
    virtual TCellBundle* FindCellBundleByName(
        const TString& name,
        NCellarClient::ECellarType cellarType,
        bool activeLifeStageOnly) = 0;
    virtual TCellBundle* GetCellBundleByNameOrThrow(
        const TString& name,
        NCellarClient::ECellarType cellarType,
        bool activeLifeStageOnly) = 0;
    virtual TCellBundle* GetCellBundleByIdOrThrow(
        TCellBundleId cellBundleId,
        bool activeLifeStageOnly) = 0;
    virtual void RenameCellBundle(TCellBundle* cellBundle, const TString& newName) = 0;
    virtual void SetCellBundleOptions(TCellBundle* cellBundle, TTabletCellOptionsPtr options) = 0;
    virtual TCellBundle* CreateCellBundle(
        const TString& name,
        std::unique_ptr<TCellBundle> holder,
        TTabletCellOptionsPtr options) = 0;
    virtual void ZombifyCellBundle(TCellBundle* cellBundle) = 0;
    virtual void DestroyCellBundle(TCellBundle* cellBundle) = 0;

    DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS(Cell, TCellBase);
    virtual const THashSet<TCellBase*>& Cells(NCellarClient::ECellarType cellarType) = 0;
    virtual TCellBase* GetCellOrThrow(TTamedCellId id) = 0;
    virtual void RemoveCell(TCellBase* cell, bool force) = 0;

    virtual TCellBase* FindCellByCellTag(NObjectClient::TCellTag cellTag) = 0;
    virtual TCellBase* GetCellByCellTagOrThrow(NObjectClient::TCellTag cellTag) = 0;

    virtual TCellBase* CreateCell(TCellBundle* cellBundle, TArea* area, std::unique_ptr<TCellBase> holder) = 0;
    virtual void ZombifyCell(TCellBase* cell) = 0;
    virtual void DestroyCell(TCellBase* cell) = 0;
    virtual void UpdatePeerCount(TCellBase* cell, std::optional<int> peerCount) = 0;

    DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS(Area, TArea);
    virtual TArea* CreateArea(
        const TString& name,
        TCellBundle* cellBundle,
        NObjectClient::TObjectId hintId) = 0;
    virtual void RenameArea(TArea* area, const TString& name) = 0;
    virtual void ZombifyArea(TArea* area) = 0;
    virtual void SetAreaNodeTagFilter(TArea* area, const TString& formula) = 0;
    virtual TArea* GetAreaByNameOrThrow(TCellBundle* cellBundle, const TString& name) = 0;

    virtual void UpdateCellArea(TCellBase* cell, TArea* area) = 0;

    DECLARE_INTERFACE_SIGNAL(void(TCellBundle* bundle), CellBundleDestroyed);
    DECLARE_INTERFACE_SIGNAL(void(TArea* area), AreaCreated);
    DECLARE_INTERFACE_SIGNAL(void(TArea* area), AreaDestroyed);
    DECLARE_INTERFACE_SIGNAL(void(TArea* area), AreaNodeTagFilterChanged);
    DECLARE_INTERFACE_SIGNAL(void(TCellBase* cell), CellCreated);
    DECLARE_INTERFACE_SIGNAL(void(TCellBase* cell), CellDecommissionStarted);
    DECLARE_INTERFACE_SIGNAL(void(), CellPeersAssigned);
    DECLARE_INTERFACE_SIGNAL(void(), AfterSnapshotLoaded);
};

DEFINE_REFCOUNTED_TYPE(ITamedCellManager)

ITamedCellManagerPtr CreateTamedCellManager(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
