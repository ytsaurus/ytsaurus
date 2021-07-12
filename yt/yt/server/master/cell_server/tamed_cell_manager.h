#pragma once

#include "public.h"
#include "config.h"
#include "cell_base.h"
#include "cell_bundle.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/cypress_server/public.h>

#include <yt/yt/server/lib/hydra/entity_map.h>
#include <yt/yt/server/lib/hydra/mutation.h>

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/core/misc/small_vector.h>

namespace NYT::NCellServer {

////////////////////////////////////////////////////////////////////////////////

class TTamedCellManager
    : public TRefCounted
{
public:
    explicit TTamedCellManager(NCellMaster::TBootstrap* bootstrap);
    ~TTamedCellManager();

    void Initialize();

    const TCellSet* FindAssignedCells(const TString& address) const;

    const TBundleNodeTrackerPtr& GetBundleNodeTracker();

    DECLARE_ENTITY_MAP_ACCESSORS(CellBundle, TCellBundle);
    TCellBundle* FindCellBundleByName(const TString& name, bool activeLifeStageOnly);
    TCellBundle* GetCellBundleByNameOrThrow(const TString& name, bool activeLifeStageOnly);
    void RenameCellBundle(TCellBundle* cellBundle, const TString& newName);
    void SetCellBundleOptions(TCellBundle* cellBundle, TTabletCellOptionsPtr options);
    TCellBundle* CreateCellBundle(
        const TString& name,
        std::unique_ptr<TCellBundle> holder,
        TTabletCellOptionsPtr options);
    void ZombifyCellBundle(TCellBundle* cellBundle);
    void DestroyCellBundle(TCellBundle* cellBundle);

    DECLARE_ENTITY_MAP_ACCESSORS(Cell, TCellBase);
    TCellBase* GetCellOrThrow(TTamedCellId id);
    void RemoveCell(TCellBase* cell, bool force);

    TCellBase* CreateCell(TCellBundle* cellBundle, TArea* area, std::unique_ptr<TCellBase> holder);
    void ZombifyCell(TCellBase* cell);
    void DestroyCell(TCellBase* cell);
    void UpdatePeerCount(TCellBase* cell, std::optional<int> peerCount);

    DECLARE_ENTITY_MAP_ACCESSORS(Area, TArea);
    TArea* CreateArea(
        const TString& name,
        TCellBundle* cellBundle,
        NObjectClient::TObjectId hintId);
    void RenameArea(TArea* area, const TString& name);
    void ZombifyArea(TArea* area);
    void SetAreaNodeTagFilter(TArea* area, const TString& formula);

    DECLARE_SIGNAL(void(TCellBundle* bundle), CellBundleDestroyed);
    DECLARE_SIGNAL(void(TArea* area), AreaCreated);
    DECLARE_SIGNAL(void(TArea* area), AreaDestroyed);
    DECLARE_SIGNAL(void(TArea* area), AreaNodeTagFilterChanged);
    DECLARE_SIGNAL(void(TCellBase* cell), CellDecommissionStarted);
    DECLARE_SIGNAL(void(), CellPeersAssigned);
    DECLARE_SIGNAL(void(), AfterSnapshotLoaded);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TTamedCellManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
