#pragma once

#include "public.h"
#include "config.h"
#include "cell_base.h"
#include "cell_bundle.h"

#include <yt/server/master/cell_master/public.h>

#include <yt/server/master/cypress_server/public.h>

#include <yt/server/lib/hydra/entity_map.h>
#include <yt/server/lib/hydra/mutation.h>

#include <yt/server/master/object_server/public.h>

#include <yt/core/misc/small_vector.h>

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
    TCellBundle* FindCellBundleByName(const TString& name);
    TCellBundle* GetCellBundleByNameOrThrow(const TString& name);
    void RenameCellBundle(TCellBundle* cellBundle, const TString& newName);
    void SetCellBundleNodeTagFilter(TCellBundle* bundle, const TString& formula);
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

    TCellBase* CreateCell(TCellBundle* cellBundle, std::unique_ptr<TCellBase> holder);
    void ZombifyCell(TCellBase* cell);
    void DestroyCell(TCellBase* cell);
    void UpdatePeerCount(TCellBase* cell, std::optional<int> peerCount);

    DECLARE_SIGNAL(void(TCellBundle* bundle), CellBundleCreated);
    DECLARE_SIGNAL(void(TCellBundle* bundle), CellBundleDestroyed);
    DECLARE_SIGNAL(void(TCellBundle* bundle), CellBundleNodeTagFilterChanged);
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
