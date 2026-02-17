#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/cell_server/public.h>

#include <yt/yt/server/master/table_server/public.h>

#include <yt/yt/server/lib/hydra/entity_map.h>

#include <yt/yt/client/table_client/public.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

struct ITabletActionManagerHost
    : public virtual TRefCounted
{
    virtual const TDynamicTabletManagerConfigPtr& GetDynamicConfig() const = 0;

    virtual TTableSettings GetTableSettings(NTableServer::TTableNode* table) const = 0;

    virtual void ValidateBundleUsePermission(TTabletCellBundle* bundle) const = 0;

    virtual TTabletBase* GetTabletOrThrow(TTabletId id) = 0;
    virtual TTabletCell* GetTabletCellOrThrow(TTabletCellId id) = 0;

    virtual void AllocateAuxiliaryServant(
        TTablet* tablet,
        TTabletCell* cell,
        const TSerializedTableSettings& serializedTableSettings) = 0;
    virtual void DeallocateAuxiliaryServant(TTabletBase* tablet) = 0;

    virtual int TrySyncReshard(
        TTabletOwnerBase* owner,
        int firstTabletIndex,
        int lastTabletIndex,
        int newTabletCount,
        const std::vector<NTableClient::TLegacyOwningKey>& pivotKeys) = 0;

    virtual void UnmountTablet(
        TTabletBase* tablet,
        bool force,
        bool onDestroy,
        TUnmountTabletOptions options) = 0;

    virtual void DoFreezeTablet(TTabletBase* tablet) = 0;
    virtual void DoUnfreezeTablet(TTabletBase* tablet) = 0;

    virtual void RequestProvisionalFlush(TTabletBase* tablet) = 0;

    virtual void SendReshardRedirectionHint(
        TTabletCellId cellId,
        const std::vector<TTabletBaseRawPtr>& newTablets,
        const std::vector<TTabletBaseRawPtr>& oldTablets,
        const std::vector<NHydra::TRevision>& oldTabletMountRevisions) = 0;

    virtual void DoMountTablets(
        TTabletOwnerBase* table,
        const TSerializedTabletOwnerSettings& serializedTableSettings,
        const std::vector<std::pair<TTabletBase*, TTabletCell*>>& assignment,
        bool freeze,
        bool useRetainedPreloadedChunks = false,
        NTransactionClient::TTimestamp mountTimestamp = NTransactionClient::NullTimestamp) = 0;

    virtual void MountTablet(
        TTabletBase* tablet,
        TTabletCell* cell,
        bool freeze) = 0;

    virtual void StartSmoothMovement(TTabletBase* tablet, TTabletCell* cell) = 0;
    virtual void AbortSmoothMovement(TTabletBase* tablet) = 0;

    virtual THashSet<NCellServer::TCellBundle*> ListHealthyTabletCellBundles() = 0;
};

DEFINE_REFCOUNTED_TYPE(ITabletActionManagerHost)

////////////////////////////////////////////////////////////////////////////////

struct ITabletActionManager
    : public virtual TRefCounted
{
    virtual void Initialize() = 0;

    virtual void Start() = 0;
    virtual void Stop() = 0;

    virtual void Reconfigure(TTabletActionManagerMasterConfigPtr config) = 0;

    virtual void OnAfterCellManagerSnapshotLoaded() = 0;

    virtual TTabletAction* CreateTabletAction(
        NObjectClient::TObjectId hintId,
        ETabletActionKind kind,
        std::vector<TTabletBaseRawPtr> tablets,
        std::vector<TTabletCellRawPtr> cells,
        std::vector<NTableClient::TLegacyOwningKey> pivotKeys,
        const std::optional<int>& tabletCount,
        TCreateTabletActionOptions options) = 0;

    virtual void CreateOrphanedTabletAction(TTabletBase* tablet, bool freeze) = 0;

    virtual void ZombifyTabletAction(TTabletAction* action) = 0;

    virtual void TouchAffectedTabletActions(
        TTabletOwnerBase* table,
        int firstTabletIndex,
        int lastTabletIndex,
        TStringBuf request) = 0;

    virtual void OnTabletActionStateChanged(TTabletAction* action) = 0;

    virtual void UnbindTabletActionFromCells(TTabletAction* action) = 0;
    virtual void UnbindTabletAction(TTabletAction* action) = 0;
    virtual void OnTabletActionDisturbed(TTabletAction* action, const TError& error) = 0;
    virtual void OnTabletActionTabletsTouched(
        TTabletAction* action,
        const THashSet<TTabletBase*>& touchedTablets,
        const TError& error) = 0;

    DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS(TabletAction, TTabletAction);

private:
    // COMPAT(ifsmirnov): EMasterReign::TabletActionManager
    friend class TTabletManager;
    virtual NHydra::TEntityMap<TTabletAction>& MutableTabletActionMapCompat() = 0;
};

DEFINE_REFCOUNTED_TYPE(ITabletActionManager)

////////////////////////////////////////////////////////////////////////////////

ITabletActionManagerPtr CreateTabletActionManager(
    NCellMaster::TBootstrap* bootstrap,
    ITabletActionManagerHostPtr host,
    NHydra::ISimpleHydraManagerPtr hydraManager,
    IInvokerPtr automatonInvoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
