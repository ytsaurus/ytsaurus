#include "stdafx.h"
#include "tablet_manager.h"
#include "tablet_slot.h"
#include "slot_automaton.h"
#include "tablet.h"
#include "private.h"

#include <server/hydra/hydra_manager.h>
#include <server/hydra/mutation_context.h>

#include <server/tablet_node/tablet_manager.pb.h>

#include <server/tablet_server/tablet_manager.pb.h>

#include <server/hive/hive_manager.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NTabletNode {

using namespace NHydra;
using namespace NTabletNode::NProto;
using namespace NTabletServer::NProto;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TTabletManager::TImpl
    : public TCompositeAutomatonPart
{
public:
    explicit TImpl(
        TTabletSlot* slot,
        NCellNode::TBootstrap* bootstrap)
        : TCompositeAutomatonPart(
            slot->GetHydraManager(),
            slot->GetAutomaton())
        , Slot(slot)
        , Bootstrap(bootstrap)
    {
        VERIFY_INVOKER_AFFINITY(Slot->GetAutomatonInvoker(), AutomatonThread);

        Slot->GetAutomaton()->RegisterPart(this);

        RegisterLoader(
            "TabletManager.Keys",
            BIND(&TImpl::LoadKeys, MakeStrong(this)));
        RegisterLoader(
            "TabletManager.Values",
            BIND(&TImpl::LoadValues, MakeStrong(this)));

        RegisterSaver(
            ESerializationPriority::Keys,
            "TabletManager.Keys",
            BIND(&TImpl::SaveKeys, MakeStrong(this)));
        RegisterSaver(
            ESerializationPriority::Values,
            "TabletManager.Values",
            BIND(&TImpl::SaveValues, MakeStrong(this)));

        RegisterMethod(BIND(&TImpl::CreateTablet, Unretained(this)));
        RegisterMethod(BIND(&TImpl::RemoveTablet, Unretained(this)));
    }

    DECLARE_ENTITY_MAP_ACCESSORS(Tablet, TTablet, TTabletId);

private:
    TTabletSlot* Slot;
    NCellNode::TBootstrap* Bootstrap;

    NHydra::TEntityMap<TTabletId, TTablet> TabletMap;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    
    void SaveKeys(TSaveContext& context) const
    {
        TabletMap.SaveKeys(context);
    }

    void SaveValues(TSaveContext& context) const
    {
        TabletMap.SaveValues(context);
    }

    void LoadKeys(TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TabletMap.LoadKeys(context);
    }

    void LoadValues(TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TabletMap.LoadValues(context);
    }

    virtual void OnBeforeSnapshotLoaded() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        DoClear();
    }

    virtual void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        DoClear();
    }

    void DoClear()
    {
        TabletMap.Clear();
    }


    void CreateTablet(const TReqCreateTablet& request)
    {
        auto id = FromProto<TTabletId>(request.tablet_id());
        auto* tablet = new TTablet(id);
        TabletMap.Insert(id, tablet);

        auto hiveManager = Slot->GetHiveManager();

        {
            TReqOnTabletCreated req;
            ToProto(req.mutable_tablet_id(), id);
            hiveManager->PostMessage(Slot->GetMasterMailbox(), req);
        }

        LOG_INFO_UNLESS(IsRecovery(), "Tablet created (TabletId: %s)",
            ~ToString(id));
    }

    void RemoveTablet(const TReqRemoveTablet& request)
    {
        auto id = FromProto<TTabletId>(request.tablet_id());
        auto* tablet = FindTablet(id);
        if (!tablet)
            return;
        
        // TODO(babenko)

        TabletMap.Remove(id);

        auto hiveManager = Slot->GetHiveManager();

        {
            TReqOnTabletRemoved req;
            ToProto(req.mutable_tablet_id(), id);
            hiveManager->PostMessage(Slot->GetMasterMailbox(), req);
        }

        LOG_INFO_UNLESS(IsRecovery(), "Tablet removed (TabletId: %s)",
            ~ToString(id));
    }

};

DEFINE_ENTITY_MAP_ACCESSORS(TTabletManager::TImpl, Tablet, TTablet, TTabletId, TabletMap)

///////////////////////////////////////////////////////////////////////////////

TTabletManager::TTabletManager(
    TTabletSlot* slot,
    NCellNode::TBootstrap* bootstrap)
    : Impl(New<TImpl>(slot, bootstrap))
{ }

TTabletManager::~TTabletManager()
{ }

DELEGATE_ENTITY_MAP_ACCESSORS(TTabletManager, Tablet, TTablet, TTabletId, *Impl)

///////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
