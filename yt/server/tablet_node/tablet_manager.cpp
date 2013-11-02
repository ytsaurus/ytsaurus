#include "stdafx.h"
#include "tablet_manager.h"
#include "tablet_slot.h"
#include "automaton.h"
#include "tablet.h"
#include "transaction.h"
#include "memory_table.h"
#include "private.h"

#include <ytlib/new_table_client/reader.h>

#include <server/hydra/hydra_manager.h>
#include <server/hydra/mutation_context.h>

#include <server/tablet_node/tablet_manager.pb.h>

#include <server/tablet_server/tablet_manager.pb.h>

#include <server/hive/hive_manager.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NTabletNode {

using namespace NHydra;
using namespace NCellNode;
using namespace NTabletNode::NProto;
using namespace NTabletServer::NProto;
using namespace NVersionedTableClient;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TTabletManager::TImpl
    : public TTabletAutomatonPart
{
public:
    explicit TImpl(
        TTabletSlot* slot,
        TBootstrap* bootstrap)
        : TTabletAutomatonPart(
            slot,
            bootstrap)
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

        RegisterMethod(BIND(&TImpl::HydraCreateTablet, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraRemoveTablet, Unretained(this)));
    }

    TTablet* GetTabletOrThrow(const TTabletId& id)
    {
        auto* tablet = FindTablet(id);
        if (!tablet) {
            THROW_ERROR_EXCEPTION("No such tablet %s",
                ~ToString(id));
        }
        return tablet;
    }

    void Write(
        TTablet* tablet,
        TTransaction* transaction,
        IReaderPtr reader)
    {
    }

    DECLARE_ENTITY_MAP_ACCESSORS(Tablet, TTablet, TTabletId);

private:
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


    void HydraCreateTablet(const TReqCreateTablet& request)
    {
        auto id = FromProto<TTabletId>(request.tablet_id());
        auto schema = FromProto<TTableSchema>(request.schema());
        auto keyColumns = FromProto<Stroka>(request.key_columns().names());

        auto* tablet = new TTablet(
            id,
            schema,
            keyColumns);
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

    void HydraRemoveTablet(const TReqRemoveTablet& request)
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
    TBootstrap* bootstrap)
    : Impl(New<TImpl>(
        slot,
        bootstrap))
{ }

TTabletManager::~TTabletManager()
{ }

TTablet* TTabletManager::GetTabletOrThrow(const TTabletId& id)
{
    return Impl->GetTabletOrThrow(id);
}

void TTabletManager::Write(
    TTablet* tablet,
    TTransaction* transaction,
    IReaderPtr reader)
{
    Impl->Write(
        tablet,
        transaction,
        std::move(reader));
}

DELEGATE_ENTITY_MAP_ACCESSORS(TTabletManager, Tablet, TTablet, TTabletId, *Impl)

///////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
