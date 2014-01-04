#include "stdafx.h"
#include "tablet_manager.h"
#include "tablet_slot.h"
#include "automaton.h"
#include "tablet.h"
#include "transaction.h"
#include "transaction_manager.h"
#include "config.h"
#include "store_manager.h"
#include "tablet_cell_controller.h"
#include "dynamic_memory_store.h"
#include "private.h"

#include <core/misc/ring_queue.h>

#include <ytlib/new_table_client/name_table.h>

#include <ytlib/tablet_client/config.h>
#include <ytlib/tablet_client/protocol.h>

#include <server/hydra/hydra_manager.h>
#include <server/hydra/mutation.h>
#include <server/hydra/mutation_context.h>

#include <server/tablet_node/tablet_manager.pb.h>

#include <server/tablet_server/tablet_manager.pb.h>

#include <server/hive/hive_manager.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NTabletNode {

using namespace NConcurrency;
using namespace NHydra;
using namespace NCellNode;
using namespace NTabletClient;
using namespace NTabletNode::NProto;
using namespace NTabletServer::NProto;
using namespace NVersionedTableClient;
using namespace NTransactionClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TTabletManager::TImpl
    : public TTabletAutomatonPart
{
public:
    explicit TImpl(
        TTabletManagerConfigPtr config,
        TTabletSlot* slot,
        TBootstrap* bootstrap)
        : TTabletAutomatonPart(
            slot,
            bootstrap)
        , Config_(config)
    {
        VERIFY_INVOKER_AFFINITY(Slot_->GetAutomatonInvoker(), AutomatonThread);

        Slot_->GetAutomaton()->RegisterPart(this);

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

        RegisterMethod(BIND(&TImpl::HydraMountTablet, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraUnmountTablet, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraFollowerExecuteWrite, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraRotateStore, Unretained(this)));
    }


    void Initialize()
    {
        auto transactionManager = Slot_->GetTransactionManager();
        transactionManager->SubscribeTransactionPrepared(BIND(&TImpl::OnTransactionPrepared, MakeStrong(this)));
        transactionManager->SubscribeTransactionCommitted(BIND(&TImpl::OnTransactionCommitted, MakeStrong(this)));
        transactionManager->SubscribeTransactionAborted(BIND(&TImpl::OnTransactionAborted, MakeStrong(this)));
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


    void Read(
        TTablet* tablet,
        TTimestamp timestamp,
        const Stroka& encodedRequest,
        Stroka* encodedResponse)
    {
        ValidateReadTimestamp(timestamp);

        TProtocolReader reader(encodedRequest);
        TProtocolWriter writer;

        while (ExecuteSingleRead(
            tablet,
            timestamp,
            &reader,
            &writer))
        { }

        *encodedResponse = writer.Finish();
    }

    void Write(
        TTablet* tablet,
        TTransaction* transaction,
        const Stroka& encodedRequest)
    {
        const auto& storeManager = tablet->GetStoreManager();
        const auto& store = storeManager->GetActiveStore();

        TProtocolReader reader(encodedRequest);

        PooledRows_.clear();
        int commandsSucceded = 0;
        try {
            while (ExecuteSingleWrite(
                tablet,
                transaction,
                &reader,
                true,
                &PooledRows_))
            {
                ++commandsSucceded;
            }
        } catch (const std::exception& /*ex*/) {
            // Just break.
        }

        int rowCount = static_cast<int>(PooledRows_.size());

        LOG_DEBUG("Rows prewritten (TransactionId: %s, TabletId: %s, RowCount: %d, CommandsSucceded: %d)",
            ~ToString(transaction->GetId()),
            ~ToString(tablet->GetId()),
            rowCount,
            commandsSucceded);

        store->Lock(static_cast<int>(PooledRows_.size()));
        for (auto row : PooledRows_) {
            PrewrittenRows_.push(TDynamicRowRef(store.Get(), row));
        }

        TReqExecuteWrite hydraRequest;
        ToProto(hydraRequest.mutable_transaction_id(), transaction->GetId());
        ToProto(hydraRequest.mutable_tablet_id(), tablet->GetId());
        hydraRequest.set_commands_succeded(commandsSucceded);
        hydraRequest.set_encoded_request(encodedRequest);
        CreateMutation(Slot_->GetHydraManager(), hydraRequest)
            ->SetAction(BIND(&TImpl::HydraLeaderExecuteWrite, MakeStrong(this), rowCount))
            ->Commit();

        CheckForRotation(storeManager);
    }


    DECLARE_ENTITY_MAP_ACCESSORS(Tablet, TTablet, TTabletId);

private:
    TTabletManagerConfigPtr Config_;

    NHydra::TEntityMap<TTabletId, TTablet> TabletMap_;

    std::vector<TDynamicRow> PooledRows_;
    TRingQueue<TDynamicRowRef> PrewrittenRows_;


    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    void ValidateReadTimestamp(TTimestamp timestamp)
    {
        if (timestamp != LastCommittedTimestamp &&
            (timestamp < MinTimestamp || timestamp > MaxTimestamp))
        {
            THROW_ERROR_EXCEPTION("Invalid timestamp %" PRIu64, timestamp);
        }
    }

    
    void SaveKeys(TSaveContext& context) const
    {
        TabletMap_.SaveKeys(context);
    }

    void SaveValues(TSaveContext& context) const
    {
        TabletMap_.SaveValues(context);
    }

    void LoadKeys(TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TabletMap_.LoadKeys(context);
    }

    void LoadValues(TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TabletMap_.LoadValues(context);
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
        TabletMap_.Clear();
    }


    virtual void OnStartLeading()
    {
        YCHECK(PrewrittenRows_.empty());
    }

    virtual void OnStopLeading()
    {
        while (!PrewrittenRows_.empty()) {
            auto rowRef = PrewrittenRows_.front();
            PrewrittenRows_.pop();
            rowRef.Store->GetTablet()->GetStoreManager()->AbortRow(rowRef);
        }

        for (const auto& pair : TabletMap_) {
            auto* tablet = pair.second;
            tablet->GetStoreManager()->ResetRotationScheduled();
        }
    }


    void HydraMountTablet(const TReqMountTablet& request)
    {
        auto id = FromProto<TTabletId>(request.tablet_id());
        auto schema = FromProto<TTableSchema>(request.schema());
        auto keyColumns = FromProto<Stroka>(request.key_columns().names());

        auto* tablet = new TTablet(
            id,
            schema,
            keyColumns,
            New<TTableMountConfig>());
        TabletMap_.Insert(id, tablet);

        auto storeManager = New<TStoreManager>(Config_, tablet);
        tablet->SetStoreManager(std::move(storeManager));

        auto hiveManager = Slot_->GetHiveManager();

        {
            TReqOnTabletMounted req;
            ToProto(req.mutable_tablet_id(), id);
            hiveManager->PostMessage(Slot_->GetMasterMailbox(), req);
        }

        LOG_INFO_UNLESS(IsRecovery(), "Tablet mounted (TabletId: %s)",
            ~ToString(id));
    }

    void HydraUnmountTablet(const TReqUnmountTablet& request)
    {
        auto id = FromProto<TTabletId>(request.tablet_id());
        auto* tablet = FindTablet(id);
        if (!tablet)
            return;
        
        // TODO(babenko): flush data
        // TODO(babenko): purge prewritten locks

        TabletMap_.Remove(id);

        auto hiveManager = Slot_->GetHiveManager();

        {
            TReqOnTabletUnmounted req;
            ToProto(req.mutable_tablet_id(), id);
            hiveManager->PostMessage(Slot_->GetMasterMailbox(), req);
        }

        LOG_INFO_UNLESS(IsRecovery(), "Tablet unmounted (TabletId: %s)",
            ~ToString(id));
    }


    void HydraLeaderExecuteWrite(int rowCount)
    {
        for (int index = 0; index < rowCount; ++index) {
            YASSERT(!PrewrittenRows_.empty());
            auto rowRef = PrewrittenRows_.front();
            PrewrittenRows_.pop();
            rowRef.Store->GetTablet()->GetStoreManager()->ConfirmRow(rowRef);
        }

        LOG_DEBUG_UNLESS(IsRecovery(), "Rows confirmed (RowCount: %d)",
            rowCount);
    }

    void HydraFollowerExecuteWrite(const TReqExecuteWrite& request)
    {
        auto transactionId = FromProto<TTransactionId>(request.transaction_id());
        auto transactionManager = Slot_->GetTransactionManager();
        auto* transaction = transactionManager->GetTransaction(transactionId);

        auto tabletId = FromProto<TTabletId>(request.tablet_id());
        auto* tablet = GetTablet(tabletId);

        int commandsSucceded = request.commands_succeded();

        TProtocolReader reader(request.encoded_request());

        try {
            for (int index = 0; index < commandsSucceded; ++index) {
                YCHECK(ExecuteSingleWrite(
                    tablet,
                    transaction,
                    &reader,
                    false,
                    nullptr));
            }
        } catch (const std::exception& ex) {
            LOG_FATAL(ex, "Error executing writes");
        }

        LOG_DEBUG_UNLESS(IsRecovery(), "Rows written (TransactionId: %s, TabletId: %s, CommandsSucceded: %d)",
            ~ToString(transaction->GetId()),
            ~ToString(tablet->GetId()),
            commandsSucceded);
    }


    void HydraRotateStore(const TReqRotateStore& request)
    {
        auto tabletId = FromProto<TTabletId>(request.tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet)
            return;

        tablet->GetStoreManager()->Rotate();
    }


    void OnTransactionPrepared(TTransaction* transaction)
    {
        if (!transaction->LockedRows().empty()) {
            for (const auto& rowRef : transaction->LockedRows()) {
                rowRef.Store->GetTablet()->GetStoreManager()->PrepareRow(rowRef);
            }

            LOG_DEBUG_UNLESS(IsRecovery(), "Locked rows prepared (TransactionId: %s, RowCount: %" PRISZT ")",
                ~ToString(transaction->GetId()),
                transaction->LockedRows().size());
        }
    }

    void OnTransactionCommitted(TTransaction* transaction)
    {
        if (!transaction->LockedRows().empty()) {
            for (const auto& rowRef : transaction->LockedRows()) {
                rowRef.Store->GetTablet()->GetStoreManager()->CommitRow(rowRef);
            }

            LOG_DEBUG_UNLESS(IsRecovery(), "Locked rows committed (TransactionId: %s, RowCount: %" PRISZT ")",
                ~ToString(transaction->GetId()),
                transaction->LockedRows().size());
        }
    }

    void OnTransactionAborted(TTransaction* transaction)
    {
        if (!transaction->LockedRows().empty()) {
            for (const auto& rowRef : transaction->LockedRows()) {
                rowRef.Store->GetTablet()->GetStoreManager()->AbortRow(rowRef);
            }

            LOG_DEBUG_UNLESS(IsRecovery(), "Locked rows aborted (TransactionId: %s, RowCount: %" PRISZT ")",
                ~ToString(transaction->GetId()),
                transaction->LockedRows().size());
        }
    }


    bool ExecuteSingleRead(
        TTablet* tablet,
        TTimestamp timestamp,
        TProtocolReader* reader,
        TProtocolWriter* writer)
    {
        auto command = reader->ReadCommand();
        if (command == EProtocolCommand::End) {
            return false;
        }

        const auto& storeManager = tablet->GetStoreManager();

        switch (command) {
            case EProtocolCommand::LookupRow:
                storeManager->LookupRow(
                    timestamp,
                    reader,
                    writer);
                break;

            default:
                THROW_ERROR_EXCEPTION("Unknown read command %s",
                    ~command.ToString());
        }

        return true;
    }

    bool ExecuteSingleWrite(
        TTablet* tablet,
        TTransaction* transaction,
        TProtocolReader* reader,
        bool prewrite,
        std::vector<TDynamicRow>* lockedRows)
    {
        auto command = reader->ReadCommand();
        if (command == EProtocolCommand::End) {
            return false;
        }
            
        const auto& storeManager = tablet->GetStoreManager();

        switch (command) {
            case EProtocolCommand::WriteRow: {
                auto row = reader->ReadUnversionedRow();
                storeManager->WriteRow(
                    transaction,
                    row,
                    prewrite,
                    lockedRows);
                break;
            }

            case EProtocolCommand::DeleteRow: {
                auto key = reader->ReadUnversionedRow();
                storeManager->DeleteRow(
                    transaction,
                    key,
                    prewrite,
                    lockedRows);
                break;
            }

            default:
                THROW_ERROR_EXCEPTION("Unknown write command %s",
                    ~command.ToString());
        }

        return true;
    }


    void CheckForRotation(const TStoreManagerPtr& storeManager)
    {
        if (!IsLeader())
            return;

        if (!storeManager->IsRotationNeeded())
            return;

        storeManager->SetRotationScheduled();

        TReqRotateStore request;
        ToProto(request.mutable_tablet_id(), storeManager->GetTablet()->GetId());
        CreateMutation(Slot_->GetHydraManager(), request)
            ->Commit();
    }

};

DEFINE_ENTITY_MAP_ACCESSORS(TTabletManager::TImpl, Tablet, TTablet, TTabletId, TabletMap_)

///////////////////////////////////////////////////////////////////////////////

TTabletManager::TTabletManager(
    TTabletManagerConfigPtr config,
    TTabletSlot* slot,
    TBootstrap* bootstrap)
    : Impl(New<TImpl>(
        config,
        slot,
        bootstrap))
{ }

TTabletManager::~TTabletManager()
{ }

TTablet* TTabletManager::GetTabletOrThrow(const TTabletId& id)
{
    return Impl->GetTabletOrThrow(id);
}

void TTabletManager::Initialize()
{
    Impl->Initialize();
}

void TTabletManager::Read(
    TTablet* tablet,
    TTimestamp timestamp,
    const Stroka& encodedRequest,
    Stroka* encodedResponse)
{
    Impl->Read(
        tablet,
        timestamp,
        encodedRequest,
        encodedResponse);
}

void TTabletManager::Write(
    TTablet* tablet,
    TTransaction* transaction,
    const Stroka& encodedRequest)
{
    Impl->Write(
        tablet,
        transaction,
        encodedRequest);
}

DELEGATE_ENTITY_MAP_ACCESSORS(TTabletManager, Tablet, TTablet, TTabletId, *Impl)

///////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
