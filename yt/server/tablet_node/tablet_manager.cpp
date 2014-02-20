#include "stdafx.h"
#include "tablet_manager.h"
#include "tablet_slot.h"
#include "automaton.h"
#include "tablet.h"
#include "partition.h"
#include "transaction.h"
#include "transaction_manager.h"
#include "config.h"
#include "store_manager.h"
#include "tablet_cell_controller.h"
#include "dynamic_memory_store.h"
#include "chunk_store.h"
#include "store_flusher.h"
#include "private.h"

#include <core/misc/ring_queue.h>
#include <core/misc/string.h>

#include <core/ytree/fluent.h>

#include <ytlib/new_table_client/name_table.h>

#include <ytlib/tablet_client/config.h>
#include <ytlib/tablet_client/wire_protocol.h>

#include <ytlib/chunk_client/block_cache.h>

#include <ytlib/object_client/helpers.h>

#include <server/hydra/hydra_manager.h>
#include <server/hydra/mutation.h>
#include <server/hydra/mutation_context.h>

#include <server/tablet_node/tablet_manager.pb.h>

#include <server/tablet_server/tablet_manager.pb.h>

#include <server/hive/hive_manager.h>

#include <server/data_node/block_store.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NTabletNode {

using namespace NConcurrency;
using namespace NYson;
using namespace NYTree;
using namespace NHydra;
using namespace NCellNode;
using namespace NTabletClient;
using namespace NTabletNode::NProto;
using namespace NTabletServer::NProto;
using namespace NVersionedTableClient;
using namespace NTransactionClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NObjectClient;

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
        RegisterMethod(BIND(&TImpl::HydraSetTabletState, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraFollowerExecuteWrite, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraRotateStore, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraCommitFlushedChunk, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraOnTabletStoresUpdated, Unretained(this)));
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
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* tablet = FindTablet(id);
        if (!tablet) {
            THROW_ERROR_EXCEPTION("No such tablet %s",
                ~ToString(id));
        }
        return tablet;
    }

    void ValidateTabletMounted(TTablet* tablet)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (tablet->GetState() != ETabletState::Mounted) {
            THROW_ERROR_EXCEPTION("Tablet %s is not in \"mounted\" state",
                ~ToString(tablet->GetId()));
        }
    }


    void SetStoreFailed(TTablet* tablet, IStorePtr store, EStoreState state)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        store->SetState(state);

        auto this_ = MakeStrong(this);
        auto callback = BIND([this, this_, store] () {
            VERIFY_THREAD_AFFINITY(AutomatonThread);

            store->SetState(store->GetPersistentState());
        }).Via(tablet->GetEpochAutomatonInvoker());

        TDelayedExecutor::Submit(callback, Config_->StoreErrorBackoffTime);
    }


    void Read(
        TTablet* tablet,
        TTimestamp timestamp,
        const Stroka& encodedRequest,
        Stroka* encodedResponse)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        ValidateTabletMounted(tablet);
        ValidateReadTimestamp(timestamp);

        TWireProtocolReader reader(encodedRequest);
        TWireProtocolWriter writer;

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
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        ValidateTabletMounted(tablet);

        const auto& store = tablet->GetActiveStore();

        TWireProtocolReader reader(encodedRequest);

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

        if (IsLeader()) {
            CheckIfRotationNeeded(tablet);
        }
    }


    IStorePtr CreateStore(TTablet* tablet, const TStoreId& storeId)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        switch (TypeFromId(storeId)) {
            case EObjectType::Chunk:
            case EObjectType::ErasureChunk:
                return CreateChunkStore(tablet, storeId, MinKey(), MaxKey());

            case EObjectType::DynamicMemoryTabletStore:
                return CreateDynamicMemoryStore(tablet, storeId);

            default:
                YUNREACHABLE();
        }
    }


    void BuildOrchidYson(IYsonConsumer* consumer)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        BuildYsonFluently(consumer)
            .DoMapFor(TabletMap_, [&] (TFluentMap fluent, const std::pair<TTabletId, TTablet*>& pair) {
                auto* tablet = pair.second;
                fluent
                    .Item(ToString(tablet->GetId()))
                    .Do(BIND(&TImpl::BuildTabletOrchidYson, Unretained(this), tablet));
            });
    }


    DECLARE_ENTITY_MAP_ACCESSORS(Tablet, TTablet, TTabletId);

private:
    TTabletManagerConfigPtr Config_;

    NHydra::TEntityMap<TTabletId, TTablet> TabletMap_;
    yhash_set<TTablet*> UnmountingTablets_;

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

    virtual void OnAfterSnapshotLoaded() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        for (const auto& pair : TabletMap_) {
            auto* tablet = pair.second;
            InitializeTablet(tablet);
            if (tablet->GetState() >= ETabletState::Unmounting) {
                YCHECK(UnmountingTablets_.insert(tablet).second);
            }
        }
    }


    virtual void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        DoClear();
    }

    void DoClear()
    {
        for (const auto& pair : TabletMap_) {
            auto* tablet = pair.second;
            StopTablet(tablet);
        }

        TabletMap_.Clear();
        UnmountingTablets_.clear();
    }


    virtual void OnLeaderRecoveryComplete() override
    {
        YCHECK(PrewrittenRows_.empty());

        for (const auto& pair : TabletMap_) {
            auto* tablet = pair.second;
            StartTablet(tablet);
            CheckIfFullyUnlocked(tablet);
            CheckIfAllStoresFlushed(tablet);
        }
    }

    virtual void OnStopLeading() override
    {
        while (!PrewrittenRows_.empty()) {
            auto rowRef = PrewrittenRows_.front();
            PrewrittenRows_.pop();
            rowRef.Store->GetTablet()->GetStoreManager()->AbortRow(rowRef);
        }

        for (const auto& pair : TabletMap_) {
            auto* tablet = pair.second;
            StopTablet(tablet);
        }
    }


    virtual void OnStartFollowing() override
    {
        YCHECK(PrewrittenRows_.empty());

        for (const auto& pair : TabletMap_) {
            auto* tablet = pair.second;
            StartTablet(tablet);
        }
    }

    virtual void OnStopFollowing() override
    {
        YCHECK(PrewrittenRows_.empty());

        for (const auto& pair : TabletMap_) {
            auto* tablet = pair.second;
            StopTablet(tablet);
        }
    }


    void HydraMountTablet(const TReqMountTablet& request)
    {
        auto tabletId = FromProto<TTabletId>(request.tablet_id());
        auto schema = FromProto<TTableSchema>(request.schema());
        auto keyColumns = FromProto<Stroka>(request.key_columns().names());
        auto pivotKey = FromProto<TOwningKey>(request.pivot_key());
        auto nextPivotKey = FromProto<TOwningKey>(request.next_pivot_key());
        auto mountConfig = ConvertTo<TTableMountConfigPtr>(TYsonString(request.mount_config()));

        auto* tablet = new TTablet(
            mountConfig,
            tabletId,
            Slot_,
            schema,
            keyColumns,
            pivotKey,
            nextPivotKey);
        tablet->AddPartition(pivotKey);
        InitializeTablet(tablet);
        tablet->GetStoreManager()->CreateActiveStore();
        tablet->SetState(ETabletState::Mounted);
        TabletMap_.Insert(tabletId, tablet);

        for (const auto& descriptor : request.chunk_stores()) {
            auto store = CreateChunkStore(tablet, descriptor);
            tablet->AddStore(std::move(store));
        }

        {
            TRspMountTablet response;
            ToProto(response.mutable_tablet_id(), tabletId);
            PostMasterMutation(response);
        }
    
        if (!IsRecovery()) {
            StartTablet(tablet);
        }

        LOG_INFO_UNLESS(IsRecovery(), "Tablet mounted (TabletId: %s, ChunkCount: %d)",
            ~ToString(tabletId),
            request.chunk_stores_size());
    }

    void HydraSetTabletState(const TReqSetTabletState& request)
    {
        auto tabletId = FromProto<TTabletId>(request.tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet)
            return;

        auto requestedState = ETabletState(request.state());

        switch (requestedState) {
            case ETabletState::Unmounting: {
                if (tablet->GetState() != ETabletState::Mounted) {
                    LOG_INFO_UNLESS(IsRecovery(), "Requested to unmount a tablet in %s state, ignored (TabletId: %s)",
                        ~FormatEnum(tablet->GetState()).Quote(),
                        ~ToString(tabletId));
                    return;
                }

                LOG_INFO_UNLESS(IsRecovery(), "Unmounting tablet (TabletId: %s)",
                    ~ToString(tabletId));
                // Just a formality.
                YCHECK(tablet->GetState() == ETabletState::Mounted);
                tablet->SetState(ETabletState::Unmounting);
                YCHECK(UnmountingTablets_.insert(tablet).second);

                LOG_INFO_UNLESS(IsRecovery(), "Waiting for all tablet locks to be released (TabletId: %s)",
                    ~ToString(tabletId));
                YCHECK(tablet->GetState() == ETabletState::Unmounting);
                tablet->SetState(ETabletState::WaitingForLocks);

                if (IsLeader()) {
                    CheckIfFullyUnlocked(tablet);
                }
                break;
            }

            case ETabletState::RotatingStore: {
                // Just a formality.
                YCHECK(tablet->GetState() == ETabletState::WaitingForLocks);
                tablet->SetState(ETabletState::RotatingStore);
                // NB: Flush requests for all other stores must already be on their way.
                RotateStore(tablet, false);

                YCHECK(tablet->GetState() == ETabletState::RotatingStore);
                tablet->SetState(ETabletState::FlushingStores);

                LOG_INFO_UNLESS(IsRecovery(), "Waiting for all tablet stores to be flushed (TabletId: %s)",
                    ~ToString(tabletId));

                if (IsLeader()) {
                    CheckIfAllStoresFlushed(tablet);
                }
                break;
            }

            case ETabletState::Unmounted: {
                // Not really necessary, just for fun.
                YCHECK(tablet->GetState() == ETabletState::FlushingStores);
                tablet->SetState(ETabletState::Unmounted);

                LOG_INFO_UNLESS(IsRecovery(), "Tablet unmounted (TabletId: %s)",
                    ~ToString(tabletId));

                if (!IsRecovery()) {
                    StopTablet(tablet);
                }

                TabletMap_.Remove(tabletId);
                YCHECK(UnmountingTablets_.erase(tablet) == 1);

                {
                    TRspUnmountTablet response;
                    ToProto(response.mutable_tablet_id(), tabletId);
                    PostMasterMutation(response);
                }
                break;
            }

            default:
                YUNREACHABLE();
        }
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

        TWireProtocolReader reader(request.encoded_request());

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

        RotateStore(tablet, true);
    }

    void HydraCommitFlushedChunk(const TReqCommitFlushedChunks& flushRequest)
    {
        auto tabletId = FromProto<TTabletId>(flushRequest.tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet)
            return;

        auto storeId = FromProto<TStoreId>(flushRequest.store_id());
        auto store = tablet->FindStore(storeId);
        if (!store)
            return;

        auto state = store->GetState();
        if (state != EStoreState::Flushing && state != EStoreState::PassiveDynamic) {
            LOG_INFO_UNLESS(IsRecovery(), "Requested to commit a flushed chunk for store in %s state, ignored (TabletId: %s, StoreId: %s)",
                ~FormatEnum(state).Quote(),
                ~ToString(tabletId),
                ~ToString(storeId));
            return;
        }

        LOG_INFO_UNLESS(IsRecovery(), "Committing flushed chunks (TabletId: %s, StoreId: %s, ChunkCount: %d)",
            ~ToString(tabletId),
            ~ToString(storeId),
            flushRequest.chunks_size());

        store->SetState(EStoreState::FlushCommitting);

        TReqUpdateTabletStores updateRequest;
        ToProto(updateRequest.mutable_tablet_id(), tabletId);
        updateRequest.mutable_stores_to_add()->MergeFrom(flushRequest.chunks());
        {
            auto* descriptor = updateRequest.add_stores_to_remove();
            ToProto(descriptor->mutable_store_id(), storeId);
        }
        auto* slot = tablet->GetSlot();
        auto hiveManager = slot->GetHiveManager();
        hiveManager->PostMessage(slot->GetMasterMailbox(), updateRequest);
    }

    void HydraOnTabletStoresUpdated(const TRspUpdateTabletStores& response)
    {
        auto tabletId = FromProto<TTabletId>(response.tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet)
            return;

        if (response.has_error()) {
            auto error = FromProto(response.error());
            LOG_WARNING(error, "Error updating tablet stores (TabletId: %s)",
                ~ToString(tabletId));

            for (const auto& descriptor : response.stores_to_remove()) {
                auto storeId = FromProto<TStoreId>(descriptor.store_id());
                auto store = tablet->GetStore(storeId);
                switch (store->GetState()) {
                    case EStoreState::FlushCommitting:
                        SetStoreFailed(tablet, store, EStoreState::FlushFailed);
                        break;
                    default:
                        YUNREACHABLE();
                }
            }
        } else {
            std::vector<TStoreId> addedStoreIds;
            for (const auto& descriptor : response.stores_to_add()) {
                auto store = CreateChunkStore(tablet, descriptor);
                tablet->AddStore(store);
            }

            std::vector<TStoreId> removedStoreIds;
            for (const auto& descriptor : response.stores_to_remove()) {
                auto storeId = FromProto<TStoreId>(descriptor.store_id());
                tablet->RemoveStore(storeId);
            }

            LOG_INFO_UNLESS(IsRecovery(), "Tablet stores updated successfully (TabletId: %s, AddedStoreIds: [%s], RemovedStoreIds: [%s])",
                ~ToString(tabletId),
                ~JoinToString(addedStoreIds),
                ~JoinToString(removedStoreIds));

            if (IsLeader()) {
                CheckIfAllStoresFlushed(tablet);
            }
        }
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
        if (transaction->LockedRows().empty())
            return;

        for (const auto& rowRef : transaction->LockedRows()) {
            rowRef.Store->GetTablet()->GetStoreManager()->CommitRow(rowRef);
        }

        LOG_DEBUG_UNLESS(IsRecovery(), "Locked rows committed (TransactionId: %s, RowCount: %" PRISZT ")",
            ~ToString(transaction->GetId()),
            transaction->LockedRows().size());

        OnTransactionFinished(transaction);
    }

    void OnTransactionAborted(TTransaction* transaction)
    {
        if (transaction->LockedRows().empty())
            return;

        for (const auto& rowRef : transaction->LockedRows()) {
            rowRef.Store->GetTablet()->GetStoreManager()->AbortRow(rowRef);
        }

        LOG_DEBUG_UNLESS(IsRecovery(), "Locked rows aborted (TransactionId: %s, RowCount: %" PRISZT ")",
            ~ToString(transaction->GetId()),
            transaction->LockedRows().size());

        OnTransactionFinished(transaction);
    }

    void OnTransactionFinished(TTransaction* /*transaction*/)
    {
        if (IsLeader()) {
            for (auto* tablet : UnmountingTablets_) {
                CheckIfFullyUnlocked(tablet);
            }
        }
    }


    bool ExecuteSingleRead(
        TTablet* tablet,
        TTimestamp timestamp,
        TWireProtocolReader* reader,
        TWireProtocolWriter* writer)
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
                    ~ToString(command));
        }

        return true;
    }

    bool ExecuteSingleWrite(
        TTablet* tablet,
        TTransaction* transaction,
        TWireProtocolReader* reader,
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
                    ~ToString(command));
        }

        return true;
    }


    void CheckIfRotationNeeded(TTablet* tablet)
    {
        const auto& storeManager = tablet->GetStoreManager();
        if (!storeManager->IsRotationNeeded())
            return;

        storeManager->SetRotationScheduled();

        TReqRotateStore request;
        ToProto(request.mutable_tablet_id(), storeManager->GetTablet()->GetId());
        PostTabletMutation(request);
    }

    void CheckIfFullyUnlocked(TTablet* tablet)
    {
        if (tablet->GetState() != ETabletState::WaitingForLocks)
            return;

        if (tablet->GetStoreManager()->HasActiveLocks())
            return;

        LOG_INFO("All tablet locks released (TabletId: %s)",
            ~ToString(tablet->GetId()));

        TReqSetTabletState request;
        ToProto(request.mutable_tablet_id(), tablet->GetId());
        request.set_state(ETabletState::RotatingStore);
        PostTabletMutation(request);
    }

    void CheckIfAllStoresFlushed(TTablet* tablet)
    {
        if (tablet->GetState() != ETabletState::FlushingStores)
            return;

        if (tablet->GetStoreManager()->HasUnflushedStores())
            return;

        LOG_INFO("All tablet stores are flushed (TabletId: %s)",
            ~ToString(tablet->GetId()));

        TReqSetTabletState request;
        ToProto(request.mutable_tablet_id(), tablet->GetId());
        request.set_state(ETabletState::Unmounted);
        PostTabletMutation(request);
    }


    void RotateStore(TTablet* tablet, bool createNew)
    {
        auto storeManager = tablet->GetStoreManager();
        storeManager->Rotate(createNew);
    }


    void PostTabletMutation(const ::google::protobuf::MessageLite& message)
    {
        auto mutation = CreateMutation(Slot_->GetHydraManager(), message);
        Slot_->GetEpochAutomatonInvoker()->Invoke(BIND(
            IgnoreResult(&TMutation::Commit),
            mutation));
    }

    void PostMasterMutation(const ::google::protobuf::MessageLite& message)
    {
        auto hiveManager = Slot_->GetHiveManager();
        hiveManager->PostMessage(Slot_->GetMasterMailbox(), message);
    }


    void InitializeTablet(TTablet* tablet)
    {
        auto storeManager = New<TStoreManager>(Config_, tablet);
        tablet->SetStoreManager(storeManager);
    }


    void StartTablet(TTablet* tablet)
    {
        auto context = New<TCancelableContext>();
        tablet->SetCancelableContext(context);

        auto hydraManager = Slot_->GetHydraManager();
        tablet->SetEpochAutomatonInvoker(context->CreateInvoker(Slot_->GetEpochAutomatonInvoker()));
    }

    void StopTablet(TTablet* tablet)
    {
        for (const auto& pair : tablet->Stores()) {
            const auto& store = pair.second;
            store->SetState(store->GetPersistentState());
        }

        auto context = tablet->GetCancelableContext();
        if (context) {
            context->Cancel();
            tablet->SetCancelableContext(nullptr);
        }

        tablet->SetEpochAutomatonInvoker(nullptr);
        
        tablet->GetStoreManager()->ResetRotationScheduled();
    }


    IStorePtr CreateChunkStore(
        TTablet* tablet,
        const NTabletServer::NProto::TAddStoreDescriptor& descriptor)
    {
        auto chunkId = FromProto<TChunkId>(descriptor.store_id());
        auto minKey = FromProto<TOwningKey>(descriptor.min_key());
        auto maxKey = FromProto<TOwningKey>(descriptor.max_key());
        return CreateChunkStore(
            tablet,
            chunkId,
            std::move(minKey),
            std::move(maxKey));
    }

    IStorePtr CreateChunkStore(
        TTablet* tablet,
        const TChunkId& chunkId,
        TOwningKey minKey,
        TOwningKey maxKey)
    {
        return New<TChunkStore>(
            Config_,
            chunkId,
            tablet,
            std::move(minKey),
            std::move(maxKey),
            Bootstrap_->GetBlockStore()->GetBlockCache(),
            Bootstrap_->GetMasterChannel(),
            Bootstrap_->GetLocalDescriptor());
    }

    IStorePtr CreateDynamicMemoryStore(TTablet* tablet, const TStoreId& storeId)
    {
        return New<TDynamicMemoryStore>(
            Config_,
            storeId,
            tablet);
    }


    void BuildTabletOrchidYson(TTablet* tablet, IYsonConsumer* consumer)
    {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("state").Value(tablet->GetState())
                .Item("pivot_key").Value(tablet->GetPivotKey())
                .Item("next_pivot_key").Value(tablet->GetNextPivotKey())
                .Item("eden").Do(BIND(&TImpl::BuildPartitionOrchidYson, Unretained(this), tablet->GetEden()))
                .Item("partitions").DoListFor(tablet->Partitions(), [&] (TFluentList fluent, const std::unique_ptr<TPartition>& partition) {
                    fluent
                        .Item()
                        .Do(BIND(&TImpl::BuildPartitionOrchidYson, Unretained(this), partition.get()));
                })
            .EndMap();
    }

    void BuildPartitionOrchidYson(TPartition* partition, IYsonConsumer* consumer)
    {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("pivot_key").Value(partition->GetPivotKey())
                .Item("stores").DoMapFor(partition->Stores(), [&] (TFluentMap fluent, const IStorePtr& store) {
                    fluent
                        .Item(ToString(store->GetId()))
                        .Do(BIND(&TImpl::BuildStoreOrchidYson, Unretained(this), store));
                })
            .EndMap();
    }

    void BuildStoreOrchidYson(IStorePtr store, IYsonConsumer* consumer)
    {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("state").Value(store->GetState())
                .Do(BIND(&IStore::BuildOrchidYson, store))
            .EndMap();
    }

};

DEFINE_ENTITY_MAP_ACCESSORS(TTabletManager::TImpl, Tablet, TTablet, TTabletId, TabletMap_)

///////////////////////////////////////////////////////////////////////////////

TTabletManager::TTabletManager(
    TTabletManagerConfigPtr config,
    TTabletSlot* slot,
    TBootstrap* bootstrap)
    : Impl_(New<TImpl>(
        config,
        slot,
        bootstrap))
{ }

TTabletManager::~TTabletManager()
{ }

void TTabletManager::Initialize()
{
    Impl_->Initialize();
}

TTablet* TTabletManager::GetTabletOrThrow(const TTabletId& id)
{
    return Impl_->GetTabletOrThrow(id);
}

void TTabletManager::ValidateTabletMounted(TTablet* tablet)
{
    Impl_->ValidateTabletMounted(tablet);
}

void TTabletManager::SetStoreFailed(TTablet* tablet, IStorePtr store, EStoreState state)
{
    Impl_->SetStoreFailed(tablet, store, state);
}

void TTabletManager::Read(
    TTablet* tablet,
    TTimestamp timestamp,
    const Stroka& encodedRequest,
    Stroka* encodedResponse)
{
    Impl_->Read(
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
    Impl_->Write(
        tablet,
        transaction,
        encodedRequest);
}

IStorePtr TTabletManager::CreateStore(TTablet* tablet, const TStoreId& storeId)
{
    return Impl_->CreateStore(tablet, storeId);
}

void TTabletManager::BuildOrchidYson(IYsonConsumer* consumer)
{
    Impl_->BuildOrchidYson(consumer);
}

DELEGATE_ENTITY_MAP_ACCESSORS(TTabletManager, Tablet, TTablet, TTabletId, *Impl_)

///////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
