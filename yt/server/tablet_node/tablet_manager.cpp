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
#include "tablet_slot_manager.h"
#include "dynamic_memory_store.h"
#include "chunk_store.h"
#include "store_flusher.h"
#include "private.h"

#include <core/misc/ring_queue.h>
#include <core/misc/string.h>

#include <core/ytree/fluent.h>

#include <core/compression/codec.h>

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

using namespace NCompression;
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

static const auto& Logger = TabletNodeLogger;

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
        , ChangelogCodec_(GetCodec(Config_->ChangelogCodec))
        , OnStoreMemoryUsageUpdated_(BIND(&TImpl::OnStoreMemoryUsageUpdated, MakeWeak(this)))
    {
        VERIFY_INVOKER_AFFINITY(Slot_->GetAutomatonInvoker(EAutomatonThreadQueue::Write), AutomatonThread);

        Slot_->GetAutomaton()->RegisterPart(this);

        RegisterLoader(
            "TabletManager.Keys",
            BIND(&TImpl::LoadKeys, Unretained(this)));
        RegisterLoader(
            "TabletManager.Values",
            BIND(&TImpl::LoadValues, Unretained(this)));

        RegisterSaver(
            ESerializationPriority::Keys,
            "TabletManager.Keys",
            BIND(&TImpl::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESerializationPriority::Values,
            "TabletManager.Values",
            BIND(&TImpl::SaveValues, Unretained(this)));

        RegisterMethod(BIND(&TImpl::HydraMountTablet, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraUnmountTablet, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraRemountTablet, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraSetTabletState, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraFollowerExecuteWrite, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraRotateStore, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraCommitTabletStoresUpdate, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraOnTabletStoresUpdated, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraSplitPartition, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraMergePartitions, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraUpdatePartitionSampleKeys, Unretained(this)));
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
            THROW_ERROR_EXCEPTION("No such tablet %v",
                id);
        }
        return tablet;
    }

    void ValidateTabletMounted(TTablet* tablet)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (tablet->GetState() != ETabletState::Mounted) {
            THROW_ERROR_EXCEPTION("Tablet %v is not in \"mounted\" state",
                tablet->GetId());
        }
    }


    void BackoffStore(IStorePtr store, EStoreState state)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        store->SetState(state);

        auto this_ = MakeStrong(this);
        auto callback = BIND([this, this_, store] () {
            VERIFY_THREAD_AFFINITY(AutomatonThread);

            store->SetState(store->GetPersistentState());
        }).Via(store->GetTablet()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::Write));

        TDelayedExecutor::Submit(callback, Config_->ErrorBackoffTime);
    }


    std::vector<TSharedRef> Read(
        TTablet* tablet,
        TTimestamp timestamp,
        const TSharedRef& requestData)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        ValidateTabletMounted(tablet);
        ValidateReadTimestamp(timestamp);

        TWireProtocolReader reader(requestData);
        TWireProtocolWriter writer;

        while (ExecuteSingleRead(
            tablet,
            timestamp,
            &reader,
            &writer))
        { }

        return writer.Flush();
    }

    void Write(
        TTablet* tablet,
        TTransaction* transaction,
        const TSharedRef& requestData)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        ValidateMemoryLimit();
        ValidateTabletMounted(tablet);
        if (transaction->GetState() != ETransactionState::Active) {
            transaction->ThrowInvalidState();
        }

        int commandsSucceded = 0;
        int prelockedRowCountBefore = PrelockedRows_.size();
        TError error;
        try {
            TWireProtocolReader reader(requestData);
            while (ExecuteSingleWrite(tablet,  transaction, &reader, true)) {
                ++commandsSucceded;
            }
        } catch (const std::exception& ex) {
            error = ex;
        }

        int prelockedRowCountAfter = PrelockedRows_.size();
        int prelockedRowCountDelta = prelockedRowCountAfter - prelockedRowCountBefore;

        LOG_DEBUG("Rows prelocked (TransactionId: %v, TabletId: %v, RowCount: %v, CommandsSucceded: %v)",
            transaction->GetId(),
            tablet->GetId(),
            prelockedRowCountDelta,
            commandsSucceded);

        auto compressedRequestData = ChangelogCodec_->Compress(requestData);

        TReqExecuteWrite hydraRequest;
        ToProto(hydraRequest.mutable_transaction_id(), transaction->GetId());
        ToProto(hydraRequest.mutable_tablet_id(), tablet->GetId());
        hydraRequest.set_commands_succeded(commandsSucceded);
        hydraRequest.set_codec(ChangelogCodec_->GetId());
        hydraRequest.set_compressed_data(ToString(compressedRequestData));
        CreateMutation(Slot_->GetHydraManager(), hydraRequest)
            ->SetAction(BIND(&TImpl::HydraLeaderExecuteWrite, MakeStrong(this), prelockedRowCountDelta))
            ->Commit();

        if (!error.IsOK()) {
            THROW_ERROR error;
        }
    }


    IStorePtr CreateStore(TTablet* tablet, const TStoreId& storeId)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        switch (TypeFromId(storeId)) {
            case EObjectType::Chunk:
            case EObjectType::ErasureChunk:
                return CreateChunkStore(tablet, storeId, nullptr);

            case EObjectType::DynamicMemoryTabletStore:
                return CreateDynamicMemoryStore(tablet, storeId);

            default:
                YUNREACHABLE();
        }
    }

    void ScheduleStoreRotation(TTablet* tablet)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& storeManager = tablet->GetStoreManager();
        if (!storeManager->IsRotationPossible())
            return;

        storeManager->SetRotationScheduled();

        TReqRotateStore request;
        ToProto(request.mutable_tablet_id(), tablet->GetId());
        PostTabletMutation(request);

        LOG_DEBUG("Store rotation scheduled (TabletId: %v)",
            tablet->GetId());
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

    ICodec* ChangelogCodec_;

    NHydra::TEntityMap<TTabletId, TTablet> TabletMap_;
    yhash_set<TTablet*> UnmountingTablets_;

    TRingQueue<TDynamicRowRef> PrelockedRows_;

    yhash_set<TDynamicMemoryStorePtr> OrphanedStores_;

    TCallback<void(i64)> OnStoreMemoryUsageUpdated_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    void ValidateReadTimestamp(TTimestamp timestamp)
    {
        if (timestamp != LastCommittedTimestamp &&
            (timestamp < MinTimestamp || timestamp > MaxTimestamp))
        {
            THROW_ERROR_EXCEPTION("Invalid timestamp %v", timestamp);
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
            if (tablet->GetState() >= ETabletState::WaitingForLocks) {
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
            StopTabletEpoch(tablet);
        }

        TabletMap_.Clear();
        UnmountingTablets_.clear();
        OrphanedStores_.clear();
    }


    virtual void OnLeaderRecoveryComplete() override
    {
        YCHECK(PrelockedRows_.empty());

        for (const auto& pair : TabletMap_) {
            auto* tablet = pair.second;
            StartTabletEpoch(tablet);
            CheckIfFullyUnlocked(tablet);
            CheckIfAllStoresFlushed(tablet);
        }
    }

    virtual void OnStopLeading() override
    {
        while (!PrelockedRows_.empty()) {
            auto rowRef = PrelockedRows_.front();
            PrelockedRows_.pop();
            if (ValidateAndDiscardRowRef(rowRef)) {
                rowRef.Store->GetTablet()->GetStoreManager()->AbortRow(rowRef);
            }
        }

        auto transactionManager = Slot_->GetTransactionManager();
        for (const auto& pair : transactionManager->Transactions()) {
            auto* transaction = pair.second;
            transaction->PrelockedRows().clear();
        }

        for (const auto& pair : TabletMap_) {
            auto* tablet = pair.second;
            StopTabletEpoch(tablet);
        }

        OrphanedStores_.clear();
    }


    virtual void OnStartFollowing() override
    {
        YCHECK(PrelockedRows_.empty());

        for (const auto& pair : TabletMap_) {
            auto* tablet = pair.second;
            StartTabletEpoch(tablet);
        }
    }

    virtual void OnStopFollowing() override
    {
        YCHECK(PrelockedRows_.empty());

        for (const auto& pair : TabletMap_) {
            auto* tablet = pair.second;
            StopTabletEpoch(tablet);
        }

        OrphanedStores_.clear();
    }


    void HydraMountTablet(const TReqMountTablet& request)
    {
        auto tabletId = FromProto<TTabletId>(request.tablet_id());
        auto schema = FromProto<TTableSchema>(request.schema());
        auto keyColumns = FromProto<TKeyColumns>(request.key_columns());
        auto pivotKey = FromProto<TOwningKey>(request.pivot_key());
        auto nextPivotKey = FromProto<TOwningKey>(request.next_pivot_key());
        auto mountConfig = ConvertTo<TTableMountConfigPtr>(TYsonString(request.mount_config()));
        auto writerOptions = ConvertTo<TTabletWriterOptionsPtr>(TYsonString(request.writer_options()));

        auto* tablet = new TTablet(
            mountConfig,
            writerOptions,
            tabletId,
            Slot_,
            schema,
            keyColumns,
            pivotKey,
            nextPivotKey);
        tablet->CreateInitialPartition();
        InitializeTablet(tablet);

        auto storeManager = tablet->GetStoreManager();
        storeManager->CreateActiveStore();
        StartMemoryUsageTracking(tablet);
        tablet->SetState(ETabletState::Mounted);
        TabletMap_.Insert(tabletId, tablet);

        for (const auto& descriptor : request.chunk_stores()) {
            YCHECK(descriptor.has_chunk_meta());
            auto chunkId = FromProto<TChunkId>(descriptor.store_id());
            auto store = CreateChunkStore(tablet, chunkId, &descriptor.chunk_meta());
            storeManager->AddStore(store);
        }

        {
            TRspMountTablet response;
            ToProto(response.mutable_tablet_id(), tabletId);
            PostMasterMutation(response);
        }
    
        if (!IsRecovery()) {
            StartTabletEpoch(tablet);
        }

        LOG_INFO_UNLESS(IsRecovery(), "Tablet mounted (TabletId: %v, StoreCount: %v, Keys: %v .. %v)",
            tabletId,
            request.chunk_stores_size(),
            pivotKey,
            nextPivotKey);
    }

    void HydraUnmountTablet(const TReqUnmountTablet& request)
    {
        auto tabletId = FromProto<TTabletId>(request.tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet)
            return;

        if (request.force()) {
            LOG_INFO_UNLESS(IsRecovery(), "Tablet is forcefully unmounted (TabletId: %v)",
                tabletId);

            // Just a formality.
            tablet->SetState(ETabletState::Unmounted);

            if (!IsRecovery()) {
                StopTabletEpoch(tablet);
            }

            for (const auto& pair : tablet->Stores()) {
                SetStoreOrphaned(pair.second);
            }

            auto storeManager = tablet->GetStoreManager();
            for (auto store : storeManager->GetLockedStores()) {
                SetStoreOrphaned(store);
            }

            TabletMap_.Remove(tabletId);
            UnmountingTablets_.erase(tablet); // don't check the result
            return;
        }

        if (tablet->GetState() != ETabletState::Mounted) {
            LOG_INFO_UNLESS(IsRecovery(), "Requested to unmount a tablet in %Qlv state, ignored (TabletId: %v)",
                tablet->GetState(),
                tabletId);
            return;
        }

        LOG_INFO_UNLESS(IsRecovery(), "Unmounting tablet (TabletId: %v)",
            tabletId);

        // Just a formality.
        YCHECK(tablet->GetState() == ETabletState::Mounted);
        tablet->SetState(ETabletState::WaitingForLocks);

        YCHECK(UnmountingTablets_.insert(tablet).second);

        LOG_INFO_IF(IsLeader(), "Waiting for all tablet locks to be released (TabletId: %v)",
            tabletId);

        if (IsLeader()) {
            CheckIfFullyUnlocked(tablet);
        }
    }

    void HydraRemountTablet(const TReqRemountTablet& request)
    {
        auto tabletId = FromProto<TTabletId>(request.tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet)
            return;

        auto mountConfig = ConvertTo<TTableMountConfigPtr>(TYsonString(request.mount_config()));
        auto writerOptions = ConvertTo<TTabletWriterOptionsPtr>(TYsonString(request.writer_options()));

        tablet->SetConfig(mountConfig);
        tablet->SetWriterOptions(writerOptions);

        for (const auto& partition : tablet->Partitions()) {
            partition->SetSamplingNeeded(true);
        }

        LOG_INFO_UNLESS(IsRecovery(), "Tablet remounted (TabletId: %v)",
            tabletId);
    }

    void HydraSetTabletState(const TReqSetTabletState& request)
    {
        auto tabletId = FromProto<TTabletId>(request.tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet)
            return;

        auto requestedState = ETabletState(request.state());

        switch (requestedState) {
            case ETabletState::Flushing: {
                tablet->SetState(ETabletState::Flushing);

                // NB: Flush requests for all other stores must already be on their way.
                RotateStores(tablet, false);

                LOG_INFO_IF(IsLeader(), "Waiting for all tablet stores to be flushed (TabletId: %v)",
                    tabletId);

                if (IsLeader()) {
                    CheckIfAllStoresFlushed(tablet);
                }
                break;
            }

            case ETabletState::Unmounted: {
                tablet->SetState(ETabletState::Unmounted);

                LOG_INFO_UNLESS(IsRecovery(), "Tablet unmounted (TabletId: %v)",
                    tabletId);

                if (!IsRecovery()) {
                    StopTabletEpoch(tablet);
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
            YASSERT(!PrelockedRows_.empty());
            auto rowRef = PrelockedRows_.front();
            PrelockedRows_.pop();

            auto* transaction = rowRef.Row.GetTransaction();
            YASSERT(transaction);

            YASSERT(!transaction->PrelockedRows().empty());
            YASSERT(transaction->PrelockedRows().front() == rowRef);
            transaction->PrelockedRows().pop();

            if (ValidateAndDiscardRowRef(rowRef)) {
                rowRef.Store->GetTablet()->GetStoreManager()->ConfirmRow(rowRef);
            }
        }

        LOG_DEBUG_UNLESS(IsRecovery(), "Rows confirmed (RowCount: %v)",
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
        auto codecId = ECodec(request.codec());
        auto* codec = GetCodec(codecId);
        auto compressedRequestData = TSharedRef::FromString(request.compressed_data());
        auto requestData = codec->Decompress(compressedRequestData);

        TWireProtocolReader reader(requestData);

        try {
            for (int index = 0; index < commandsSucceded; ++index) {
                YCHECK(ExecuteSingleWrite(tablet, transaction, &reader, false));
            }
        } catch (const std::exception& ex) {
            LOG_FATAL(ex, "Error executing writes");
        }

        LOG_DEBUG_UNLESS(IsRecovery(), "Rows written (TransactionId: %v, TabletId: %v, CommandsSucceded: %v)",
            transaction->GetId(),
            tablet->GetId(),
            commandsSucceded);
    }

    void HydraRotateStore(const TReqRotateStore& request)
    {
        auto tabletId = FromProto<TTabletId>(request.tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet)
            return;

        RotateStores(tablet, true);
        StartMemoryUsageTracking(tablet);
    }


    void HydraCommitTabletStoresUpdate(const TReqCommitTabletStoresUpdate& commitRequest)
    {
        auto tabletId = FromProto<TTabletId>(commitRequest.tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet)
            return;

        std::vector<TStoreId> storeIdsToAdd;
        for (const auto& descriptor : commitRequest.stores_to_add()) {
            storeIdsToAdd.push_back(FromProto<TStoreId>(descriptor.store_id()));
        }

        std::vector<TStoreId> storeIdsToRemove;
        for (const auto& descriptor : commitRequest.stores_to_remove()) {
            auto storeId = FromProto<TStoreId>(descriptor.store_id());
            storeIdsToRemove.push_back(storeId);
            auto store = tablet->GetStore(storeId);
            YCHECK(store->GetState() != EStoreState::ActiveDynamic);
            store->SetState(EStoreState::RemoveCommitting);
        }

        LOG_INFO_UNLESS(IsRecovery(), "Committing tablet stores update (TabletId: %v, StoreIdsToAdd: [%v], StoreIdsToRemove: [%v])",
            tabletId,
            JoinToString(storeIdsToAdd),
            JoinToString(storeIdsToRemove));

        TReqUpdateTabletStores updateRequest;
        ToProto(updateRequest.mutable_tablet_id(), tabletId);
        updateRequest.mutable_stores_to_add()->MergeFrom(commitRequest.stores_to_add());
        updateRequest.mutable_stores_to_remove()->MergeFrom(commitRequest.stores_to_remove());

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
            auto error = FromProto<TError>(response.error());
            LOG_WARNING_UNLESS(IsRecovery(), error, "Error updating tablet stores (TabletId: %v)",
                tabletId);

            for (const auto& descriptor : response.stores_to_remove()) {
                auto storeId = FromProto<TStoreId>(descriptor.store_id());
                auto store = tablet->GetStore(storeId);
                YCHECK(store->GetState() == EStoreState::RemoveCommitting);
                if (IsLeader()) {
                    BackoffStore(store, EStoreState::RemoveFailed);
                }
            }
        } else {
            auto storeManager = tablet->GetStoreManager();
            std::vector<TStoreId> addedStoreIds;
            for (const auto& descriptor : response.stores_to_add()) {
                auto storeId = FromProto<TChunkId>(descriptor.store_id());
                addedStoreIds.push_back(storeId);
                YCHECK(descriptor.has_chunk_meta());
                auto store = CreateChunkStore(tablet, storeId, &descriptor.chunk_meta());
                storeManager->AddStore(store);
            }

            std::vector<TStoreId> removedStoreIds;
            for (const auto& descriptor : response.stores_to_remove()) {
                auto storeId = FromProto<TStoreId>(descriptor.store_id());
                removedStoreIds.push_back(storeId);
                auto store = tablet->GetStore(storeId);
                storeManager->RemoveStore(store);
            }

            LOG_INFO_UNLESS(IsRecovery(), "Tablet stores updated successfully (TabletId: %v, AddedStoreIds: [%v], RemovedStoreIds: [%v])",
                tabletId,
                JoinToString(addedStoreIds),
                JoinToString(removedStoreIds));

            if (IsLeader()) {
                CheckIfAllStoresFlushed(tablet);
            }
        }
    }

    void HydraSplitPartition(const TReqSplitPartition& request)
    {
        auto tabletId = FromProto<TTabletId>(request.tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet)
            return;

        if (tablet->Partitions().size() >= tablet->GetConfig()->MaxPartitionCount)
            return;

        auto pivotKeys = FromProto<TOwningKey>(request.pivot_keys());
        auto* partition = tablet->GetPartitionByPivotKey(pivotKeys[0]);
        int partitionIndex = partition->GetIndex();

        LOG_INFO_UNLESS(IsRecovery(), "Splitting partition (TabletId: %v, PartitionIndex: %v, DataSize: %v, Keys: %v)",
            tablet->GetId(),
            partitionIndex,
            partition->GetTotalDataSize(),
            JoinToString(pivotKeys, Stroka(" .. ")));

        tablet->SplitPartition(partitionIndex, pivotKeys);

        if (!IsRecovery()) {
            auto tabletSlotManager = Bootstrap_->GetTabletSlotManager();
            tabletSlotManager->UpdateTablet(tablet);
        }
    }

    void HydraMergePartitions(const TReqMergePartitions& request)
    {
        auto tabletId = FromProto<TTabletId>(request.tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet)
            return;

        auto pivotKey = FromProto<TOwningKey>(request.pivot_key());
        auto* partition = tablet->GetPartitionByPivotKey(pivotKey);
        int firstPartitionIndex = partition->GetIndex();
        int lastPartitionIndex = firstPartitionIndex + request.partition_count() - 1;

        LOG_INFO_UNLESS(IsRecovery(), "Merging partitions (TabletId: %v, PartitionIndexes: %v .. %v, Keys: %v .. %v)",
            tablet->GetId(),
            firstPartitionIndex,
            lastPartitionIndex,
            tablet->Partitions()[firstPartitionIndex]->GetPivotKey(),
            tablet->Partitions()[lastPartitionIndex]->GetNextPivotKey());

        tablet->MergePartitions(firstPartitionIndex, lastPartitionIndex);

        if (!IsRecovery()) {
            auto tabletSlotManager = Bootstrap_->GetTabletSlotManager();
            tabletSlotManager->UpdateTablet(tablet);
        }
    }

    void HydraUpdatePartitionSampleKeys(const TReqUpdatePartitionSampleKeys& request)
    {
        auto tabletId = FromProto<TTabletId>(request.tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet)
            return;

        auto pivotKey = FromProto<TOwningKey>(request.pivot_key());
        auto* partition = tablet->FindPartitionByPivotKey(pivotKey);
        if (!partition)
            return;

        LOG_INFO_UNLESS(IsRecovery(), "Updating partition sample keys (TabletId: %v, PartitionIndex: %v, SampleKeyCount: %v)",
            tablet->GetId(),
            partition->GetIndex(),
            request.sample_keys_size());

        partition->SampleKeys() = FromProto<TOwningKey>(request.sample_keys());
        partition->SetSamplingNeeded(false);

        if (!IsRecovery()) {
            auto tabletSlotManager = Bootstrap_->GetTabletSlotManager();
            tabletSlotManager->UpdateTablet(tablet);
        }
    }


    void OnTransactionPrepared(TTransaction* transaction)
    {
        if (transaction->LockedRows().empty()) {
            auto handleRow = [&] (const TDynamicRowRef& rowRef) {
                // NB: Don't call ValidateAndDiscardRowRef, row refs are just scanned.
                if (ValidateRowRef(rowRef)) {
                    rowRef.Store->GetTablet()->GetStoreManager()->PrepareRow(rowRef);
                }
            };

            for (const auto& rowRef : transaction->LockedRows()) {
                handleRow(rowRef);
            }

            for (auto it = transaction->PrelockedRows().begin();
                it != transaction->PrelockedRows().end();
                transaction->PrelockedRows().move_forward(it))
            {
                handleRow(*it);
            }

            LOG_DEBUG_UNLESS(IsRecovery(), "Locked rows prepared (TransactionId: %v, LockedRowCount: %v, PrelockedRowCount: %v)",
                transaction->GetId(),
                transaction->LockedRows().size(),
                transaction->PrelockedRows().size());
        }
    }

    void OnTransactionCommitted(TTransaction* transaction)
    {
        if (!transaction->LockedRows().empty()) {
            auto handleRow = [&] (const TDynamicRowRef& rowRef) {
                if (ValidateAndDiscardRowRef(rowRef)) {
                    rowRef.Store->GetTablet()->GetStoreManager()->CommitRow(rowRef);
                }
            };

            for (const auto& rowRef : transaction->LockedRows()) {
                handleRow(rowRef);
            }

            LOG_DEBUG_UNLESS(IsRecovery(), "Locked rows committed (TransactionId: %v, RowCount: %v)",
                transaction->GetId(),
                transaction->LockedRows().size());

            YCHECK(transaction->PrelockedRows().empty());
            transaction->LockedRows().clear();
        }

        OnTransactionFinished(transaction);
    }

    void OnTransactionAborted(TTransaction* transaction)
    {
        if (!transaction->LockedRows().empty()) {
            auto handleRow = [&] (const TDynamicRowRef& rowRef) {
                if (ValidateAndDiscardRowRef(rowRef)) {
                    rowRef.Store->GetTablet()->GetStoreManager()->AbortRow(rowRef);
                }
            };

            for (const auto& rowRef : transaction->LockedRows()) {
                handleRow(rowRef);
            }

            LOG_DEBUG_UNLESS(IsRecovery(), "Locked rows aborted (TransactionId: %v, RowCount: %v)",
                transaction->GetId(),
                transaction->LockedRows().size());

            YCHECK(transaction->PrelockedRows().empty());
            transaction->LockedRows().clear();
        }

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


    void SetStoreOrphaned(IStorePtr store)
    {
        if (store->GetState() == EStoreState::Orphaned)
            return;
        store->SetState(EStoreState::Orphaned);

        if (store->GetType() != EStoreType::DynamicMemory)
            return;
        
        auto dynamicStore = store->AsDynamicMemory();
        int lockCount = dynamicStore->GetLockCount();
        if (lockCount > 0) {
            YCHECK(OrphanedStores_.insert(dynamicStore).second);
            LOG_INFO_UNLESS(IsRecovery(), "Dynamic memory store is orphaned and will be kept (StoreId: %v, TabletId: %v, LockCount: %v)",
                store->GetId(),
                store->GetTablet()->GetId(),
                lockCount);
        }
    }

    bool ValidateRowRef(const TDynamicRowRef& rowRef)
    {
        auto* store = rowRef.Store;
        return store->GetState() != EStoreState::Orphaned;
    }

    bool ValidateAndDiscardRowRef(const TDynamicRowRef& rowRef)
    {
        auto* store = rowRef.Store;
        if (store->GetState() != EStoreState::Orphaned) {
            return true;
        }

        int lockCount = store->Unlock();
        if (lockCount == 0) {
            LOG_INFO_UNLESS(IsRecovery(), "Store unlocked and will be dropped (StoreId: %v)",
                store->GetId());
            YCHECK(OrphanedStores_.erase(store) == 1);
        }

        return false;
    }


    bool ExecuteSingleRead(
        TTablet* tablet,
        TTimestamp timestamp,
        TWireProtocolReader* reader,
        TWireProtocolWriter* writer)
    {
        auto command = reader->ReadCommand();
        if (command == EWireProtocolCommand::End) {
            return false;
        }

        const auto& storeManager = tablet->GetStoreManager();

        switch (command) {
            case EWireProtocolCommand::LookupRows:
                storeManager->LookupRows(
                    timestamp,
                    reader,
                    writer);
                break;

            default:
                THROW_ERROR_EXCEPTION("Unknown read command %v",
                    command);
        }

        return true;
    }

    bool ExecuteSingleWrite(
        TTablet* tablet,
        TTransaction* transaction,
        TWireProtocolReader* reader,
        bool prelock)
    {
        auto command = reader->ReadCommand();
        if (command == EWireProtocolCommand::End) {
            return false;
        }
            
        const auto& storeManager = tablet->GetStoreManager();
        TDynamicRowRef rowRef;
        switch (command) {
            case EWireProtocolCommand::WriteRow: {
                auto row = reader->ReadUnversionedRow();
                rowRef = storeManager->WriteRow(transaction, row, prelock);
                break;
            }

            case EWireProtocolCommand::DeleteRow: {
                auto key = reader->ReadUnversionedRow();
                rowRef = storeManager->DeleteRow(transaction, key, prelock);
                break;
            }

            default:
                THROW_ERROR_EXCEPTION("Unknown write command %v",
                    command);
        }

        if (prelock && rowRef) {
            PrelockedRows_.push(rowRef);
            transaction->PrelockedRows().push(rowRef);
        }

        return true;
    }


    void CheckIfFullyUnlocked(TTablet* tablet)
    {
        if (tablet->GetState() != ETabletState::WaitingForLocks)
            return;

        if (tablet->GetStoreManager()->HasActiveLocks())
            return;

        LOG_INFO_UNLESS(IsRecovery(), "All tablet locks released (TabletId: %v)",
            tablet->GetId());

        tablet->SetState(ETabletState::FlushPending);

        TReqSetTabletState request;
        ToProto(request.mutable_tablet_id(), tablet->GetId());
        request.set_state(ETabletState::Flushing);
        PostTabletMutation(request);
    }

    void CheckIfAllStoresFlushed(TTablet* tablet)
    {
        if (tablet->GetState() != ETabletState::Flushing)
            return;

        if (tablet->GetStoreManager()->HasUnflushedStores())
            return;

        LOG_INFO_UNLESS(IsRecovery(), "All tablet stores flushed (TabletId: %v)",
            tablet->GetId());

        tablet->SetState(ETabletState::UnmountPending);

        TReqSetTabletState request;
        ToProto(request.mutable_tablet_id(), tablet->GetId());
        request.set_state(ETabletState::Unmounted);
        PostTabletMutation(request);
    }


    void RotateStores(TTablet* tablet, bool createNew)
    {
        auto storeManager = tablet->GetStoreManager();
        storeManager->RotateStores(createNew);
    }


    void PostTabletMutation(const ::google::protobuf::MessageLite& message)
    {
        auto mutation = CreateMutation(Slot_->GetHydraManager(), message);
        Slot_->GetEpochAutomatonInvoker(EAutomatonThreadQueue::Write)->Invoke(BIND(
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


    void StartTabletEpoch(TTablet* tablet)
    {
        tablet->StartEpoch(Slot_);

        auto tabletSlotManager = Bootstrap_->GetTabletSlotManager();
        tabletSlotManager->RegisterTablet(tablet);
    }

    void StopTabletEpoch(TTablet* tablet)
    {
        tablet->SetState(tablet->GetPersistentState());
        
        for (const auto& partition : tablet->Partitions()) {
            partition->SetState(EPartitionState::None);
            partition->SetSamplingNeeded(false);
        }

        for (const auto& pair : tablet->Stores()) {
            const auto& store = pair.second;
            store->SetState(store->GetPersistentState());
        }

        tablet->StopEpoch();
        
        tablet->GetStoreManager()->ResetRotationScheduled();

        auto tabletSlotManager = Bootstrap_->GetTabletSlotManager();
        tabletSlotManager->UnregisterTablet(tablet);
    }


    IStorePtr CreateChunkStore(
        TTablet* tablet,
        const TChunkId& chunkId,
        const TChunkMeta* chunkMeta)
    {
        return New<TChunkStore>(
            chunkId,
            tablet,
            chunkMeta,
            Bootstrap_);
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
            .BeginAttributes()
                .Item("opaque").Value(true)
            .EndAttributes()
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
                .Item("state").Value(partition->GetState())
                .Item("pivot_key").Value(partition->GetPivotKey())
                .Item("next_pivot_key").Value(partition->GetNextPivotKey())
                .Item("sample_key_count").Value(partition->SampleKeys().size())
                .Item("sampling_needed").Value(partition->GetSamplingNeeded())
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


    void OnStoreMemoryUsageUpdated(i64 delta)
    {
        auto* tracker = Bootstrap_->GetMemoryUsageTracker();
        if (delta >= 0) {
            tracker->Acquire(EMemoryConsumer::Tablet, delta);
        } else {
            tracker->Release(EMemoryConsumer::Tablet, -delta);
        }
    }

    void StartMemoryUsageTracking(TTablet* tablet)
    {
        auto store = tablet->GetActiveStore();
        store->SubscribeMemoryUsageUpdated(OnStoreMemoryUsageUpdated_);
        OnStoreMemoryUsageUpdated(store->GetMemoryUsage());
    }

    void ValidateMemoryLimit()
    {
        if (Bootstrap_->GetTabletSlotManager()->IsOutOfMemory()) {
            THROW_ERROR_EXCEPTION("Out of tablet memory, all writes disabled");
        }
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

void TTabletManager::BackoffStore(IStorePtr store, EStoreState state)
{
    Impl_->BackoffStore(store, state);
}

std::vector<TSharedRef> TTabletManager::Read(
    TTablet* tablet,
    TTimestamp timestamp,
    const TSharedRef& requestData)
 {
    return Impl_->Read(
        tablet,
        timestamp,
        requestData);
}

void TTabletManager::Write(
    TTablet* tablet,
    TTransaction* transaction,
    const TSharedRef& requestData)
{
    Impl_->Write(
        tablet,
        transaction,
        requestData);
}

IStorePtr TTabletManager::CreateStore(TTablet* tablet, const TStoreId& storeId)
{
    return Impl_->CreateStore(tablet, storeId);
}

void TTabletManager::ScheduleStoreRotation(TTablet* tablet)
{
    Impl_->ScheduleStoreRotation(tablet);
}

void TTabletManager::BuildOrchidYson(IYsonConsumer* consumer)
{
    Impl_->BuildOrchidYson(consumer);
}

DELEGATE_ENTITY_MAP_ACCESSORS(TTabletManager, Tablet, TTablet, TTabletId, *Impl_)

///////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
