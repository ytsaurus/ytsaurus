#include "tablet_manager.h"
#include "private.h"
#include "automaton.h"
#include "sorted_chunk_store.h"
#include "ordered_chunk_store.h"
#include "config.h"
#include "sorted_dynamic_store.h"
#include "ordered_dynamic_store.h"
#include "in_memory_manager.h"
#include "lookup.h"
#include "partition.h"
#include "security_manager.h"
#include "slot_manager.h"
#include "store_flusher.h"
#include "sorted_store_manager.h"
#include "ordered_store_manager.h"
#include "tablet.h"
#include "tablet_slot.h"
#include "transaction.h"
#include "transaction_manager.h"

#include <yt/server/cell_node/bootstrap.h>

#include <yt/server/data_node/chunk_block_manager.h>
#include <yt/server/data_node/master_connector.h>

#include <yt/server/hive/hive_manager.h>
#include <yt/server/hive/transaction_supervisor.pb.h>

#include <yt/server/hydra/hydra_manager.h>
#include <yt/server/hydra/mutation.h>
#include <yt/server/hydra/mutation_context.h>

#include <yt/server/misc/memory_usage_tracker.h>

#include <yt/server/tablet_node/tablet_manager.pb.h>
#include <yt/server/tablet_node/transaction_manager.h>

#include <yt/server/tablet_server/tablet_manager.pb.h>

#include <yt/ytlib/chunk_client/block_cache.h>
#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/ytlib/table_client/name_table.h>

#include <yt/ytlib/tablet_client/config.h>
#include <yt/ytlib/tablet_client/wire_protocol.h>
#include <yt/ytlib/tablet_client/wire_protocol.pb.h>

#include <yt/ytlib/transaction_client/helpers.h>
#include <yt/ytlib/transaction_client/timestamp_provider.h>

#include <yt/ytlib/api/client.h>

#include <yt/core/compression/codec.h>

#include <yt/core/misc/nullable.h>
#include <yt/core/misc/ring_queue.h>
#include <yt/core/misc/string.h>

#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/virtual.h>

namespace NYT {
namespace NTabletNode {

using namespace NCompression;
using namespace NConcurrency;
using namespace NYson;
using namespace NYTree;
using namespace NHydra;
using namespace NCellNode;
using namespace NTabletClient;
using namespace NTabletClient::NProto;
using namespace NTabletNode::NProto;
using namespace NTabletServer::NProto;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NObjectClient;
using namespace NNodeTrackerClient;
using namespace NHive;
using namespace NHive::NProto;
using namespace NQueryClient;

////////////////////////////////////////////////////////////////////////////////

class TTabletManager::TImpl
    : public TTabletAutomatonPart
{
public:
    explicit TImpl(
        TTabletManagerConfigPtr config,
        TTabletSlotPtr slot,
        TBootstrap* bootstrap)
        : TTabletAutomatonPart(
            slot,
            bootstrap)
        , Config_(config)
        , ChangelogCodec_(GetCodec(Config_->ChangelogCodec))
        , TabletContext_(this)
        , TabletMap_(TTabletMapTraits(this))
        , OrchidService_(TOrchidService::Create(MakeWeak(this), Slot_->GetGuardedAutomatonInvoker()))
    {
        VERIFY_INVOKER_THREAD_AFFINITY(Slot_->GetAutomatonInvoker(), AutomatonThread);

        RegisterLoader(
            "TabletManager.Keys",
            BIND(&TImpl::LoadKeys, Unretained(this)));
        RegisterLoader(
            "TabletManager.Values",
            BIND(&TImpl::LoadValues, Unretained(this)));
        RegisterLoader(
            "TabletManager.Async",
            BIND(&TImpl::LoadAsync, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "TabletManager.Keys",
            BIND(&TImpl::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "TabletManager.Values",
            BIND(&TImpl::SaveValues, Unretained(this)));
        RegisterSaver(
            EAsyncSerializationPriority::Default,
            "TabletManager.Async",
            BIND(&TImpl::SaveAsync, Unretained(this)));

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
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::NoSuchTablet,
                "No such tablet %v",
                id);
        }
        return tablet;
    }


    void Read(
        TTabletSnapshotPtr tabletSnapshot,
        TTimestamp timestamp,
        const TWorkloadDescriptor& workloadDescriptor,
        TWireProtocolReader* reader,
        TWireProtocolWriter* writer)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        ValidateReadTimestamp(timestamp);

        while (!reader->IsFinished()) {
            ExecuteSingleRead(
                tabletSnapshot,
                timestamp,
                workloadDescriptor,
                reader,
                writer);
        }
    }

    void Write(
        TTabletSnapshotPtr tabletSnapshot,
        const TTransactionId& transactionId,
        TWireProtocolReader* reader,
        TFuture<void>* commitResult)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        // NB: No yielding beyond this point.
        // May access tablet and transaction.

        auto* tablet = GetTabletOrThrow(tabletSnapshot->TabletId);

        tablet->ValidateMountRevision(tabletSnapshot->MountRevision);
        ValidateTabletMounted(tablet);
        ValidateTabletStoreLimit(tablet);
        ValidateMemoryLimit();

        auto atomicity = AtomicityFromTransactionId(transactionId);
        switch (atomicity) {
            case EAtomicity::Full:
                WriteAtomic(tablet, transactionId, reader, commitResult);
                break;

            case EAtomicity::None:
                ValidateClientTimestamp(transactionId);
                WriteNonAtomic(tablet, transactionId, reader, commitResult);
                break;

            default:
                YUNREACHABLE();
        }
    }


    void ScheduleStoreRotation(TTablet* tablet)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& storeManager = tablet->GetStoreManager();
        if (!storeManager->IsRotationPossible()) {
            return;
        }

        storeManager->ScheduleRotation();

        TReqRotateStore request;
        ToProto(request.mutable_tablet_id(), tablet->GetId());
        request.set_mount_revision(tablet->GetMountRevision());
        CommitTabletMutation(request);
    }

    IYPathServicePtr GetOrchidService()
    {
        return OrchidService_;
    }


    DECLARE_ENTITY_MAP_ACCESSORS(Tablet, TTablet);

private:
    class TOrchidService
        : public TVirtualMapBase
    {
    public:
        static IYPathServicePtr Create(TWeakPtr<TImpl> impl, IInvokerPtr invoker)
        {
            return New<TOrchidService>(std::move(impl))
                ->Via(invoker);
        }

        virtual std::vector<Stroka> GetKeys(i64 limit) const override
        {
            std::vector<Stroka> keys;
            if (auto owner = Owner_.Lock()) {
                for (const auto& tablet : owner->Tablets()) {
                    if (keys.size() >= limit) {
                        break;
                    }
                    keys.push_back(ToString(tablet.first));
                }
            }
            return keys;
        }

        virtual i64 GetSize() const override
        {
            if (auto owner = Owner_.Lock()) {
                return owner->Tablets().size();
            }
            return 0;
        }

        virtual IYPathServicePtr FindItemService(const TStringBuf& key) const override
        {
            if (auto owner = Owner_.Lock()) {
                if (auto tablet = owner->FindTablet(TTabletId::FromString(key))) {
                    auto producer = BIND(&TImpl::BuildTabletOrchidYson, owner, tablet);
                    return ConvertToNode(producer);
                }
            }
            return nullptr;
        }

    private:
        const TWeakPtr<TImpl> Owner_;

        explicit TOrchidService(TWeakPtr<TImpl> impl)
            : Owner_(std::move(impl))
        { }

        DECLARE_NEW_FRIEND();
    };

    const TTabletManagerConfigPtr Config_;

    ICodec* const ChangelogCodec_;

    class TTabletContext
        : public ITabletContext
    {
    public:
        explicit TTabletContext(TImpl* owner)
            : Owner_(owner)
        { }

        virtual TCellId GetCellId() override
        {
            return Owner_->Slot_->GetCellId();
        }

        virtual TColumnEvaluatorCachePtr GetColumnEvaluatorCache() override
        {
            return Owner_->Bootstrap_->GetColumnEvaluatorCache();
        }

        virtual TObjectId GenerateId(EObjectType type) override
        {
            return Owner_->Slot_->GenerateId(type);
        }

        virtual IStorePtr CreateStore(
            TTablet* tablet,
            EStoreType type,
            const TStoreId& storeId,
            const TAddStoreDescriptor* descriptor) override
        {
            return Owner_->CreateStore(tablet, type, storeId, descriptor);
        }

    private:
        TImpl* const Owner_;

    };

    class TTabletMapTraits
    {
    public:
        explicit TTabletMapTraits(TImpl* owner)
            : Owner_(owner)
        { }

        std::unique_ptr<TTablet> Create(const TTabletId& id) const
        {
            return std::make_unique<TTablet>(id, &Owner_->TabletContext_);
        }

    private:
        TImpl* const Owner_;

    };

    TTimestamp LastCommittedTimestamp_ = MinTimestamp;
    TTabletContext TabletContext_;
    TEntityMap<TTablet, TTabletMapTraits> TabletMap_;
    yhash_set<TTablet*> UnmountingTablets_;

    yhash_set<IDynamicStorePtr> OrphanedStores_;

    const IYPathServicePtr OrchidService_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    void SaveKeys(TSaveContext& context) const
    {
        TabletMap_.SaveKeys(context);
    }

    void SaveValues(TSaveContext& context) const
    {
        using NYT::Save;

        Save(context, LastCommittedTimestamp_);
        TabletMap_.SaveValues(context);
    }

    TCallback<void(TSaveContext&)> SaveAsync()
    {
        std::vector<std::pair<TTabletId, TCallback<void(TSaveContext&)>>> capturedTablets;
        for (const auto& pair : TabletMap_) {
            auto* tablet = pair.second;
            capturedTablets.push_back(std::make_pair(tablet->GetId(), tablet->AsyncSave()));
        }

        return BIND(
            [
                capturedTablets = std::move(capturedTablets)
            ] (TSaveContext& context) {
                using NYT::Save;
                for (const auto& pair : capturedTablets) {
                    Save(context, pair.first);
                    pair.second.Run(context);
                }
            });
    }

    void LoadKeys(TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TabletMap_.LoadKeys(context);
    }

    void LoadValues(TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        Load(context, LastCommittedTimestamp_);
        TabletMap_.LoadValues(context);
    }

    void LoadAsync(TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        SERIALIZATION_DUMP_WRITE(context, "tablets[%v]", TabletMap_.size());
        SERIALIZATION_DUMP_INDENT(context) {
            for (size_t index = 0; index != TabletMap_.size(); ++index) {
                auto tabletId = LoadSuspended<TTabletId>(context);
                auto* tablet = GetTablet(tabletId);
                SERIALIZATION_DUMP_WRITE(context, "%v =>", tabletId);
                SERIALIZATION_DUMP_INDENT(context) {
                    tablet->AsyncLoad(context);
                }
            }
        }
    }


    virtual void OnAfterSnapshotLoaded() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TTabletAutomatonPart::OnAfterSnapshotLoaded();

        for (const auto& pair : TabletMap_) {
            auto* tablet = pair.second;
            auto storeManager = CreateStoreManager(tablet);
            tablet->SetStoreManager(storeManager);
            if (tablet->GetState() >= ETabletState::WaitingForLocks) {
                YCHECK(UnmountingTablets_.insert(tablet).second);
            }
        }

        auto transactionManager = Slot_->GetTransactionManager();
        for (const auto& pair : transactionManager->Transactions()) {
            auto* transaction = pair.second;
            int rowCount = 0;
            for (const auto& record : transaction->WriteLog()) {
                auto* tablet = FindTablet(record.TabletId);
                if (!tablet) {
                    // NB: Tablet could be missing if it was e.g. forcefully removed.
                    continue;
                }

                TWireProtocolReader reader(record.Data);
                const auto& storeManager = tablet->GetStoreManager();
                while (!reader.IsFinished()) {
                    storeManager->ExecuteAtomicWrite(tablet, transaction, &reader, false);
                    ++rowCount;
                }
            }
            LOG_DEBUG_IF(rowCount > 0, "Transaction write log applied (TransactionId: %v, RowCount: %v)",
                transaction->GetId(),
                rowCount);

            if (transaction->GetState() == ETransactionState::PersistentCommitPrepared) {
                OnTransactionPrepared(transaction);
            }
        }
    }

    virtual void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TTabletAutomatonPart::Clear();

        TabletMap_.Clear();
        UnmountingTablets_.clear();
        OrphanedStores_.clear();
    }


    virtual void OnLeaderRecoveryComplete() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TTabletAutomatonPart::OnLeaderRecoveryComplete();

        StartEpoch();
    }

    virtual void OnLeaderActive() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TTabletAutomatonPart::OnLeaderActive();

        for (const auto& pair : TabletMap_) {
            auto* tablet = pair.second;
            CheckIfFullyUnlocked(tablet);
            CheckIfFullyFlushed(tablet);
        }
    }

    
    template <class TPrelockedRows>
    void HandleRowsOnStopLeading(TTransaction* transaction, TPrelockedRows& rows)
    {
        while (!rows.empty()) {
            auto rowRef = rows.front();
            rows.pop();
            if (ValidateAndDiscardRowRef(rowRef)) {
                rowRef.StoreManager->AbortRow(transaction, rowRef);
            }
        }
    }
    
    virtual void OnStopLeading() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TTabletAutomatonPart::OnStopLeading();

        auto transactionManager = Slot_->GetTransactionManager();
        for (const auto& pair : transactionManager->Transactions()) {
            auto* transaction = pair.second;
            HandleRowsOnStopLeading(transaction, transaction->PrelockedSortedRows());
            HandleRowsOnStopLeading(transaction, transaction->PrelockedOrderedRows());
        }

        StopEpoch();
    }


    virtual void OnFollowerRecoveryComplete() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TTabletAutomatonPart::OnFollowerRecoveryComplete();

        StartEpoch();
    }

    virtual void OnStopFollowing() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TTabletAutomatonPart::OnStopFollowing();

        StopEpoch();
    }


    void StartEpoch()
    {
        for (const auto& pair : TabletMap_) {
            auto* tablet = pair.second;
            StartTabletEpoch(tablet);
        }
    }

    void StopEpoch()
    {
        for (const auto& pair : TabletMap_) {
            auto* tablet = pair.second;
            StopTabletEpoch(tablet);
        }
    }


    void HydraMountTablet(TReqMountTablet* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto mountRevision = request->mount_revision();
        auto tableId = FromProto<TObjectId>(request->table_id());
        auto schema = FromProto<TTableSchema>(request->schema());
        auto pivotKey = request->has_pivot_key() ? FromProto<TOwningKey>(request->pivot_key()) : TOwningKey();
        auto nextPivotKey = request->has_next_pivot_key() ? FromProto<TOwningKey>(request->next_pivot_key()) : TOwningKey();
        auto mountConfig = DeserializeTableMountConfig((TYsonString(request->mount_config())), tabletId);
        auto writerOptions = DeserializeTabletWriterOptions(TYsonString(request->writer_options()), tabletId);
        auto atomicity = EAtomicity(request->atomicity());
        auto storeDescriptors = FromProto<std::vector<TAddStoreDescriptor>>(request->stores());

        auto tabletHolder = std::make_unique<TTablet>(
            mountConfig,
            writerOptions,
            tabletId,
            mountRevision,
            tableId,
            &TabletContext_,
            schema,
            pivotKey,
            nextPivotKey,
            atomicity);
        auto* tablet = TabletMap_.Insert(tabletId, std::move(tabletHolder));

        auto storeManager = CreateStoreManager(tablet);
        tablet->SetStoreManager(storeManager);

        storeManager->Mount(storeDescriptors);

        // TODO(babenko): move somewhere?
        for (const auto& descriptor : storeDescriptors) {
            const auto& extensions = descriptor.chunk_meta().extensions();
            auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(extensions);
            if (miscExt.has_max_timestamp()) {
                UpdateLastCommittedTimestamp(miscExt.max_timestamp());
            }
        }

        {
            TRspMountTablet response;
            ToProto(response.mutable_tablet_id(), tabletId);
            PostMasterMutation(response);
        }

        if (!IsRecovery()) {
            StartTabletEpoch(tablet);
        }

        LOG_INFO_UNLESS(IsRecovery(), "Tablet mounted (TabletId: %v, MountRevision: %x, TableId: %v, Keys: %v .. %v, "
            "StoreCount: %v, PartitionCount: %v, Atomicity: %v)",
            tabletId,
            mountRevision,
            tableId,
            pivotKey,
            nextPivotKey,
            request->stores_size(),
            tablet->IsSorted() ? MakeNullable(tablet->PartitionList().size()) : Null,
            tablet->GetAtomicity());
    }

    void HydraUnmountTablet(TReqUnmountTablet* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        if (request->force()) {
            LOG_INFO_UNLESS(IsRecovery(), "Tablet is forcefully unmounted (TabletId: %v)",
                tabletId);

            // Just a formality.
            tablet->SetState(ETabletState::Unmounted);

            for (const auto& pair : tablet->StoreIdMap()) {
                SetStoreOrphaned(tablet, pair.second);
            }

            const auto& storeManager = tablet->GetStoreManager();
            for (const auto& store : storeManager->GetLockedStores()) {
                SetStoreOrphaned(tablet, store);
            }

            if (!IsRecovery()) {
                StopTabletEpoch(tablet);
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

    void HydraRemountTablet(TReqRemountTablet* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto mountConfig = DeserializeTableMountConfig((TYsonString(request->mount_config())), tabletId);
        auto writerOptions = DeserializeTabletWriterOptions(TYsonString(request->writer_options()), tabletId);

        if (mountConfig->ReadOnly && !tablet->GetConfig()->ReadOnly) {
            RotateStores(tablet, true);
        }

        const auto& storeManager = tablet->GetStoreManager();
        storeManager->Remount(mountConfig, writerOptions);

        UpdateTabletSnapshot(tablet);

        LOG_INFO_UNLESS(IsRecovery(), "Tablet remounted (TabletId: %v)",
            tabletId);
    }

    void HydraSetTabletState(TReqSetTabletState* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto mountRevision = request->mount_revision();
        if (mountRevision != tablet->GetMountRevision()) {
            return;
        }

        auto requestedState = ETabletState(request->state());

        switch (requestedState) {
            case ETabletState::Flushing: {
                tablet->SetState(ETabletState::Flushing);

                // NB: Flush requests for all other stores must already be on their way.
                RotateStores(tablet, false);

                LOG_INFO_IF(IsLeader(), "Waiting for all tablet stores to be flushed (TabletId: %v)",
                    tabletId);

                if (IsLeader()) {
                    CheckIfFullyFlushed(tablet);
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
    
    template <class TPrelockedRows>
    void HandleRowsOnLeaderExecuteWriteAtomic(TTransaction* transaction, TPrelockedRows& rows, int rowCount)
    {
        for (int index = 0; index < rowCount; ++index) {
            Y_ASSERT(!rows.empty());
            auto rowRef = rows.front();
            rows.pop();
            if (ValidateAndDiscardRowRef(rowRef)) {
                rowRef.StoreManager->ConfirmRow(transaction, rowRef);
            }
        }
    }

    void HydraLeaderExecuteWriteAtomic(
        const TTransactionId& transactionId,
        int sortedRowCount,
        int orderedRowCount,
        const TTransactionWriteRecord& writeRecord,
        TMutationContext* /*context*/)
    {
        auto transactionManager = Slot_->GetTransactionManager();
        auto* transaction = transactionManager->GetTransaction(transactionId);

        HandleRowsOnLeaderExecuteWriteAtomic(transaction, transaction->PrelockedSortedRows(), sortedRowCount);
        HandleRowsOnLeaderExecuteWriteAtomic(transaction, transaction->PrelockedOrderedRows(), orderedRowCount);

        transaction->WriteLog().Enqueue(writeRecord);

        LOG_DEBUG_UNLESS(IsRecovery(), "Rows confirmed (TabletId: %v, TransactionId: %v, "
            "SortedRows: %v, OrderedRows: %v, WriteRecordSize: %v)",
            writeRecord.TabletId,
            transactionId,
            sortedRowCount,
            orderedRowCount,
            writeRecord.Data.Size());
    }

    void HydraLeaderExecuteWriteNonAtomic(
        const TTabletId& tabletId,
        i64 mountRevision,
        const TTransactionId& transactionId,
        const TSharedRef& recordData,
        TMutationContext* /*context*/)
    {
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            // NB: Tablet could be missing if it was e.g. forcefully removed.
            return;
        }

        tablet->ValidateMountRevision(mountRevision);

        auto commitTimestamp = TimestampFromTransactionId(transactionId);
        auto adjustedCommitTimestamp = AdjustCommitTimestamp(commitTimestamp);

        TWireProtocolReader reader(recordData);
        int rowCount = 0;
        const auto& storeManager = tablet->GetStoreManager();
        while (!reader.IsFinished()) {
            storeManager->ExecuteNonAtomicWrite(tablet, adjustedCommitTimestamp, &reader);
            ++rowCount;
        }

        LOG_DEBUG_UNLESS(IsRecovery(), "Rows written (TransactionId: %v, TabletId: %v, RowCount: %v, "
            "WriteRecordSize: %v)",
            transactionId,
            tabletId,
            rowCount,
            recordData.Size());
    }

    void HydraFollowerExecuteWrite(TReqExecuteWrite* request) noexcept
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto atomicity = AtomicityFromTransactionId(transactionId);

        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            // NB: Tablet could be missing if it was e.g. forcefully removed.
            return;
        }

        auto mountRevision = request->mount_revision();
        if (mountRevision != tablet->GetMountRevision()) {
            return;
        }

        auto codecId = ECodec(request->codec());
        auto* codec = GetCodec(codecId);
        auto compressedRecordData = TSharedRef::FromString(request->compressed_data());
        auto recordData = codec->Decompress(compressedRecordData);

        TWireProtocolReader reader(recordData);
        int rowCount = 0;

        const auto& storeManager = tablet->GetStoreManager();

        switch (atomicity) {
            case EAtomicity::Full: {
                auto transactionManager = Slot_->GetTransactionManager();
                auto* transaction = transactionManager->GetTransaction(transactionId);

                auto writeRecord = TTransactionWriteRecord{tabletId, recordData};

                while (!reader.IsFinished()) {
                    storeManager->ExecuteAtomicWrite(tablet, transaction, &reader, false);
                    ++rowCount;
                }

                transaction->WriteLog().Enqueue(writeRecord);
            }

            case EAtomicity::None: {
                auto commitTimestamp = TimestampFromTransactionId(transactionId);
                auto adjustedCommitTimestamp = AdjustCommitTimestamp(commitTimestamp);
                while (!reader.IsFinished()) {
                    storeManager->ExecuteNonAtomicWrite(tablet, adjustedCommitTimestamp, &reader);
                    ++rowCount;
                }
                break;
            }

            default:
                YUNREACHABLE();
        }

        LOG_DEBUG_UNLESS(IsRecovery(), "Rows written (TransactionId: %v, TabletId: %v, RowCount: %v, WriteRecordSize: %v)",
            transactionId,
            tabletId,
            rowCount,
            recordData.Size());
    }

    void HydraRotateStore(TReqRotateStore* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }
        if (tablet->GetState() != ETabletState::Mounted) {
            return;
        }

        auto mountRevision = request->mount_revision();
        if (mountRevision != tablet->GetMountRevision()) {
            return;
        }

        RotateStores(tablet, true);
        UpdateTabletSnapshot(tablet);
    }


    void HydraCommitTabletStoresUpdate(TReqCommitTabletStoresUpdate* commitRequest)
    {
        auto tabletId = FromProto<TTabletId>(commitRequest->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto mountRevision = commitRequest->mount_revision();
        if (mountRevision != tablet->GetMountRevision()) {
            return;
        }

        SmallVector<TStoreId, TypicalStoreIdCount> storeIdsToAdd;
        for (const auto& descriptor : commitRequest->stores_to_add()) {
            auto storeId = FromProto<TStoreId>(descriptor.store_id());
            storeIdsToAdd.push_back(storeId);
        }

        SmallVector<TStoreId, TypicalStoreIdCount> storeIdsToRemove;
        for (const auto& descriptor : commitRequest->stores_to_remove()) {
            auto storeId = FromProto<TStoreId>(descriptor.store_id());
            storeIdsToRemove.push_back(storeId);
            auto store = tablet->GetStore(storeId);
            YCHECK(store->GetStoreState() != EStoreState::ActiveDynamic);
            store->SetStoreState(EStoreState::RemoveCommitting);
        }

        LOG_INFO_UNLESS(IsRecovery(), "Committing tablet stores update "
            "(TabletId: %v, StoreIdsToAdd: %v, StoreIdsToRemove: %v)",
            tabletId,
            storeIdsToAdd,
            storeIdsToRemove);

        auto hiveManager = Slot_->GetHiveManager();
        auto* masterMailbox = Slot_->GetMasterMailbox();

        {
            TReqUpdateTabletStores masterRequest;
            ToProto(masterRequest.mutable_tablet_id(), tabletId);
            masterRequest.set_mount_revision(mountRevision);
            masterRequest.mutable_stores_to_add()->MergeFrom(commitRequest->stores_to_add());
            masterRequest.mutable_stores_to_remove()->MergeFrom(commitRequest->stores_to_remove());

            hiveManager->PostMessage(masterMailbox, masterRequest);
        }

        if (commitRequest->has_transaction_id()) {
            auto transactionId = FromProto<TTransactionId>(commitRequest->transaction_id());

            TReqHydraAbortTransaction masterRequest;
            ToProto(masterRequest.mutable_transaction_id(), transactionId);
            ToProto(masterRequest.mutable_mutation_id(), NRpc::NullMutationId);

            hiveManager->PostMessage(masterMailbox, masterRequest);
        }
    }

    void HydraOnTabletStoresUpdated(TRspUpdateTabletStores* response)
    {
        auto tabletId = FromProto<TTabletId>(response->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto mountRevision = response->mount_revision();
        if (mountRevision != tablet->GetMountRevision()) {
            return;
        }

        const auto& storeManager = tablet->GetStoreManager();

        if (response->has_error()) {
            auto error = FromProto<TError>(response->error());
            LOG_WARNING_UNLESS(IsRecovery(), error, "Error updating tablet stores (TabletId: %v)",
                tabletId);

            for (const auto& descriptor : response->stores_to_remove()) {
                auto storeId = FromProto<TStoreId>(descriptor.store_id());
                auto store = tablet->GetStore(storeId);

                YCHECK(store->GetStoreState() == EStoreState::RemoveCommitting);
                switch (store->GetType()) {
                    case EStoreType::SortedDynamic:
                    case EStoreType::OrderedDynamic:
                        store->SetStoreState(EStoreState::PassiveDynamic);
                        break;
                    case EStoreType::SortedChunk:
                    case EStoreType::OrderedChunk:
                        store->SetStoreState(EStoreState::Persistent);
                        break;
                    default:
                        YUNREACHABLE();
                }

                if (IsLeader()) {
                    storeManager->BackoffStoreRemoval(store);
                }
            }

            if (IsLeader()) {
                CheckIfFullyFlushed(tablet);
            }
            return;
        }

        auto mountConfig = tablet->GetConfig();
        auto inMemoryManager = Bootstrap_->GetInMemoryManager();

        // NB: Must handle store removals before store additions since
        // row index map forbids having multiple stores with the same starting row index.
        // But before proceeding to removals, we must take care of backing stores.
        yhash_map<TStoreId, IDynamicStorePtr> idToBackingStore;
        auto registerBackingStore = [&] (const IStorePtr& store) {
            YCHECK(idToBackingStore.insert(std::make_pair(store->GetId(), store->AsDynamic())).second);
        };
        auto getBackingStore = [&] (const TStoreId& id) {
            auto it = idToBackingStore.find(id);
            YCHECK(it != idToBackingStore.end());
            return it->second;
        };

        if (!IsRecovery()) {
            for (const auto& descriptor : response->stores_to_add()) {
                if (descriptor.has_backing_store_id()) {
                    auto backingStoreId = FromProto<TStoreId>(descriptor.backing_store_id());
                    auto backingStore = tablet->GetStore(backingStoreId);
                    registerBackingStore(backingStore);
                }
            }
        }

        std::vector<TStoreId> removedStoreIds;
        for (const auto& descriptor : response->stores_to_remove()) {
            auto storeId = FromProto<TStoreId>(descriptor.store_id());
            removedStoreIds.push_back(storeId);

            auto store = tablet->GetStore(storeId);
            storeManager->RemoveStore(store);

            LOG_DEBUG_UNLESS(IsRecovery(), "Store removed (TabletId: %v, StoreId: %v)",
                tabletId,
                storeId);
        }

        std::vector<TStoreId> addedStoreIds;
        for (const auto& descriptor : response->stores_to_add()) {
            auto storeType = EStoreType(descriptor.store_type());
            auto storeId = FromProto<TChunkId>(descriptor.store_id());
            addedStoreIds.push_back(storeId);

            auto store = CreateStore(tablet, storeType, storeId, &descriptor)->AsChunk();
            storeManager->AddStore(store, false);

            TStoreId backingStoreId;
            if (!IsRecovery() && descriptor.has_backing_store_id()) {
                backingStoreId = FromProto<TStoreId>(descriptor.backing_store_id());
                auto backingStore = getBackingStore(backingStoreId);
                SetBackingStore(tablet, store, backingStore);
            }

            LOG_DEBUG_UNLESS(IsRecovery(), "Store added (TabletId: %v, StoreId: %v, BackingStoreId: %v)",
                tabletId,
                storeId,
                backingStoreId);
        }

        LOG_INFO_UNLESS(IsRecovery(), "Tablet stores updated successfully "
            "(TabletId: %v, AddedStoreIds: %v, RemovedStoreIds: %v)",
            tabletId,
            addedStoreIds,
            removedStoreIds);

        UpdateTabletSnapshot(tablet);
        if (IsLeader()) {
            CheckIfFullyFlushed(tablet);
        }
    }

    void HydraSplitPartition(TReqSplitPartition* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        YCHECK(tablet->IsSorted());

        auto mountRevision = request->mount_revision();
        if (mountRevision != tablet->GetMountRevision()) {
            return;
        }

        auto partitionId = FromProto<TPartitionId>(request->partition_id());
        auto* partition = tablet->GetPartition(partitionId);

        auto pivotKeys = FromProto<std::vector<TOwningKey>>(request->pivot_keys());

        int partitionIndex = partition->GetIndex();
        i64 partitionDataSize = partition->GetUncompressedDataSize();

        auto storeManager = tablet->GetStoreManager()->AsSorted();
        bool result = storeManager->SplitPartition(partition->GetIndex(), pivotKeys);
        if (!result) {
            LOG_INFO_UNLESS(IsRecovery(), "Partition split failed (TabletId: %v, PartitionId: %v, Keys: %v)",
                tablet->GetId(),
                partitionId,
                JoinToString(pivotKeys, STRINGBUF(" .. ")));
            return;
        }

        UpdateTabletSnapshot(tablet);

        LOG_INFO_UNLESS(IsRecovery(), "Partition split (TabletId: %v, OriginalPartitionId: %v, "
            "ResultingPartitionIds: %v, DataSize: %v, Keys: %v)",
            tablet->GetId(),
            partitionId,
            MakeFormattableRange(
                MakeRange(
                    tablet->PartitionList().data() + partitionIndex,
                    tablet->PartitionList().data() + partitionIndex + pivotKeys.size()),
                TPartitionIdFormatter()),
            partitionDataSize,
            JoinToString(pivotKeys, STRINGBUF(" .. ")));
    }

    void HydraMergePartitions(TReqMergePartitions* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        YCHECK(tablet->IsSorted());

        auto mountRevision = request->mount_revision();
        if (mountRevision != tablet->GetMountRevision()) {
            return;
        }

        auto firstPartitionId = FromProto<TPartitionId>(request->partition_id());
        auto* firstPartition = tablet->GetPartition(firstPartitionId);

        int firstPartitionIndex = firstPartition->GetIndex();
        int lastPartitionIndex = firstPartitionIndex + request->partition_count() - 1;

        auto originalPartitionIds = Format("%v",
            MakeFormattableRange(
                MakeRange(
                    tablet->PartitionList().data() + firstPartitionIndex,
                    tablet->PartitionList().data() + lastPartitionIndex + 1),
                TPartitionIdFormatter()));

        i64 partitionsDataSize = 0;
        for (int index = firstPartitionIndex; index <= lastPartitionIndex; ++index) {
            const auto& partition = tablet->PartitionList()[index];
            partitionsDataSize += partition->GetUncompressedDataSize();
        }

        auto storeManager = tablet->GetStoreManager()->AsSorted();
        storeManager->MergePartitions(
            firstPartition->GetIndex(),
            firstPartition->GetIndex() + request->partition_count() - 1);

        UpdateTabletSnapshot(tablet);

        LOG_INFO_UNLESS(IsRecovery(), "Partitions merged (TabletId: %v, OriginalPartitionIds: %v, "
            "ResultingPartitionId: %v, DataSize: %v)",
            tablet->GetId(),
            originalPartitionIds,
            tablet->PartitionList()[firstPartitionIndex]->GetId(),
            partitionsDataSize);
    }

    void HydraUpdatePartitionSampleKeys(TReqUpdatePartitionSampleKeys* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        YCHECK(tablet->IsSorted());

        auto mountRevision = request->mount_revision();
        if (mountRevision != tablet->GetMountRevision()) {
            return;
        }

        auto partitionId = FromProto<TPartitionId>(request->partition_id());
        auto* partition = tablet->FindPartition(partitionId);
        if (!partition) {
            return;
        }

        // COMPAT(babenko)
        TSharedRange<TKey> sampleKeys;
        if (request->legacy_sample_keys_size() > 0) {
            auto rowBuffer = New<TRowBuffer>();
            std::vector<TKey> keys;
            for (const auto& protoKey : request->legacy_sample_keys()) {
                keys.push_back(FromProto<TKey>(protoKey, rowBuffer));
            }
            sampleKeys = MakeSharedRange(std::move(keys), std::move(rowBuffer));
        } else {
            TWireProtocolReader reader(TSharedRef::FromString(request->sample_keys()));
            sampleKeys = CaptureRows<TSampleKeyListTag>(reader.ReadUnversionedRowset());
        }

        auto storeManager = tablet->GetStoreManager()->AsSorted();
        storeManager->UpdatePartitionSampleKeys(partition, sampleKeys);

        UpdateTabletSnapshot(tablet);

        LOG_INFO_UNLESS(IsRecovery(), "Partition sample keys updated (TabletId: %v, PartitionId: %v, SampleKeyCount: %v)",
            tabletId,
            partition->GetId(),
            sampleKeys.Size());
    }


    template <class TRef>
    void HandleRowOnTransactionPrepare(TTransaction* transaction, const TRef& rowRef)
    {
        // NB: Don't call ValidateAndDiscardRowRef, row refs are just scanned.
        if (ValidateRowRef(rowRef)) {
            rowRef.StoreManager->PrepareRow(transaction, rowRef);
        }
    }

    template <class TLockedRows, class TPrelockedRows>
    void HandleRowsOnTransactionPrepare(TTransaction* transaction, TLockedRows& lockedRows, TPrelockedRows& prelockedRows)
    {
        for (const auto& rowRef : lockedRows) {
            HandleRowOnTransactionPrepare(transaction, rowRef);
        }

        for (auto it = prelockedRows.begin();
             it != prelockedRows.end();
             prelockedRows.move_forward(it))
        {
            HandleRowOnTransactionPrepare(transaction, *it);
        }
    }
    
    void OnTransactionPrepared(TTransaction* transaction)
    {
        auto lockedSortedRowCount = transaction->LockedSortedRows().size();
        auto prelockedSortedRowCount = transaction->PrelockedSortedRows().size();
        auto lockedOrderedRowCount = transaction->LockedOrderedRows().size();
        auto prelockedOrderedRowCount = transaction->PrelockedOrderedRows().size();

        HandleRowsOnTransactionPrepare(transaction, transaction->LockedSortedRows(), transaction->PrelockedSortedRows());
        HandleRowsOnTransactionPrepare(transaction, transaction->LockedOrderedRows(), transaction->PrelockedOrderedRows());

        LOG_DEBUG_UNLESS(IsRecovery(), "Locked rows prepared (TransactionId: %v, "
            "SortedLockedRows: %v, SortedPrelockedRows: %v, "
            "OrderedLockedRows: %v, OrderedPrelockedRows: %v)",
            transaction->GetId(),
            lockedSortedRowCount,
            prelockedSortedRowCount,
            lockedOrderedRowCount,
            prelockedOrderedRowCount);
    }


    template <class TLockedRows, class TPrelockedRows>
    void HandleRowsOnTransactionCommit(TTransaction* transaction, TLockedRows& lockedRows, TPrelockedRows& prelockedRows)
    {
        YCHECK(prelockedRows.empty());
        for (const auto& rowRef : lockedRows) {
            if (ValidateAndDiscardRowRef(rowRef)) {
                rowRef.StoreManager->CommitRow(transaction, rowRef);
            }
        }
        lockedRows.clear();
    }

    void OnTransactionCommitted(TTransaction* transaction)
    {
        auto lockedSortedRowCount = transaction->LockedSortedRows().size();
        auto lockedOrderedRowCount = transaction->LockedOrderedRows().size();

        HandleRowsOnTransactionCommit(transaction, transaction->LockedSortedRows(), transaction->PrelockedSortedRows());
        HandleRowsOnTransactionCommit(transaction, transaction->LockedOrderedRows(), transaction->PrelockedOrderedRows());

        LOG_DEBUG_UNLESS(IsRecovery(), "Locked rows prepared (TransactionId: %v, "
            "SortedRows: %v, OrderedRows: %v)",
            transaction->GetId(),
            lockedSortedRowCount,
            lockedOrderedRowCount);

        UpdateLastCommittedTimestamp(transaction->GetCommitTimestamp());

        OnTransactionFinished(transaction);
    }


    template <class TLockedRows, class TPrelockedRows>
    void HandleRowsOnTransactionAbort(TTransaction* transaction, TLockedRows& lockedRows, TPrelockedRows& prelockedRows)
    {
        YCHECK(prelockedRows.empty());
        for (const auto& rowRef : lockedRows) {
            if (ValidateAndDiscardRowRef(rowRef)) {
               rowRef.StoreManager->AbortRow(transaction, rowRef);
            }
        }
        lockedRows.clear();
    }
    
    void OnTransactionAborted(TTransaction* transaction)
    {
        auto lockedSortedRowCount = transaction->LockedSortedRows().size();
        auto lockedOrderedRowCount = transaction->LockedOrderedRows().size();

        HandleRowsOnTransactionAbort(transaction, transaction->LockedSortedRows(), transaction->PrelockedSortedRows());
        HandleRowsOnTransactionAbort(transaction, transaction->LockedOrderedRows(), transaction->PrelockedOrderedRows());

        LOG_DEBUG_UNLESS(IsRecovery(), "Locked rows aborted (TransactionId: %v, "
            "SortedRows: %v, OrderedRows: %v)",
            transaction->GetId(),
            lockedSortedRowCount,
            lockedOrderedRowCount);

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


    void SetStoreOrphaned(TTablet* tablet, IStorePtr store)
    {
        if (store->GetStoreState() == EStoreState::Orphaned) {
            return;
        }

        store->SetStoreState(EStoreState::Orphaned);

        if (!store->IsDynamic()) {
            return;
        }
        
        auto dynamicStore = store->AsDynamic();
        auto lockCount = dynamicStore->GetLockCount();
        if (lockCount > 0) {
            YCHECK(OrphanedStores_.insert(dynamicStore).second);
            LOG_INFO_UNLESS(IsRecovery(), "Dynamic memory store is orphaned and will be kept "
                "(StoreId: %v, TabletId: %v, LockCount: %v)",
                store->GetId(),
                tablet->GetId(),
                lockCount);
        }
    }


    template <class TRowRef>
    bool ValidateRowRef(const TRowRef& rowRef)
    {
        auto* store = rowRef.Store;
        return store->GetStoreState() != EStoreState::Orphaned;
    }

    template <class TRowRef>
    bool ValidateAndDiscardRowRef(const TRowRef& rowRef)
    {
        auto* store = rowRef.Store;
        if (store->GetStoreState() != EStoreState::Orphaned) {
            return true;
        }

        auto lockCount = store->Unlock();
        if (lockCount == 0) {
            LOG_INFO_UNLESS(IsRecovery(), "Store unlocked and will be dropped (StoreId: %v)",
                store->GetId());
            YCHECK(OrphanedStores_.erase(store) == 1);
        }

        return false;
    }


    void ExecuteSingleRead(
        TTabletSnapshotPtr tabletSnapshot,
        TTimestamp timestamp,
        const TWorkloadDescriptor& workloadDescriptor,
        TWireProtocolReader* reader,
        TWireProtocolWriter* writer)
    {
        auto command = reader->ReadCommand();
        switch (command) {
            case EWireProtocolCommand::LookupRows:
                LookupRows(
                    std::move(tabletSnapshot),
                    timestamp,
                    workloadDescriptor,
                    reader,
                    writer);
                break;

            default:
                THROW_ERROR_EXCEPTION("Unknown read command %v",
                    command);
        }
    }


    void WriteAtomic(
        TTablet* tablet,
        const TTransactionId& transactionId,
        TWireProtocolReader* reader,
        TFuture<void>* commitResult)
    {
        const auto& tabletId = tablet->GetId();
        const auto& storeManager = tablet->GetStoreManager();

        auto transactionManager = Slot_->GetTransactionManager();
        auto* transaction = transactionManager->GetTransactionOrThrow(transactionId);
        ValidateTransactionActive(transaction);

        auto prelockedSortedBefore = transaction->PrelockedSortedRows().size();
        auto prelockedOrderedBefore = transaction->PrelockedOrderedRows().size();
        auto readerBegin = reader->GetCurrent();

        TError error;
        TNullable<TRowBlockedException> rowBlockedEx;

        while (!reader->IsFinished()) {
            const char* readerCheckpoint = reader->GetCurrent();
            auto rewindReader = [&] () {
                reader->SetCurrent(readerCheckpoint);
            };
            try {
                storeManager->ExecuteAtomicWrite(tablet, transaction, reader, true);
            } catch (const TRowBlockedException& ex) {
                rewindReader();
                rowBlockedEx = ex;
                break;
            } catch (const std::exception& ex) {
                rewindReader();
                error = ex;
                break;
            }
        }

        auto prelockedSortedAfter = transaction->PrelockedSortedRows().size();
        auto prelockedOrderedAfter = transaction->PrelockedOrderedRows().size();

        auto prelockedSortedDelta = prelockedSortedAfter - prelockedSortedBefore;
        auto prelockedOrderedDelta = prelockedOrderedAfter - prelockedOrderedBefore;

        if (prelockedSortedDelta + prelockedOrderedDelta > 0) {
            LOG_DEBUG("Rows prelocked (TransactionId: %v, TabletId: %v, SortedRows: %v, OrderedRows: %v)",
                transactionId,
                tabletId,
                prelockedSortedDelta,
                prelockedOrderedDelta);

            auto readerEnd = reader->GetCurrent();
            auto recordData = reader->Slice(readerBegin, readerEnd);
            auto compressedRecordData = ChangelogCodec_->Compress(recordData);
            auto writeRecord = TTransactionWriteRecord{tabletId, recordData};

            TReqExecuteWrite hydraRequest;
            ToProto(hydraRequest.mutable_transaction_id(), transactionId);
            ToProto(hydraRequest.mutable_tablet_id(), tabletId);
            hydraRequest.set_mount_revision(tablet->GetMountRevision());
            hydraRequest.set_codec(static_cast<int>(ChangelogCodec_->GetId()));
            hydraRequest.set_compressed_data(ToString(compressedRecordData));
            *commitResult = CreateMutation(Slot_->GetHydraManager(), hydraRequest)
                ->SetHandler(BIND(
                    &TImpl::HydraLeaderExecuteWriteAtomic,
                    MakeStrong(this),
                    transactionId,
                    prelockedSortedDelta,
                    prelockedOrderedDelta,
                    writeRecord))
                ->Commit()
                 .As<void>();
        }

        // NB: Yielding is now possible.
        // Cannot neither access tablet, nor transaction.

        if (rowBlockedEx) {
            rowBlockedEx->GetStore()->WaitOnBlockedRow(
                rowBlockedEx->GetRow(),
                rowBlockedEx->GetLockMask(),
                rowBlockedEx->GetTimestamp());
        }

        error.ThrowOnError();
    }

    void WriteNonAtomic(
        TTablet* tablet,
        const TTransactionId& transactionId,
        TWireProtocolReader* reader,
        TFuture<void>* commitResult)
    {
        // Get and skip the whole reader content.
        auto begin = reader->GetBegin();
        auto end = reader->GetEnd();
        auto recordData = reader->Slice(begin, end);
        reader->SetCurrent(end);

        auto compressedRecordData = ChangelogCodec_->Compress(recordData);

        TReqExecuteWrite hydraRequest;
        ToProto(hydraRequest.mutable_transaction_id(), transactionId);
        ToProto(hydraRequest.mutable_tablet_id(), tablet->GetId());
        hydraRequest.set_mount_revision(tablet->GetMountRevision());
        hydraRequest.set_codec(static_cast<int>(ChangelogCodec_->GetId()));
        hydraRequest.set_compressed_data(ToString(compressedRecordData));
        *commitResult = CreateMutation(Slot_->GetHydraManager(), hydraRequest)
            ->SetHandler(BIND(
                &TImpl::HydraLeaderExecuteWriteNonAtomic,
                MakeStrong(this),
                tablet->GetId(),
                tablet->GetMountRevision(),
                transactionId,
                recordData))
            ->Commit()
             .As<void>();
    }


    void CheckIfFullyUnlocked(TTablet* tablet)
    {
        if (tablet->GetState() != ETabletState::WaitingForLocks) {
            return;
        }

        if (tablet->GetStoreManager()->HasActiveLocks()) {
            return;
        }

        LOG_INFO_UNLESS(IsRecovery(), "All tablet locks released (TabletId: %v)",
            tablet->GetId());

        tablet->SetState(ETabletState::FlushPending);

        TReqSetTabletState request;
        ToProto(request.mutable_tablet_id(), tablet->GetId());
        request.set_mount_revision(tablet->GetMountRevision());
        request.set_state(static_cast<int>(ETabletState::Flushing));
        CommitTabletMutation(request);
    }

    void CheckIfFullyFlushed(TTablet* tablet)
    {
        if (tablet->GetState() != ETabletState::Flushing) {
            return;
        }

        if (tablet->GetStoreManager()->HasUnflushedStores()) {
            return;
        }

        LOG_INFO_UNLESS(IsRecovery(), "All tablet stores flushed (TabletId: %v)",
            tablet->GetId());

        tablet->SetState(ETabletState::UnmountPending);

        TReqSetTabletState request;
        ToProto(request.mutable_tablet_id(), tablet->GetId());
        request.set_mount_revision(tablet->GetMountRevision());
        request.set_state(static_cast<int>(ETabletState::Unmounted));
        CommitTabletMutation(request);
    }


    void RotateStores(TTablet* tablet, bool createNew)
    {
        tablet->GetStoreManager()->Rotate(createNew);
    }


    void CommitTabletMutation(const ::google::protobuf::MessageLite& message)
    {
        auto mutation = CreateMutation(Slot_->GetHydraManager(), message);
        Slot_->GetEpochAutomatonInvoker()->Invoke(
            BIND(IgnoreResult(&TMutation::CommitAndLog), mutation, Logger));
    }

    void PostMasterMutation(const ::google::protobuf::MessageLite& message)
    {
        auto hiveManager = Slot_->GetHiveManager();
        hiveManager->PostMessage(Slot_->GetMasterMailbox(), message);
    }


    void StartTabletEpoch(TTablet* tablet)
    {
        const auto& storeManager = tablet->GetStoreManager();
        storeManager->StartEpoch(Slot_);

        auto slotManager = Bootstrap_->GetTabletSlotManager();
        slotManager->RegisterTabletSnapshot(Slot_, tablet);
    }

    void StopTabletEpoch(TTablet* tablet)
    {
        const auto& storeManager = tablet->GetStoreManager();
        if (storeManager) {
            // Store Manager could be null if snapshot loading is aborted.
            storeManager->StopEpoch(Slot_);
        }

        auto slotManager = Bootstrap_->GetTabletSlotManager();
        slotManager->UnregisterTabletSnapshot(Slot_, tablet);
    }


    void SetBackingStore(TTablet* tablet, IChunkStorePtr store, IDynamicStorePtr backingStore)
    {
        store->SetBackingStore(backingStore);
        LOG_DEBUG("Backing store set (StoreId: %v, BackingStoreId: %v)",
            store->GetId(),
            backingStore->GetId());

        auto callback = BIND([=, this_ = MakeStrong(this)] () {
            VERIFY_THREAD_AFFINITY(AutomatonThread);
            store->SetBackingStore(nullptr);
            LOG_DEBUG("Backing store released (StoreId: %v)", store->GetId());
        });
        TDelayedExecutor::Submit(
            // NB: Submit the callback via the regular automaton invoker, not the epoch one since
            // we need the store to be released even if the epoch ends.
            callback.Via(Slot_->GetAutomatonInvoker()),
            tablet->GetConfig()->BackingStoreRetentionTime);
    }


    void BuildTabletOrchidYson(TTablet* tablet, IYsonConsumer* consumer)
    {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("table_id").Value(tablet->GetTableId())
                .Item("state").Value(tablet->GetState())
                .DoIf(tablet->IsSorted(), [&] (TFluentMap fluent) {
                    fluent
                        .Item("pivot_key").Value(tablet->GetPivotKey())
                        .Item("next_pivot_key").Value(tablet->GetNextPivotKey())
                        .Item("eden").Do(BIND(&TImpl::BuildPartitionOrchidYson, Unretained(this), tablet->GetEden()))
                        .Item("partitions").DoListFor(
                            tablet->PartitionList(), [&] (TFluentList fluent, const std::unique_ptr<TPartition>& partition) {
                                fluent
                                    .Item()
                                    .Do(BIND(&TImpl::BuildPartitionOrchidYson, Unretained(this), partition.get()));
                            });
                })
                .DoIf(!tablet->IsSorted(), [&] (TFluentMap fluent) {
                    fluent
                        .Item("stores").DoMapFor(
                            tablet->StoreIdMap(), [&] (TFluentMap fluent, const std::pair<const TStoreId, IStorePtr>& pair) {
                                const auto& store = pair.second;
                                fluent
                                    .Item(ToString(store->GetId()))
                                    .Do(BIND(&TImpl::BuildStoreOrchidYson, Unretained(this), store));
                            });
                })
            .EndMap();
    }

    void BuildPartitionOrchidYson(TPartition* partition, IYsonConsumer* consumer)
    {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("id").Value(partition->GetId())
                .Item("state").Value(partition->GetState())
                .Item("pivot_key").Value(partition->GetPivotKey())
                .Item("next_pivot_key").Value(partition->GetNextPivotKey())
                .Item("sample_key_count").Value(partition->GetSampleKeys()->Keys.Size())
                .Item("sampling_time").Value(partition->GetSamplingTime())
                .Item("sampling_request_time").Value(partition->GetSamplingRequestTime())
                .Item("compaction_time").Value(partition->GetCompactionTime())
                .Item("uncompressed_data_size").Value(partition->GetUncompressedDataSize())
                .Item("unmerged_row_count").Value(partition->GetUnmergedRowCount())
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
            .BeginAttributes()
                .Item("opaque").Value(true)
            .EndAttributes()
            .BeginMap()
                .Do(BIND(&IStore::BuildOrchidYson, store))
            .EndMap();
    }


    static EMemoryCategory GetMemoryCategoryFromStore(IStorePtr store)
    {
        switch (store->GetType()) {
            case EStoreType::SortedDynamic:
            case EStoreType::OrderedDynamic:
                return EMemoryCategory::TabletDynamic;
            case EStoreType::SortedChunk:
            case EStoreType::OrderedChunk:
                return EMemoryCategory::TabletStatic;
            default:
                YUNREACHABLE();
        }
    }

    static void OnStoreMemoryUsageUpdated(NCellNode::TBootstrap* bootstrap, EMemoryCategory category, i64 delta)
    {
        auto* tracker = bootstrap->GetMemoryUsageTracker();
        if (delta >= 0) {
            tracker->Acquire(category, delta);
        } else {
            tracker->Release(category, -delta);
        }
    }

    void StartMemoryUsageTracking(IStorePtr store)
    {
        store->SubscribeMemoryUsageUpdated(BIND(
            &TImpl::OnStoreMemoryUsageUpdated,
            Bootstrap_,
            GetMemoryCategoryFromStore(store)));
    }

    void ValidateMemoryLimit()
    {
        if (Bootstrap_->GetTabletSlotManager()->IsOutOfMemory()) {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::AllWritesDisabled,
                "Node is out of tablet memory, all writes disabled");
        }
    }

    void ValidateClientTimestamp(const TTransactionId& transactionId)
    {
        auto clientTimestamp = TimestampFromTransactionId(transactionId);
        auto timestampProvider = Bootstrap_->GetMasterClient()->GetConnection()->GetTimestampProvider();
        auto serverTimestamp = timestampProvider->GetLatestTimestamp();
        auto clientInstant = TimestampToInstant(clientTimestamp).first;
        auto serverInstant = TimestampToInstant(serverTimestamp).first;
        if (clientInstant > serverInstant + Config_->ClientTimestampThreshold ||
            clientInstant < serverInstant - Config_->ClientTimestampThreshold)
        {
            THROW_ERROR_EXCEPTION("Transaction timestamp is off limits, check the local clock readings")
                << TErrorAttribute("client_timestamp", clientTimestamp)
                << TErrorAttribute("server_timestamp", serverTimestamp);
        }
    }

    void ValidateTabletStoreLimit(TTablet* tablet)
    {
        auto storeCount = tablet->StoreIdMap().size();
        auto storeLimit = tablet->GetConfig()->MaxStoresPerTablet;
        if (storeCount >= storeLimit) {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::AllWritesDisabled,
                "Too many stores in tablet, all writes disabled")
                << TErrorAttribute("tablet_id", tablet->GetId())
                << TErrorAttribute("store_count", storeCount)
                << TErrorAttribute("store_limit", storeLimit);
        }

        auto overlappingStoreCount = tablet->GetOverlappingStoreCount();
        auto overlappingStoreLimit = tablet->GetConfig()->MaxOverlappingStoreCount;
        if (overlappingStoreCount >= overlappingStoreLimit) {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::AllWritesDisabled,
                "Too many overlapping stores in tablet, all writes disabled")
                << TErrorAttribute("tablet_id", tablet->GetId())
                << TErrorAttribute("overlapping_store_count", overlappingStoreCount)
                << TErrorAttribute("overlapping_store_limit", overlappingStoreLimit);
        }
    }


    void UpdateTabletSnapshot(TTablet* tablet)
    {
        if (!IsRecovery()) {
            auto slotManager = Bootstrap_->GetTabletSlotManager();
            slotManager->RegisterTabletSnapshot(Slot_, tablet);
        }
    }


    void ValidateTabletMounted(TTablet* tablet)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (tablet->GetState() != ETabletState::Mounted) {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::TabletNotMounted,
                "Tablet %v is not in \"mounted\" state",
                tablet->GetId());
        }
    }

    void ValidateTransactionActive(TTransaction* transaction)
    {
        if (transaction->GetState() != ETransactionState::Active) {
            transaction->ThrowInvalidState();
        }
    }


    TTableMountConfigPtr DeserializeTableMountConfig(const TYsonString& str, const TTabletId& tabletId)
    {
        try {
            return ConvertTo<TTableMountConfigPtr>(str);
        } catch (const std::exception& ex) {
            LOG_ERROR_UNLESS(IsRecovery(), ex, "Error deserializing tablet mount config (TabletId: %v)",
                 tabletId);
            return New<TTableMountConfig>();
        }
    }

    TTabletWriterOptionsPtr DeserializeTabletWriterOptions(const TYsonString& str, const TTabletId& tabletId)
    {
        try {
            return ConvertTo<TTabletWriterOptionsPtr>(str);
        } catch (const std::exception& ex) {
            LOG_ERROR_UNLESS(IsRecovery(), ex, "Error deserializing writer options (TabletId: %v)",
                 tabletId);
            return New<TTabletWriterOptions>();
        }
    }


    void UpdateLastCommittedTimestamp(TTimestamp timestamp)
    {
        LastCommittedTimestamp_ = std::max(LastCommittedTimestamp_, timestamp);
    }

    TTimestamp AdjustCommitTimestamp(TTimestamp timestamp)
    {
        auto adjustedTimestamp = std::max(timestamp, LastCommittedTimestamp_ + 1);
        UpdateLastCommittedTimestamp(adjustedTimestamp);
        return adjustedTimestamp;
    }


    IStoreManagerPtr CreateStoreManager(TTablet* tablet)
    {
        if (tablet->IsSorted()) {
            return New<TSortedStoreManager>(
                Config_,
                tablet,
                &TabletContext_,
                Slot_->GetHydraManager(),
                Bootstrap_->GetInMemoryManager(),
                Bootstrap_->GetMasterClient());
        } else {
            return New<TOrderedStoreManager>(
                Config_,
                tablet,
                &TabletContext_,
                Slot_->GetHydraManager(),
                Bootstrap_->GetInMemoryManager(),
                Bootstrap_->GetMasterClient());
        }
    }

    IStorePtr CreateStore(
        TTablet* tablet,
        EStoreType type,
        const TStoreId& storeId,
        const TAddStoreDescriptor* descriptor)
    {
        auto store = DoCreateStore(tablet, type, storeId, descriptor);
        StartMemoryUsageTracking(store);
        return store;
    }

    IStorePtr DoCreateStore(
        TTablet* tablet,
        EStoreType type,
        const TStoreId& storeId,
        const TAddStoreDescriptor* descriptor)
    {
        switch (type) {
            case EStoreType::SortedChunk: {
                auto store = New<TSortedChunkStore>(
                    Config_,
                    storeId,
                    tablet,
                    Bootstrap_->GetBlockCache(),
                    Bootstrap_->GetChunkRegistry(),
                    Bootstrap_->GetChunkBlockManager(),
                    Bootstrap_->GetMasterClient(),
                    Bootstrap_->GetMasterConnector()->GetLocalDescriptor());
                store->Initialize(descriptor);
                return store;
            }

            case EStoreType::SortedDynamic:
                return New<TSortedDynamicStore>(
                    Config_,
                    storeId,
                    tablet);

            case EStoreType::OrderedChunk: {
                auto store = New<TOrderedChunkStore>(
                    Config_,
                    storeId,
                    tablet,
                    Bootstrap_->GetBlockCache(),
                    Bootstrap_->GetChunkRegistry(),
                    Bootstrap_->GetChunkBlockManager(),
                    Bootstrap_->GetMasterClient(),
                    Bootstrap_->GetMasterConnector()->GetLocalDescriptor());
                store->Initialize(descriptor);
                return store;
            }

            case EStoreType::OrderedDynamic:
                return New<TOrderedDynamicStore>(
                    Config_,
                    storeId,
                    tablet);

            default:
                YUNREACHABLE();
        }
    }

};

DEFINE_ENTITY_MAP_ACCESSORS(TTabletManager::TImpl, Tablet, TTablet, TabletMap_)

///////////////////////////////////////////////////////////////////////////////

TTabletManager::TTabletManager(
    TTabletManagerConfigPtr config,
    TTabletSlotPtr slot,
    TBootstrap* bootstrap)
    : Impl_(New<TImpl>(
        config,
        slot,
        bootstrap))
{ }

TTabletManager::~TTabletManager() = default;

void TTabletManager::Initialize()
{
    Impl_->Initialize();
}

TTablet* TTabletManager::GetTabletOrThrow(const TTabletId& id)
{
    return Impl_->GetTabletOrThrow(id);
}

void TTabletManager::Read(
    TTabletSnapshotPtr tabletSnapshot,
    TTimestamp timestamp,
    const TWorkloadDescriptor& workloadDescriptor,
    TWireProtocolReader* reader,
    TWireProtocolWriter* writer)
{
    Impl_->Read(
        std::move(tabletSnapshot),
        timestamp,
        workloadDescriptor,
        reader,
        writer);
}

void TTabletManager::Write(
    TTabletSnapshotPtr tabletSnapshot,
    const TTransactionId& transactionId,
    TWireProtocolReader* reader,
    TFuture<void>* commitResult)
{
    return Impl_->Write(
        std::move(tabletSnapshot),
        transactionId,
        reader,
        commitResult);
}

void TTabletManager::ScheduleStoreRotation(TTablet* tablet)
{
    Impl_->ScheduleStoreRotation(tablet);
}

IYPathServicePtr TTabletManager::GetOrchidService()
{
    return Impl_->GetOrchidService();
}

DELEGATE_ENTITY_MAP_ACCESSORS(TTabletManager, Tablet, TTablet, *Impl_)

///////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
