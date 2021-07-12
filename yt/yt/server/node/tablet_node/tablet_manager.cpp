#include "tablet_manager.h"
#include "private.h"
#include "automaton.h"
#include "bootstrap.h"
#include "distributed_throttler_manager.h"
#include "sorted_chunk_store.h"
#include "ordered_chunk_store.h"
#include "sorted_dynamic_store.h"
#include "ordered_dynamic_store.h"
#include "hunk_chunk.h"
#include "replicated_store_manager.h"
#include "in_memory_manager.h"
#include "partition.h"
#include "security_manager.h"
#include "slot_manager.h"
#include "sorted_store_manager.h"
#include "ordered_store_manager.h"
#include "structured_logger.h"
#include "tablet.h"
#include "tablet_slot.h"
#include "transaction.h"
#include "transaction_manager.h"
#include "table_replicator.h"
#include "tablet_profiling.h"
#include "tablet_snapshot_store.h"

#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/server/node/data_node/chunk_block_manager.h>
#include <yt/yt/server/node/data_node/legacy_master_connector.h>

#include <yt/yt/server/node/tablet_node/transaction_manager.h>

#include <yt/yt/server/lib/hive/hive_manager.h>
#include <yt/yt/server/lib/hive/transaction_supervisor.h>
#include <yt/yt/server/lib/hive/helpers.h>

#include <yt/yt/server/lib/hydra/distributed_hydra_manager.h>
#include <yt/yt/server/lib/hydra/mutation.h>
#include <yt/yt/server/lib/hydra/mutation_context.h>

#include <yt/yt/server/lib/misc/profiling_helpers.h>

#include <yt/yt/server/lib/tablet_node/proto/tablet_manager.pb.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

#include <yt/yt/ytlib/chunk_client/block_cache.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/yt/ytlib/distributed_throttler/config.h>

#include <yt/yt/ytlib/api/native/transaction.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/yt/ytlib/tablet_client/config.h>

#include <yt/yt/ytlib/transaction_client/action.h>
#include <yt/yt/ytlib/transaction_client/helpers.h>

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/client/table_client/wire_protocol.h>
#include <yt/yt_proto/yt/client/table_chunk_format/proto/wire_protocol.pb.h>
#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/client/transaction_client/helpers.h>
#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/core/concurrency/async_semaphore.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/compression/codec.h>

#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/optional.h>
#include <yt/yt/core/misc/ring_queue.h>
#include <yt/yt/core/misc/small_vector.h>
#include <yt/yt/core/misc/small_flat_map.h>
#include <yt/yt/core/misc/string.h>
#include <yt/yt/core/misc/tls_cache.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/virtual.h>

#include <util/generic/cast.h>
#include <util/generic/algorithm.h>

namespace NYT::NTabletNode {

using namespace NCompression;
using namespace NConcurrency;
using namespace NYson;
using namespace NYTree;
using namespace NHydra;
using namespace NClusterNode;
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
using namespace NHiveServer;
using namespace NHiveServer::NProto;
using namespace NQueryClient;
using namespace NApi;
using namespace NProfiling;
using namespace NDistributedThrottler;

////////////////////////////////////////////////////////////////////////////////

class TTabletManager::TImpl
    : public TTabletAutomatonPart
{
public:
    explicit TImpl(
        TTabletManagerConfigPtr config,
        ITabletSlotPtr slot,
        IBootstrap* bootstrap)
        : TTabletAutomatonPart(
            slot,
            bootstrap)
        , Config_(config)
        , ChangelogCodec_(GetCodec(Config_->ChangelogCodec))
        , TabletContext_(this)
        , TabletMap_(TTabletMapTraits(this))
        , WriteLogsMemoryTrackerGuard_(TMemoryUsageTrackerGuard::Acquire(
            Bootstrap_
                ->GetMemoryUsageTracker()
                ->WithCategory(EMemoryCategory::TabletDynamic),
            0 /*size*/,
            MemoryUsageGranularity))
        , DistributedThrottlerManager_(
            CreateDistributedThrottlerManager(Bootstrap_, Slot_->GetCellId()))
        , DecommissionCheckExecutor_(New<TPeriodicExecutor>(
            Slot_->GetAutomatonInvoker(),
            BIND(&TImpl::OnCheckTabletCellDecommission, MakeWeak(this)),
            Config_->TabletCellDecommissionCheckPeriod))
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
        RegisterMethod(BIND(&TImpl::HydraFreezeTablet, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraUnfreezeTablet, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraSetTabletState, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraFollowerWriteRows, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraTrimRows, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraLockTablet, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraReportTabletLocked, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraUnlockTablet, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraRotateStore, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraSplitPartition, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraMergePartitions, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraUpdatePartitionSampleKeys, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraAddTableReplica, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraRemoveTableReplica, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraSetTableReplicaEnabled, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraAlterTableReplica, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraDecommissionTabletCell, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraOnTabletCellDecommissioned, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraOnDynamicStoreAllocated, Unretained(this)));
    }

    void Initialize()
    {
        const auto& transactionManager = Slot_->GetTransactionManager();

        transactionManager->SubscribeTransactionPrepared(BIND(&TImpl::OnTransactionPrepared, MakeStrong(this)));
        transactionManager->SubscribeTransactionCommitted(BIND(&TImpl::OnTransactionCommitted, MakeStrong(this)));
        transactionManager->SubscribeTransactionSerialized(BIND(&TImpl::OnTransactionSerialized, MakeStrong(this)));
        transactionManager->SubscribeTransactionAborted(BIND(&TImpl::OnTransactionAborted, MakeStrong(this)));
        transactionManager->SubscribeTransactionTransientReset(BIND(&TImpl::OnTransactionTransientReset, MakeStrong(this)));
        transactionManager->RegisterTransactionActionHandlers(
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraPrepareReplicateRows, MakeStrong(this))),
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraCommitReplicateRows, MakeStrong(this))),
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraAbortReplicateRows, MakeStrong(this))));
        transactionManager->RegisterTransactionActionHandlers(
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraPrepareUpdateTabletStores, MakeStrong(this))),
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraCommitUpdateTabletStores, MakeStrong(this))),
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraAbortUpdateTabletStores, MakeStrong(this))));
    }


    TTablet* GetTabletOrThrow(TTabletId id)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* tablet = FindTablet(id);
        if (!tablet) {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::NoSuchTablet,
                "No such tablet %v",
                id)
                << TErrorAttribute("tablet_id", id);
        }
        return tablet;
    }

    void Write(
        const TTabletSnapshotPtr& tabletSnapshot,
        TTransactionId transactionId,
        TTimestamp transactionStartTimestamp,
        TDuration transactionTimeout,
        TTransactionSignature signature,
        int rowCount,
        size_t dataWeight,
        bool versioned,
        const TSyncReplicaIdList& syncReplicaIds,
        TWireProtocolReader* reader,
        TFuture<void>* commitResult)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TTablet* tablet = nullptr;
        const auto& transactionManager = Slot_->GetTransactionManager();

        auto atomicity = AtomicityFromTransactionId(transactionId);
        if (atomicity == EAtomicity::None) {
            ValidateClientTimestamp(transactionId);
        }

        tabletSnapshot->TabletRuntimeData->ModificationTime = NProfiling::GetInstant();

        auto actualizeTablet = [&] {
            if (!tablet) {
                tablet = GetTabletOrThrow(tabletSnapshot->TabletId);
                tablet->ValidateMountRevision(tabletSnapshot->MountRevision);
                ValidateTabletMounted(tablet);
            }
        };

        actualizeTablet();

        if (atomicity == EAtomicity::Full) {
            const auto& lockManager = tablet->GetLockManager();
            auto error = lockManager->ValidateTransactionConflict(transactionStartTimestamp);
            if (!error.IsOK()) {
                THROW_ERROR error
                    << TErrorAttribute("tablet_id", tablet->GetId())
                    << TErrorAttribute("transaction_id", transactionId);
            }
        }

        while (!reader->IsFinished()) {
            // NB: No yielding beyond this point.
            // May access tablet and transaction.

            actualizeTablet();

            ValidateTabletStoreLimit(tablet);

            auto poolTag = Slot_->GetDynamicOptions()->EnableTabletDynamicMemoryLimit
                ? tablet->GetPoolTagByMemoryCategory(EMemoryCategory::TabletDynamic)
                : std::nullopt;
            ValidateMemoryLimit(poolTag);

            auto tabletId = tablet->GetId();
            const auto& storeManager = tablet->GetStoreManager();

            TTransaction* transaction = nullptr;
            bool transactionIsFresh = false;
            if (atomicity == EAtomicity::Full) {
                transaction = transactionManager->GetOrCreateTransaction(
                    transactionId,
                    transactionStartTimestamp,
                    transactionTimeout,
                    true,
                    &transactionIsFresh);
                ValidateTransactionActive(transaction);
            }

            TWriteContext context;
            context.Phase = EWritePhase::Prelock;
            context.Transaction = transaction;

            auto readerBefore = reader->GetCurrent();
            auto adjustedSignature = signature;
            auto lockless =
                atomicity == EAtomicity::None ||
                tablet->IsPhysicallyOrdered() ||
                tablet->IsReplicated() ||
                versioned;
            if (lockless) {
                // Skip the whole message.
                reader->SetCurrent(reader->GetEnd());
                context.RowCount = rowCount;
                context.DataWeight = dataWeight;
            } else {
                storeManager->ExecuteWrites(reader, &context);
                if (!reader->IsFinished()) {
                    adjustedSignature = 0;
                }
                YT_LOG_DEBUG_IF(context.RowCount > 0, "Rows prelocked (TransactionId: %v, TabletId: %v, RowCount: %v, Signature: %x)",
                    transactionId,
                    tabletId,
                    context.RowCount,
                    adjustedSignature);
            }
            auto readerAfter = reader->GetCurrent();

            if (atomicity == EAtomicity::Full) {
                transaction->SetTransientSignature(transaction->GetTransientSignature() + adjustedSignature);
            }

            if (readerBefore != readerAfter) {
                auto recordData = reader->Slice(readerBefore, readerAfter);
                auto compressedRecordData = ChangelogCodec_->Compress(recordData);
                TTransactionWriteRecord writeRecord(tabletId, recordData, context.RowCount, context.DataWeight, syncReplicaIds);

                PrelockedTablets_.push(tablet);
                LockTablet(tablet);

                TReqWriteRows hydraRequest;
                ToProto(hydraRequest.mutable_transaction_id(), transactionId);
                hydraRequest.set_transaction_start_timestamp(transactionStartTimestamp);
                hydraRequest.set_transaction_timeout(ToProto<i64>(transactionTimeout));
                ToProto(hydraRequest.mutable_tablet_id(), tabletId);
                hydraRequest.set_mount_revision(tablet->GetMountRevision());
                hydraRequest.set_codec(static_cast<int>(ChangelogCodec_->GetId()));
                hydraRequest.set_compressed_data(ToString(compressedRecordData));
                hydraRequest.set_signature(adjustedSignature);
                hydraRequest.set_lockless(lockless);
                hydraRequest.set_row_count(writeRecord.RowCount);
                hydraRequest.set_data_weight(writeRecord.DataWeight);
                ToProto(hydraRequest.mutable_sync_replica_ids(), syncReplicaIds);

                const auto& identity = NRpc::GetCurrentAuthenticationIdentity();
                NRpc::WriteAuthenticationIdentityToProto(&hydraRequest, identity);

                auto mutation = CreateMutation(Slot_->GetHydraManager(), hydraRequest);
                mutation->SetHandler(BIND_DONT_CAPTURE_TRACE_CONTEXT(
                    &TImpl::HydraLeaderWriteRows,
                    MakeStrong(this),
                    transactionId,
                    tablet->GetMountRevision(),
                    adjustedSignature,
                    lockless,
                    writeRecord,
                    identity));
                mutation->SetCurrentTraceContext();
                *commitResult = mutation->Commit().As<void>();

                auto counters = tablet->GetTableProfiler()->GetWriteCounters(GetCurrentProfilingUser());
                counters->RowCount.Increment(writeRecord.RowCount);
                counters->DataWeight.Increment(writeRecord.DataWeight);
            } else if (transactionIsFresh) {
                transactionManager->DropTransaction(transaction);
            }

            // NB: Yielding is now possible.
            // Cannot neither access tablet, nor transaction.
            if (context.BlockedStore) {
                context.BlockedStore->WaitOnBlockedRow(
                    context.BlockedRow,
                    context.BlockedLockMask,
                    context.BlockedTimestamp);
                tablet = nullptr;
            }

            context.Error.ThrowOnError();
        }
    }

    TFuture<void> Trim(
        const TTabletSnapshotPtr& tabletSnapshot,
        i64 trimmedRowCount)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        try {
            auto* tablet = GetTabletOrThrow(tabletSnapshot->TabletId);

            if (tablet->IsReplicated()) {
                THROW_ERROR_EXCEPTION("Cannot trim a replicated table tablet");
            }

            tablet->ValidateMountRevision(tabletSnapshot->MountRevision);
            ValidateTabletMounted(tablet);

            i64 totalRowCount = tablet->GetTotalRowCount();
            if (trimmedRowCount > totalRowCount) {
                THROW_ERROR_EXCEPTION("Cannot trim tablet %v at row %v since it only has %v row(s)",
                    tablet->GetId(),
                    trimmedRowCount,
                    totalRowCount);
            }

            NProto::TReqTrimRows hydraRequest;
            ToProto(hydraRequest.mutable_tablet_id(), tablet->GetId());
            hydraRequest.set_mount_revision(tablet->GetMountRevision());
            hydraRequest.set_trimmed_row_count(trimmedRowCount);

            auto mutation = CreateMutation(Slot_->GetHydraManager(), hydraRequest);
            mutation->SetCurrentTraceContext();
            return mutation->Commit().As<void>();
        } catch (const std::exception& ex) {
            return MakeFuture(TError(ex));
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

    void ReleaseBackingStore(const IChunkStorePtr& store)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (store->HasBackingStore()) {
            store->SetBackingStore(nullptr);
            YT_LOG_DEBUG("Backing store released (StoreId: %v)", store->GetId());
            // XXX(ifsmirnov): uncomment when tablet id is stored in TStoreBase.
            // store->GetTablet()->GetStructuredLogger()->OnBackingStoreReleased(store);
        }
    }

    TFuture<void> CommitTabletStoresUpdateTransaction(
        TTablet* tablet,
        const ITransactionPtr& transaction)
    {
        YT_LOG_DEBUG("Acquiring tablet stores commit semaphore (%v, TransactionId: %v)",
            tablet->GetLoggingTag(),
            transaction->GetId());

        auto promise = NewPromise<void>();
        tablet
            ->GetStoresUpdateCommitSemaphore()
            ->AsyncAcquire(
                BIND(&TImpl::OnStoresUpdateCommitSemaphoreAcquired, MakeWeak(this), tablet, transaction, promise),
                tablet->GetEpochAutomatonInvoker());
        return promise;
    }

    IYPathServicePtr GetOrchidService()
    {
        return OrchidService_;
    }

    ETabletCellLifeStage GetTabletCellLifeStage() const
    {
        return CellLifeStage_;
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

        virtual std::vector<TString> GetKeys(i64 limit) const override
        {
            std::vector<TString> keys;
            if (auto owner = Owner_.Lock()) {
                for (const auto& tablet : owner->Tablets()) {
                    if (std::ssize(keys) >= limit) {
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

        virtual IYPathServicePtr FindItemService(TStringBuf key) const override
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

        virtual const TString& GetTabletCellBundleName() override
        {
            return Owner_->Slot_->GetTabletCellBundleName();
        }

        virtual EPeerState GetAutomatonState() override
        {
            return Owner_->Slot_->GetAutomatonState();
        }

        virtual IColumnEvaluatorCachePtr GetColumnEvaluatorCache() override
        {
            return Owner_->Bootstrap_->GetColumnEvaluatorCache();
        }

        virtual NTabletNode::IRowComparerProviderPtr GetRowComparerProvider() override
        {
            return Owner_->Bootstrap_->GetRowComparerProvider();
        }

        virtual TObjectId GenerateId(EObjectType type) override
        {
            return Owner_->Slot_->GenerateId(type);
        }

        virtual IStorePtr CreateStore(
            TTablet* tablet,
            EStoreType type,
            TStoreId storeId,
            const TAddStoreDescriptor* descriptor) override
        {
            return Owner_->CreateStore(tablet, type, storeId, descriptor);
        }

        virtual THunkChunkPtr CreateHunkChunk(
            TTablet* tablet,
            TChunkId chunkId,
            const TAddHunkChunkDescriptor* descriptor) override
        {
            return Owner_->CreateHunkChunk(tablet, chunkId, descriptor);
        }

        virtual TTransactionManagerPtr GetTransactionManager() override
        {
            return Owner_->Slot_->GetTransactionManager();
        }

        virtual NRpc::IServerPtr GetLocalRpcServer() override
        {
            return Owner_->Bootstrap_->GetRpcServer();
        }

        virtual NClusterNode::TNodeMemoryTrackerPtr GetMemoryUsageTracker() override
        {
            return Owner_->Bootstrap_->GetMemoryUsageTracker();
        }

        virtual NNodeTrackerClient::TNodeDescriptor GetLocalDescriptor() override
        {
            return Owner_->Bootstrap_->GetLocalDescriptor();
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

        std::unique_ptr<TTablet> Create(TTabletId id) const
        {
            return std::make_unique<TTablet>(id, &Owner_->TabletContext_);
        }

    private:
        TImpl* const Owner_;

    };

    TTabletContext TabletContext_;
    TEntityMap<TTablet, TTabletMapTraits> TabletMap_;
    ETabletCellLifeStage CellLifeStage_ = ETabletCellLifeStage::Running;

    TRingQueue<TTablet*> PrelockedTablets_;

    THashSet<IDynamicStorePtr> OrphanedStores_;
    THashMap<TTabletId, std::unique_ptr<TTablet>> OrphanedTablets_;

    // NB: Write logs are generally much smaller than dynamic stores,
    // so we don't worry about per-pool management here.
    TMemoryUsageTrackerGuard WriteLogsMemoryTrackerGuard_;

    const IDistributedThrottlerManagerPtr DistributedThrottlerManager_;

    const TPeriodicExecutorPtr DecommissionCheckExecutor_;

    const IYPathServicePtr OrchidService_;

    bool RemoveDynamicStoresFromFrozenTablets_ = false;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    void SaveKeys(TSaveContext& context) const
    {
        TabletMap_.SaveKeys(context);
    }

    void SaveValues(TSaveContext& context) const
    {
        using NYT::Save;

        TabletMap_.SaveValues(context);
        Save(context, CellLifeStage_);
    }

    TCallback<void(TSaveContext&)> SaveAsync()
    {
        std::vector<std::pair<TTabletId, TCallback<void(TSaveContext&)>>> capturedTablets;
        for (auto [tabletId, tablet] : TabletMap_) {
            capturedTablets.emplace_back(tabletId, tablet->AsyncSave());
        }

        return BIND(
            [
                capturedTablets = std::move(capturedTablets)
            ] (TSaveContext& context) {
                using NYT::Save;
                for (const auto& [tabletId, callback] : capturedTablets) {
                    Save(context, tabletId);
                    callback(context);
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

        using NYT::Load;

        TabletMap_.LoadValues(context);

        Load(context, CellLifeStage_);

        Automaton_->RememberReign(static_cast<TReign>(context.GetVersion()));

        // COMPAT(ifsmirnov)
        // NB. Comparison is correct here. Frozen tablets used to have dynamic stores
        // before mentioned reign. We want to remove them when compatibility snapshot is
        // built. For regular snapshots built later this compat is a no-op.
        if (context.GetVersion() >= ETabletReign::DynamicStoreRead) {
            RemoveDynamicStoresFromFrozenTablets_ = true;
        }
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

    virtual void OnAfterSnapshotLoaded() noexcept override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TTabletAutomatonPart::OnAfterSnapshotLoaded();

        // NB: Dynamic stores should be removed before store managers are created.
        // COMPAT(ifsmirnov)
        if (RemoveDynamicStoresFromFrozenTablets_) {
            for (const auto& [id, tablet] : TabletMap_) {
                if (tablet->GetState() == ETabletState::Frozen) {
                    if (auto activeStore = tablet->GetActiveStore()) {
                        tablet->RemoveStore(activeStore);
                        tablet->SetActiveStore(nullptr);
                    }
                }
            }
        }

        for (auto [tabletId, tablet] : TabletMap_) {
            tablet->SetStructuredLogger(
                Bootstrap_->GetStructuredLogger()->CreateLogger(tablet));
            auto storeManager = CreateStoreManager(tablet);
            tablet->SetStoreManager(storeManager);
            tablet->ReconfigureDistributedThrottlers(DistributedThrottlerManager_);
            tablet->FillProfilerTags();
            tablet->UpdateReplicaCounters();
            Bootstrap_->GetStructuredLogger()->OnHeartbeatRequest(
                Slot_->GetTabletManager(),
                true /*initial*/);
        }

        const auto& transactionManager = Slot_->GetTransactionManager();
        auto transactions = transactionManager->GetTransactions();
        for (auto* transaction : transactions) {
            YT_VERIFY(!transaction->GetTransient());

            for (const auto& record : transaction->ImmediateLockedWriteLog()) {
                auto* tablet = FindTablet(record.TabletId);
                if (!tablet) {
                    // NB: Tablet could be missing if it was, e.g., forcefully removed.
                    continue;
                }

                TWireProtocolReader reader(record.Data);
                const auto& storeManager = tablet->GetStoreManager();

                TWriteContext context;
                context.Phase = EWritePhase::Lock;
                context.Transaction = transaction;
                YT_VERIFY(storeManager->ExecuteWrites(&reader, &context));
            }

            for (const auto& record : transaction->ImmediateLocklessWriteLog()) {
                auto* tablet = FindTablet(record.TabletId);
                if (!tablet) {
                    // NB: Tablet could be missing if it was, e.g., forcefully removed.
                    continue;
                }

                LockTablet(tablet);
                transaction->LockedTablets().push_back(tablet);
            }

            for (const auto& record : transaction->DelayedLocklessWriteLog()) {
                auto* tablet = FindTablet(record.TabletId);
                if (!tablet) {
                    // NB: Tablet could be missing if it was, e.g., forcefully removed.
                    continue;
                }

                LockTablet(tablet);
                transaction->LockedTablets().push_back(tablet);

                if (tablet->IsReplicated() && transaction->GetRowsPrepared()) {
                    tablet->SetDelayedLocklessRowCount(tablet->GetDelayedLocklessRowCount() + record.RowCount);
                }
            }

            WriteLogsMemoryTrackerGuard_.IncrementSize(
                GetTransactionWriteLogMemoryUsage(transaction->ImmediateLockedWriteLog()) +
                GetTransactionWriteLogMemoryUsage(transaction->ImmediateLocklessWriteLog()) +
                GetTransactionWriteLogMemoryUsage(transaction->DelayedLocklessWriteLog()));

            if (transaction->IsPrepared()) {
                PrepareLockedRows(transaction);
            }
        }
    }

    virtual void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TTabletAutomatonPart::Clear();

        TabletMap_.Clear();
        OrphanedStores_.clear();
        OrphanedTablets_.clear();
        WriteLogsMemoryTrackerGuard_.SetSize(0);
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

        for (auto [tabletId, tablet] : TabletMap_) {
            CheckIfTabletFullyUnlocked(tablet);
            CheckIfTabletFullyFlushed(tablet);
        }

        DecommissionCheckExecutor_->Start();
    }


    virtual void OnStopLeading() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TTabletAutomatonPart::OnStopLeading();

        StopEpoch();

        while (!PrelockedTablets_.empty()) {
            auto* tablet = PrelockedTablets_.front();
            PrelockedTablets_.pop();
            UnlockTablet(tablet);
        }

        DecommissionCheckExecutor_->Stop();
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
        for (auto [tabletId, tablet] : TabletMap_) {
            StartTabletEpoch(tablet);
        }
    }

    void StopEpoch()
    {
        for (auto [tabletId, tablet] : TabletMap_) {
            StopTabletEpoch(tablet);
        }
    }


    void HydraMountTablet(TReqMountTablet* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto mountRevision = request->mount_revision();
        auto tableId = FromProto<TObjectId>(request->table_id());
        const auto& path = request->path();
        auto schema = FromProto<TTableSchemaPtr>(request->schema());
        auto pivotKey = request->has_pivot_key() ? FromProto<TLegacyOwningKey>(request->pivot_key()) : TLegacyOwningKey();
        auto nextPivotKey = request->has_next_pivot_key() ? FromProto<TLegacyOwningKey>(request->next_pivot_key()) : TLegacyOwningKey();
        auto settings = DeserializeTableSettings(request, tabletId);
        auto atomicity = FromProto<EAtomicity>(request->atomicity());
        auto commitOrdering = FromProto<ECommitOrdering>(request->commit_ordering());
        bool freeze = request->freeze();
        auto upstreamReplicaId = FromProto<TTableReplicaId>(request->upstream_replica_id());
        auto replicaDescriptors = FromProto<std::vector<TTableReplicaDescriptor>>(request->replicas());
        auto retainedTimestamp = request->has_retained_timestamp()
            ? FromProto<TTimestamp>(request->retained_timestamp())
            : MinTimestamp;
        const auto& mountHint = request->mount_hint();

        auto tabletHolder = std::make_unique<TTablet>(
            tabletId,
            settings,
            mountRevision,
            tableId,
            path,
            &TabletContext_,
            schema,
            pivotKey,
            nextPivotKey,
            atomicity,
            commitOrdering,
            upstreamReplicaId,
            retainedTimestamp);

        tabletHolder->ReconfigureDistributedThrottlers(DistributedThrottlerManager_);
        tabletHolder->FillProfilerTags();
        tabletHolder->SetStructuredLogger(
            Bootstrap_->GetStructuredLogger()->CreateLogger(tabletHolder.get()));
        auto* tablet = TabletMap_.Insert(tabletId, std::move(tabletHolder));

        if (tablet->IsPhysicallyOrdered()) {
            tablet->SetTrimmedRowCount(request->trimmed_row_count());
        }

        auto storeManager = CreateStoreManager(tablet);
        tablet->SetStoreManager(storeManager);
        PopulateDynamicStoreIdPool(tablet, request);

        // COMPAT(ifsmirnov)
        const auto* mutationContext = GetCurrentMutationContext();
        storeManager->Mount(
            MakeRange(request->stores()),
            MakeRange(request->hunk_chunks()),
            mutationContext->Request().Reign >= ToUnderlying(ETabletReign::DynamicStoreRead) ? !freeze : true,
            mountHint);

        tablet->SetState(freeze ? ETabletState::Frozen : ETabletState::Mounted);

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Tablet mounted (%v, MountRevision: %llx, Keys: %v .. %v, "
            "StoreCount: %v, HunkChunkCount: %v, PartitionCount: %v, TotalRowCount: %v, TrimmedRowCount: %v, Atomicity: %v, "
            "CommitOrdering: %v, Frozen: %v, UpstreamReplicaId: %v, RetainedTimestamp: %v)",
            tablet->GetLoggingTag(),
            mountRevision,
            pivotKey,
            nextPivotKey,
            request->stores_size(),
            request->hunk_chunks_size(),
            tablet->IsPhysicallySorted() ? std::make_optional(tablet->PartitionList().size()) : std::nullopt,
            tablet->IsPhysicallySorted() ? std::nullopt : std::make_optional(tablet->GetTotalRowCount()),
            tablet->IsPhysicallySorted() ? std::nullopt : std::make_optional(tablet->GetTrimmedRowCount()),
            tablet->GetAtomicity(),
            tablet->GetCommitOrdering(),
            freeze,
            upstreamReplicaId,
            retainedTimestamp);

        for (const auto& descriptor : request->replicas()) {
            AddTableReplica(tablet, descriptor);
        }

        const auto& lockManager = tablet->GetLockManager();

        for (const auto& lock : request->locks()) {
            auto transactionId = FromProto<TTabletId>(lock.transaction_id());
            auto lockTimestamp = static_cast<TTimestamp>(lock.timestamp());
            lockManager->Lock(lockTimestamp, transactionId, true);
        }

        {
            TRspMountTablet response;
            ToProto(response.mutable_tablet_id(), tabletId);
            response.set_frozen(freeze);
            PostMasterMutation(tabletId, response);
        }

        tablet->GetStructuredLogger()->OnFullHeartbeat();

        if (!IsRecovery()) {
            StartTabletEpoch(tablet);
        }
    }

    void HydraUnmountTablet(TReqUnmountTablet* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        if (request->force()) {
            YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Tablet is forcefully unmounted (%v)",
                tablet->GetLoggingTag());

            auto tabletHolder = TabletMap_.Release(tabletId);

            if (tablet->GetTabletLockCount() > 0) {
                SetTabletOrphaned(std::move(tabletHolder));
            } else {
                // Just a formality.
                tablet->SetState(ETabletState::Unmounted);
            }

            for (const auto& [storeId, store] : tablet->StoreIdMap()) {
                SetStoreOrphaned(tablet, store);
            }

            const auto& storeManager = tablet->GetStoreManager();
            for (const auto& store : storeManager->GetLockedStores()) {
                SetStoreOrphaned(tablet, store);
            }

            if (!IsRecovery()) {
                StopTabletEpoch(tablet);
            }
        } else {
            auto state = tablet->GetState();
            if (IsInUnmountWorkflow(state)) {
                YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Requested to unmount a tablet in a wrong state, ignored (State: %v, %v)",
                    state,
                    tablet->GetLoggingTag());
                return;
            }

            YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Unmounting tablet (%v)",
                tablet->GetLoggingTag());

            tablet->SetState(ETabletState::UnmountWaitingForLocks);

            YT_LOG_INFO_IF(IsLeader(), "Waiting for all tablet locks to be released (%v)",
                tablet->GetLoggingTag());

            CheckIfTabletFullyUnlocked(tablet);
        }
    }

    void HydraRemountTablet(TReqRemountTablet* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto settings = DeserializeTableSettings(request, tabletId);

        const auto& storeManager = tablet->GetStoreManager();
        storeManager->Remount(settings);

        tablet->ReconfigureThrottlers();
        tablet->ReconfigureDistributedThrottlers(DistributedThrottlerManager_);
        tablet->FillProfilerTags();
        tablet->UpdateReplicaCounters();
        tablet->GetStructuredLogger()->SetEnabled(settings.MountConfig->EnableStructuredLogger);
        UpdateTabletSnapshot(tablet);

        if (!IsRecovery()) {
            for (auto& [replicaId, replicaInfo] : tablet->Replicas()) {
                StopTableReplicaEpoch(&replicaInfo);
                StartTableReplicaEpoch(tablet, &replicaInfo);
            }
        }

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Tablet remounted (%v)",
            tablet->GetLoggingTag());
    }

    void HydraFreezeTablet(TReqFreezeTablet* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto state = tablet->GetState();
        if (IsInUnmountWorkflow(state) || IsInFreezeWorkflow(state)) {
            YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "Requested to freeze a tablet in a wrong state, ignored (State: %v, %v)",
                state,
                tablet->GetLoggingTag());
            return;
        }

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Freezing tablet (%v)",
            tablet->GetLoggingTag());

        tablet->SetState(ETabletState::FreezeWaitingForLocks);

        YT_LOG_INFO_IF(IsLeader(), "Waiting for all tablet locks to be released (%v)",
            tablet->GetLoggingTag());

        CheckIfTabletFullyUnlocked(tablet);
    }

    void HydraUnfreezeTablet(TReqUnfreezeTablet* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto state = tablet->GetState();
        if (state != ETabletState::Frozen)  {
            YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Requested to unfreeze a tablet in a wrong state, ignored (State: %v, %v)",
                state,
                tablet->GetLoggingTag());
            return;
        }

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Tablet unfrozen (%v)",
            tablet->GetLoggingTag());

        tablet->SetState(ETabletState::Mounted);

        auto mutationReign = GetCurrentMutationContext()->Request().Reign;

        const auto& storeManager = tablet->GetStoreManager();

        PopulateDynamicStoreIdPool(tablet, request);

        // COMPAT(ifsmirnov)
        if (mutationReign >= ToUnderlying(ETabletReign::DynamicStoreRead)) {
            storeManager->Rotate(true);
        }

        storeManager->InitializeRotation();

        // COMPAT(ifsmirnov)
        if (mutationReign >= ToUnderlying(ETabletReign::DynamicStoreRead)) {
            UpdateTabletSnapshot(tablet);
        }

        TRspUnfreezeTablet response;
        ToProto(response.mutable_tablet_id(), tabletId);
        PostMasterMutation(tabletId, response);
    }

    void HydraLockTablet(TReqLockTablet* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }
        auto transactionId = FromProto<TTabletId>(request->lock().transaction_id());
        auto lockTimestamp = static_cast<TTimestamp>(request->lock().timestamp());

        const auto& lockManager = tablet->GetLockManager();
        lockManager->Lock(lockTimestamp, transactionId, /*confirmed*/ false);

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Tablet locked (TabletId: %v, TransactionId: %v)",
            tabletId,
            transactionId);

        CheckIfTabletFullyUnlocked(tablet);
    }

    void HydraReportTabletLocked(TReqReportTabletLocked* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        const auto& lockManager = tablet->GetLockManager();
        auto transactionIds = lockManager->RemoveUnconfirmedTransactions();
        if (transactionIds.empty()) {
            return;
        }

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Tablet lock confirmed (TabletId: %v, TransactionIds: %v)",
            tabletId,
            transactionIds);

        TRspLockTablet response;
        ToProto(response.mutable_tablet_id(), tabletId);
        ToProto(response.mutable_transaction_ids(), transactionIds);
        PostMasterMutation(tabletId, response);
    }

    void HydraUnlockTablet(TReqUnlockTablet* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        // COMPAT(ifsmirnov)
        if (request->has_mount_revision() && request->mount_revision() != 0) {
            auto mountRevision = request->mount_revision();
            if (static_cast<ui64>(mountRevision) != tablet->GetMountRevision()) {
                return;
            }
        }

        auto transactionId = FromProto<TTabletId>(request->transaction_id());
        auto updateMode = FromProto<EUpdateMode>(request->update_mode());

        std::vector<TStoreId> addedStoreIds;
        std::vector<IStorePtr> storesToAdd;
        for (const auto& descriptor : request->stores_to_add()) {
            auto storeType = FromProto<EStoreType>(descriptor.store_type());
            auto storeId = FromProto<TChunkId>(descriptor.store_id());
            addedStoreIds.push_back(storeId);

            auto store = CreateStore(tablet, storeType, storeId, &descriptor)->AsChunk();
            store->Initialize();
            storesToAdd.push_back(std::move(store));
        }

        const auto& storeManager = tablet->GetStoreManager();

        if (updateMode == EUpdateMode::Overwrite) {
            YT_LOG_INFO_IF(IsMutationLoggingEnabled(),
                "All stores of tablet are going to be discarded (%v)",
                tablet->GetLoggingTag());

            tablet->ClearDynamicStoreIdPool();
            PopulateDynamicStoreIdPool(tablet, request);

            storeManager->DiscardAllStores();
        }

        const auto& structuredLogger = tablet->GetStructuredLogger();
        structuredLogger->OnTabletUnlocked(
            MakeRange(storesToAdd),
            updateMode == EUpdateMode::Overwrite,
            transactionId);

        storeManager->BulkAddStores(MakeRange(storesToAdd), /*onMount*/ false);

        const auto& lockManager = tablet->GetLockManager();

        if (tablet->GetAtomicity() == EAtomicity::Full) {
            auto nextEpoch = lockManager->GetEpoch() + 1;
            UpdateTabletSnapshot(tablet, nextEpoch);

            // COMPAT(ifsmirnov)
            auto commitTimestamp = request->has_commit_timestamp()
                ? request->commit_timestamp()
                : MinTimestamp;
            lockManager->Unlock(commitTimestamp, transactionId);
        } else {
            UpdateTabletSnapshot(tablet);
        }

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(),
            "Tablet unlocked (%v, TransactionId: %v, AddedStoreIds: %v, LockManagerEpoch: %v)",
            tablet->GetLoggingTag(),
            transactionId,
            addedStoreIds,
            lockManager->GetEpoch());
    }

    void HydraSetTabletState(TReqSetTabletState* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto mountRevision = request->mount_revision();
        if (static_cast<ui64>(mountRevision) != tablet->GetMountRevision()) {
            return;
        }

        auto requestedState = ETabletState(request->state());

        switch (requestedState) {
            case ETabletState::FreezeFlushing: {
                auto state = tablet->GetState();
                if (IsInUnmountWorkflow(state)) {
                    YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Improper tablet state transition requested, ignored (CurrentState: %v, RequestedState: %v, %v)",
                        state,
                        requestedState,
                        tablet->GetLoggingTag());
                    return;
                }
                // No break intentionally.
            }

            case ETabletState::UnmountFlushing: {
                tablet->SetState(requestedState);

                const auto& storeManager = tablet->GetStoreManager();
                // COMPAT(ifsmirnov)
                if (GetCurrentMutationContext()->Request().Reign >= ToUnderlying(ETabletReign::DynamicStoreRead)) {
                    storeManager->Rotate(false);
                } else if (requestedState == ETabletState::UnmountFlushing ||
                    storeManager->IsFlushNeeded())
                {
                    storeManager->Rotate(requestedState == ETabletState::FreezeFlushing);
                }

                YT_LOG_INFO_IF(IsLeader(), "Waiting for all tablet stores to be flushed (%v, NewState: %v)",
                    tablet->GetLoggingTag(),
                    requestedState);

                CheckIfTabletFullyFlushed(tablet);
                break;
            }

            case ETabletState::Unmounted: {
                tablet->SetState(ETabletState::Unmounted);

                YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Tablet unmounted (%v)",
                    tablet->GetLoggingTag());

                if (!IsRecovery()) {
                    StopTabletEpoch(tablet);
                }

                for (const auto& [replicaId, replicaInfo] : tablet->Replicas()) {
                    PostTableReplicaStatistics(tablet, replicaInfo);
                }

                TRspUnmountTablet response;
                ToProto(response.mutable_tablet_id(), tabletId);
                *response.mutable_mount_hint() = tablet->GetMountHint();

                TabletMap_.Remove(tabletId);

                PostMasterMutation(tabletId, response);
                break;
            }

            case ETabletState::Frozen: {
                auto state = tablet->GetState();
                if (IsInUnmountWorkflow(state)) {
                    YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Improper tablet state transition requested, ignored (CurrentState %v, RequestedState: %v, %v)",
                        state,
                        requestedState,
                        tablet->GetLoggingTag());
                    return;
                }

                tablet->SetState(ETabletState::Frozen);
                tablet->ClearDynamicStoreIdPool();

                for (const auto& [storeId, store] : tablet->StoreIdMap()) {
                    if (store->IsChunk()) {
                        ReleaseBackingStore(store->AsChunk());
                    }
                }

                YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Tablet frozen (%v)",
                    tablet->GetLoggingTag());

                TRspFreezeTablet response;
                ToProto(response.mutable_tablet_id(), tabletId);
                *response.mutable_mount_hint() = tablet->GetMountHint();
                PostMasterMutation(tabletId, response);
                break;
            }

            default:
                YT_ABORT();
        }
    }


    void HydraLeaderWriteRows(
        TTransactionId transactionId,
        NHydra::TRevision mountRevision,
        TTransactionSignature signature,
        bool lockless,
        const TTransactionWriteRecord& writeRecord,
        const NRpc::TAuthenticationIdentity& identity,
        TMutationContext* /*context*/) noexcept
    {
        NRpc::TCurrentAuthenticationIdentityGuard identityGuard(&identity);

        auto atomicity = AtomicityFromTransactionId(transactionId);

        auto* tablet = PrelockedTablets_.front();
        PrelockedTablets_.pop();
        YT_VERIFY(tablet->GetId() == writeRecord.TabletId);
        auto finallyGuard = Finally([&]() {
            UnlockTablet(tablet);
        });

        if (mountRevision != tablet->GetMountRevision()) {
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Mount revision mismatch; write ignored "
                "(%v, TransactionId: %v, MutationMountRevision: %llx, CurrentMountRevision: %llx)",
                tablet->GetLoggingTag(),
                transactionId,
                mountRevision,
                tablet->GetMountRevision());
            return;
        }

        TTransaction* transaction = nullptr;
        switch (atomicity) {
            case EAtomicity::Full: {
                const auto& transactionManager = Slot_->GetTransactionManager();
                transaction = transactionManager->MakeTransactionPersistent(transactionId);

                if (lockless) {
                    transaction->LockedTablets().push_back(tablet);
                    LockTablet(tablet);

                    YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Prelocked tablet confirmed (TabletId: %v, TransactionId: %v, "
                        "RowCount: %v, LockCount: %v)",
                        writeRecord.TabletId,
                        transactionId,
                        writeRecord.RowCount,
                        tablet->GetTabletLockCount());
                } else {
                    auto& prelockedRows = transaction->PrelockedRows();
                    for (int index = 0; index < writeRecord.RowCount; ++index) {
                        YT_ASSERT(!prelockedRows.empty());
                        auto rowRef = prelockedRows.front();
                        prelockedRows.pop();
                        if (ValidateAndDiscardRowRef(rowRef)) {
                            rowRef.StoreManager->ConfirmRow(transaction, rowRef);
                        }
                    }

                    YT_LOG_DEBUG("Prelocked rows confirmed (TabletId: %v, TransactionId: %v, RowCount: %v)",
                        writeRecord.TabletId,
                        transactionId,
                        writeRecord.RowCount);
                }

                bool immediate = tablet->GetCommitOrdering() == ECommitOrdering::Weak;
                auto* writeLog = immediate
                    ? (lockless ? &transaction->ImmediateLocklessWriteLog() : &transaction->ImmediateLockedWriteLog())
                    : &transaction->DelayedLocklessWriteLog();
                EnqueueTransactionWriteRecord(transaction, writeLog, writeRecord, signature);

                YT_LOG_DEBUG_UNLESS(writeLog == &transaction->ImmediateLockedWriteLog(),
                    "Rows batched (TabletId: %v, TransactionId: %v, WriteRecordSize: %v, Immediate: %v, Lockless: %v)",
                    writeRecord.TabletId,
                    transactionId,
                    writeRecord.GetByteSize(),
                    immediate,
                    lockless);
                break;
            }

            case EAtomicity::None: {
                if (tablet->GetState() == ETabletState::Orphaned) {
                    YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Tablet is orphaned; non-atomic write ignored "
                        "(%v, TransactionId: %v)",
                        tablet->GetLoggingTag(),
                        transactionId);
                    return;
                }

                TWireProtocolReader reader(writeRecord.Data);
                TWriteContext context;
                context.Phase = EWritePhase::Commit;
                context.CommitTimestamp = TimestampFromTransactionId(transactionId);
                const auto& storeManager = tablet->GetStoreManager();
                YT_VERIFY(storeManager->ExecuteWrites(&reader, &context));
                YT_VERIFY(writeRecord.RowCount == context.RowCount);

                auto counters = tablet->GetTableProfiler()->GetCommitCounters(GetCurrentProfilingUser());
                counters->RowCount.Increment(writeRecord.RowCount);
                counters->DataWeight.Increment(writeRecord.DataWeight);

                YT_LOG_DEBUG("Non-atomic rows committed (TransactionId: %v, TabletId: %v, "
                    "RowCount: %v, WriteRecordSize: %v, ActualTimestamp: %llx)",
                    transactionId,
                    writeRecord.TabletId,
                    writeRecord.RowCount,
                    writeRecord.Data.Size(),
                    context.CommitTimestamp);
                break;
            }

            default:
                YT_ABORT();
        }
    }

    void HydraFollowerWriteRows(TReqWriteRows* request) noexcept
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto atomicity = AtomicityFromTransactionId(transactionId);
        auto transactionStartTimestamp = request->transaction_start_timestamp();
        auto transactionTimeout = FromProto<TDuration>(request->transaction_timeout());
        auto signature = request->signature();
        auto lockless = request->lockless();
        auto rowCount = request->row_count();
        auto dataWeight = request->data_weight();
        auto syncReplicaIds = FromProto<TSyncReplicaIdList>(request->sync_replica_ids());

        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            // NB: Tablet could be missing if it was, e.g., forcefully removed.
            return;
        }

        auto mountRevision = request->mount_revision();
        if (static_cast<ui64>(mountRevision) != tablet->GetMountRevision()) {
            // Same as above.
            return;
        }

        auto identity = NRpc::ParseAuthenticationIdentityFromProto(*request);
        NRpc::TCurrentAuthenticationIdentityGuard identityGuard(&identity);

        auto codecId = FromProto<ECodec>(request->codec());
        auto* codec = GetCodec(codecId);
        auto compressedRecordData = TSharedRef::FromString(request->compressed_data());
        auto recordData = codec->Decompress(compressedRecordData);
        TTransactionWriteRecord writeRecord(tabletId, recordData, rowCount, dataWeight, syncReplicaIds);
        TWireProtocolReader reader(recordData);

        const auto& storeManager = tablet->GetStoreManager();

        switch (atomicity) {
            case EAtomicity::Full: {
                const auto& transactionManager = Slot_->GetTransactionManager();
                auto* transaction = transactionManager->GetOrCreateTransaction(
                    transactionId,
                    transactionStartTimestamp,
                    transactionTimeout,
                    false);

                bool immediate = tablet->GetCommitOrdering() == ECommitOrdering::Weak;
                auto* writeLog = immediate
                    ? (lockless ? &transaction->ImmediateLocklessWriteLog() : &transaction->ImmediateLockedWriteLog())
                    : &transaction->DelayedLocklessWriteLog();
                EnqueueTransactionWriteRecord(transaction, writeLog, writeRecord, signature);

                if (immediate && !lockless) {
                    TWriteContext context;
                    context.Phase = EWritePhase::Lock;
                    context.Transaction = transaction;
                    YT_VERIFY(storeManager->ExecuteWrites(&reader, &context));

                    YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Rows locked (TransactionId: %v, TabletId: %v, RowCount: %v, "
                        "WriteRecordSize: %v, Signature: %x)",
                        transactionId,
                        tabletId,
                        context.RowCount,
                        writeRecord.GetByteSize(),
                        signature);
                } else {
                    YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Rows batched (TransactionId: %v, TabletId: %v, "
                        "WriteRecordSize: %v, Signature: %x)",
                        transactionId,
                        tabletId,
                        writeRecord.GetByteSize(),
                        signature);

                    transaction->LockedTablets().push_back(tablet);
                    auto lockCount = LockTablet(tablet);

                    YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Tablet locked (TabletId: %v, TransactionId: %v, LockCount: %v)",
                        writeRecord.TabletId,
                        transactionId,
                        lockCount);
                }
                break;
            }

            case EAtomicity::None: {
                TWriteContext context;
                context.Phase = EWritePhase::Commit;
                context.CommitTimestamp = TimestampFromTransactionId(transactionId);

                YT_VERIFY(storeManager->ExecuteWrites(&reader, &context));

                FinishTabletCommit(tablet, nullptr, context.CommitTimestamp);

                YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Non-atomic rows committed (TransactionId: %v, TabletId: %v, "
                    "RowCount: %v, WriteRecordSize: %v, Signature: %x)",
                    transactionId,
                    tabletId,
                    context.RowCount,
                    writeRecord.GetByteSize(),
                    signature);
                break;
            }

            default:
                YT_ABORT();
        }
    }

    void HydraTrimRows(TReqTrimRows* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto mountRevision = request->mount_revision();
        if (static_cast<ui64>(mountRevision) != tablet->GetMountRevision()) {
            return;
        }

        auto trimmedRowCount = request->trimmed_row_count();

        auto identity = NRpc::ParseAuthenticationIdentityFromProto(*request);
        NRpc::TCurrentAuthenticationIdentityGuard identityGuard(&identity);

        UpdateTrimmedRowCount(tablet, trimmedRowCount);
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
        if (static_cast<ui64>(mountRevision) != tablet->GetMountRevision()) {
            return;
        }

        const auto& storeManager = tablet->GetStoreManager();

        if (tablet->GetSettings().MountConfig->EnableDynamicStoreRead && tablet->DynamicStoreIdPool().empty()) {
            if (!tablet->GetDynamicStoreIdRequested()) {
                AllocateDynamicStore(tablet);
            }
            // TODO(ifsmirnov): Store flusher will try making unsuccessful mutations if response
            // from master comes late. Maybe should optimize.
            storeManager->UnscheduleRotation();
            return;
        }

        storeManager->Rotate(true);
        UpdateTabletSnapshot(tablet);
    }

    void HydraPrepareUpdateTabletStores(TTransaction* transaction, TReqUpdateTabletStores* request, bool persistent)
    {
        YT_VERIFY(persistent);

        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = GetTabletOrThrow(tabletId);
        const auto& structuredLogger = tablet->GetStructuredLogger();

        // Validate.
        auto mountRevision = request->mount_revision();
        tablet->ValidateMountRevision(mountRevision);

        THashSet<TChunkId> hunkChunkIdsToAdd;
        for (const auto& descriptor : request->hunk_chunks_to_add()) {
            auto chunkId = FromProto<TStoreId>(descriptor.chunk_id());
            YT_VERIFY(hunkChunkIdsToAdd.insert(chunkId).second);
        }

        std::vector<TStoreId> storeIdsToAdd;
        for (const auto& descriptor : request->stores_to_add()) {
            auto storeId = FromProto<TStoreId>(descriptor.store_id());
            if (auto optionalHunkChunkRefsExt = FindProtoExtension<NTableClient::NProto::THunkChunkRefsExt>(descriptor.chunk_meta().extensions())) {
                for (const auto& ref : optionalHunkChunkRefsExt->refs()) {
                    auto chunkId = FromProto<TChunkId>(ref.chunk_id());
                    if (!hunkChunkIdsToAdd.contains(chunkId)) {
                        auto hunkChunk = tablet->GetHunkChunkOrThrow(chunkId);
                        if (hunkChunk->GetState() != EHunkChunkState::Active) {
                            THROW_ERROR_EXCEPTION("Referenced hunk chunk %v is in %Qlv state",
                                chunkId,
                                hunkChunk->GetState());
                        }
                    }
                }
            }
            storeIdsToAdd.push_back(storeId);
        }

        std::vector<TStoreId> storeIdsToRemove;
        for (const auto& descriptor : request->stores_to_remove()) {
            auto storeId = FromProto<TStoreId>(descriptor.store_id());
            storeIdsToRemove.push_back(storeId);
            auto store = tablet->GetStoreOrThrow(storeId);
            auto state = store->GetStoreState();
            if (state != EStoreState::PassiveDynamic && state != EStoreState::Persistent) {
                THROW_ERROR_EXCEPTION("Store %v has invalid state %Qlv",
                    storeId,
                    state);
            }
        }

        std::vector<TChunkId> hunkChunkIdsToRemove;
        for (const auto& descriptor : request->hunk_chunks_to_remove()) {
            auto chunkId = FromProto<TStoreId>(descriptor.chunk_id());
            hunkChunkIdsToRemove.push_back(chunkId);
            auto hunkChunk = tablet->GetHunkChunkOrThrow(chunkId);
            auto state = hunkChunk->GetState();
            if (state != EHunkChunkState::Active) {
                THROW_ERROR_EXCEPTION("Hunk chunk %v is in %Qlv state",
                    chunkId,
                    state);
            }
            if (!hunkChunk->IsDangling()) {
                THROW_ERROR_EXCEPTION("Hunk chunk %v is not dangling",
                    chunkId)
                    << TErrorAttribute("store_ref_count", hunkChunk->GetStoreRefCount())
                    << TErrorAttribute("prepared_store_ref_count", hunkChunk->GetPreparedStoreRefCount());
            }
        }

        // Prepare.
        for (const auto& descriptor : request->stores_to_remove()) {
            auto storeId = FromProto<TStoreId>(descriptor.store_id());
            auto store = tablet->GetStore(storeId);
            store->SetStoreState(EStoreState::RemovePrepared);
            structuredLogger->OnStoreStateChanged(store);
        }

        for (const auto& descriptor : request->hunk_chunks_to_remove()) {
            auto chunkId = FromProto<TStoreId>(descriptor.chunk_id());
            auto hunkChunk = tablet->GetHunkChunk(chunkId);
            hunkChunk->SetState(EHunkChunkState::RemovePrepared);
            structuredLogger->OnHunkChunkStateChanged(hunkChunk);
        }

        for (const auto& descriptor : request->stores_to_add()) {
            if (auto optionalHunkChunkRefsExt = FindProtoExtension<NTableClient::NProto::THunkChunkRefsExt>(descriptor.chunk_meta().extensions())) {
                for (const auto& ref : optionalHunkChunkRefsExt->refs()) {
                    auto chunkId = FromProto<TChunkId>(ref.chunk_id());
                    if (!hunkChunkIdsToAdd.contains(chunkId)) {
                        auto hunkChunk = tablet->GetHunkChunk(chunkId);
                        tablet->UpdatePreparedStoreRefCount(hunkChunk, +1);
                    }
                }
            }
        }

        auto updateReason = FromProto<ETabletStoresUpdateReason>(request->update_reason());

        // TODO(ifsmirnov): log preparation errors as well.
        structuredLogger->OnTabletStoresUpdatePrepared(
            storeIdsToAdd,
            storeIdsToRemove,
            updateReason,
            transaction->GetId());

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Tablet stores update prepared "
            "(%v, TransactionId: %v, StoreIdsToAdd: %v, HunkChunkIdsToAdd: %v, StoreIdsToRemove: %v, HunkChunkIdsToRemove: %v, "
            "UpdateReason: %v)",
            tablet->GetLoggingTag(),
            transaction->GetId(),
            storeIdsToAdd,
            hunkChunkIdsToAdd,
            storeIdsToRemove,
            hunkChunkIdsToRemove,
            updateReason);
    }

    void HydraAbortUpdateTabletStores(TTransaction* transaction, TReqUpdateTabletStores* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto mountRevision = request->mount_revision();
        if (tablet->GetMountRevision() != static_cast<ui64>(mountRevision)) {
            return;
        }

        const auto& structuredLogger = tablet->GetStructuredLogger();

        THashSet<TChunkId> hunkChunkIdsToAdd;
        for (const auto& descriptor : request->hunk_chunks_to_add()) {
            auto chunkId = FromProto<TChunkId>(descriptor.chunk_id());
            YT_VERIFY(hunkChunkIdsToAdd.insert(chunkId).second);
        }

        for (const auto& descriptor : request->stores_to_add()) {
            if (auto optionalHunkChunkRefsExt = FindProtoExtension<NTableClient::NProto::THunkChunkRefsExt>(descriptor.chunk_meta().extensions())) {
                for (const auto& ref : optionalHunkChunkRefsExt->refs()) {
                    auto chunkId = FromProto<TChunkId>(ref.chunk_id());
                    if (!hunkChunkIdsToAdd.contains(chunkId)) {
                        auto hunkChunk = tablet->FindHunkChunk(chunkId);
                        if (!hunkChunk) {
                            continue;
                        }

                        tablet->UpdatePreparedStoreRefCount(hunkChunk, -1);
                    }
                }
            }
        }

        const auto& storeManager = tablet->GetStoreManager();
        for (const auto& descriptor : request->stores_to_remove()) {
            auto storeId = FromProto<TStoreId>(descriptor.store_id());
            auto store = tablet->FindStore(storeId);
            if (!store) {
                continue;
            }

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
                    YT_ABORT();
            }

            structuredLogger->OnStoreStateChanged(store);

            if (IsLeader()) {
                storeManager->BackoffStoreRemoval(store);
            }
        }

        for (const auto& descriptor : request->hunk_chunks_to_remove()) {
            auto chunkId = FromProto<TStoreId>(descriptor.chunk_id());
            auto hunkChunk = tablet->FindHunkChunk(chunkId);
            if (!hunkChunk) {
                continue;
            }

            hunkChunk->SetState(EHunkChunkState::Active);
        }

        CheckIfTabletFullyFlushed(tablet);

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Tablet stores update aborted "
            "(%v, TransactionId: %v)",
            tablet->GetLoggingTag(),
            transaction->GetId());
    }

    bool IsBackingStoreRequired(TTablet* tablet)
    {
        return
            tablet->GetAtomicity() == EAtomicity::Full &&
            tablet->GetSettings().MountConfig->BackingStoreRetentionTime != TDuration::Zero();
    }

    void HydraCommitUpdateTabletStores(TTransaction* transaction, TReqUpdateTabletStores* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto mountRevision = request->mount_revision();
        if (static_cast<ui64>(mountRevision) != tablet->GetMountRevision()) {
            return;
        }

        auto updateReason = FromProto<ETabletStoresUpdateReason>(request->update_reason());

        const auto& storeManager = tablet->GetStoreManager();

        // NB: Must handle store removals before store additions since
        // row index map forbids having multiple stores with the same starting row index.
        // But before proceeding to removals, we must take care of backing stores.
        THashMap<TStoreId, IDynamicStorePtr> idToBackingStore;
        auto registerBackingStore = [&] (const IStorePtr& store) {
            YT_VERIFY(idToBackingStore.emplace(store->GetId(), store->AsDynamic()).second);
        };

        if (!IsRecovery()) {
            for (const auto& descriptor : request->stores_to_add()) {
                if (descriptor.has_backing_store_id()) {
                    auto backingStoreId = FromProto<TStoreId>(descriptor.backing_store_id());
                    auto backingStore = tablet->GetStore(backingStoreId);
                    registerBackingStore(backingStore);
                }
            }
        }

        THashSet<THunkChunkPtr> addedHunkChunks;
        for (const auto& descriptor : request->hunk_chunks_to_add()) {
            auto chunkId = FromProto<TChunkId>(descriptor.chunk_id());

            auto hunkChunk = CreateHunkChunk(tablet, chunkId, &descriptor);
            hunkChunk->Initialize();
            tablet->AddHunkChunk(hunkChunk);
            YT_VERIFY(addedHunkChunks.insert(hunkChunk).second);

            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Hunk chunk added (%v, ChunkId: %v)",
                tablet->GetLoggingTag(),
                chunkId);
        }

        std::vector<TStoreId> removedStoreIds;
        for (const auto& descriptor : request->stores_to_remove()) {
            auto storeId = FromProto<TStoreId>(descriptor.store_id());
            removedStoreIds.push_back(storeId);

            auto store = tablet->GetStore(storeId);
            storeManager->RemoveStore(store);

            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Store removed (%v, StoreId: %v, DynamicMemoryUsage: %v)",
                tablet->GetLoggingTag(),
                storeId,
                store->GetDynamicMemoryUsage());

            if (store->IsChunk()) {
                auto chunkStore = store->AsChunk();
                for (const auto& ref : chunkStore->HunkChunkRefs()) {
                    tablet->UpdateHunkChunkRef(ref, -1);

                    const auto& hunkChunk = ref.HunkChunk;

                    YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Hunk chunk unreferenced (%v, StoreId: %v, HunkChunkRef: %v, StoreRefCount: %v)",
                        tablet->GetLoggingTag(),
                        storeId,
                        ref,
                        hunkChunk->GetStoreRefCount());
                }
            }
        }

        std::vector<TChunkId> removedHunkChunkIds;
        for (const auto& descriptor : request->hunk_chunks_to_remove()) {
            auto chunkId = FromProto<TStoreId>(descriptor.chunk_id());
            removedHunkChunkIds.push_back(chunkId);

            auto hunkChunk = tablet->GetHunkChunk(chunkId);
            tablet->RemoveHunkChunk(hunkChunk);
            hunkChunk->SetState(EHunkChunkState::Removed);

            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Hunk chunk removed (%v, ChunkId: %v)",
                tablet->GetLoggingTag(),
                chunkId);
        }

        std::vector<IStorePtr> addedStores;
        for (const auto& descriptor : request->stores_to_add()) {
            auto storeType = FromProto<EStoreType>(descriptor.store_type());
            auto storeId = FromProto<TChunkId>(descriptor.store_id());

            auto store = CreateStore(tablet, storeType, storeId, &descriptor)->AsChunk();
            store->Initialize();
            storeManager->AddStore(store, false);
            addedStores.push_back(store);

            TStoreId backingStoreId;
            if (!IsRecovery() && descriptor.has_backing_store_id() && IsBackingStoreRequired(tablet)) {
                backingStoreId = FromProto<TStoreId>(descriptor.backing_store_id());
                const auto& backingStore = GetOrCrash(idToBackingStore, backingStoreId);
                SetBackingStore(tablet, store, backingStore);
            }

            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Chunk store added (%v, StoreId: %v, MaxTimestamp: %llx, BackingStoreId: %v)",
                tablet->GetLoggingTag(),
                storeId,
                store->GetMaxTimestamp(),
                backingStoreId);

            if (store->IsChunk()) {
                auto chunkStore = store->AsChunk();
                for (const auto& ref : chunkStore->HunkChunkRefs()) {
                    tablet->UpdateHunkChunkRef(ref, +1);

                    const auto& hunkChunk = ref.HunkChunk;
                    if (!addedHunkChunks.contains(hunkChunk)) {
                        tablet->UpdatePreparedStoreRefCount(hunkChunk, -1);
                    }

                    YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Hunk chunk referenced (%v, StoreId: %v, HunkChunkRef: %v, StoreRefCount: %v)",
                        tablet->GetLoggingTag(),
                        storeId,
                        ref,
                        hunkChunk->GetStoreRefCount());
                }
            }
        }

        auto retainedTimestamp = std::max(
            tablet->GetRetainedTimestamp(),
            static_cast<TTimestamp>(request->retained_timestamp()));
        tablet->SetRetainedTimestamp(retainedTimestamp);
        TDynamicStoreId allocatedDynamicStoreId;

        if (updateReason == ETabletStoresUpdateReason::Flush && request->request_dynamic_store_id()) {
            auto storeId = ReplaceTypeInId(
                transaction->GetId(),
                tablet->IsPhysicallySorted()
                    ? EObjectType::SortedDynamicTabletStore
                    : EObjectType::OrderedDynamicTabletStore);
            tablet->PushDynamicStoreIdToPool(storeId);
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Dynamic store id added to the pool (%v, StoreId: %v)",
                tablet->GetLoggingTag(),
                storeId);

            allocatedDynamicStoreId = storeId;
        }

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Tablet stores update committed "
            "(%v, TransactionId: %v, AddedStoreIds: %v, RemovedStoreIds: %v, AddedHunkChunkIds: %v, RemovedHunkChunkIds: %v, "
            "RetainedTimestamp: %llx, UpdateReason: %v)",
            tablet->GetLoggingTag(),
            transaction->GetId(),
            MakeFormattableView(addedStores, TStoreIdFormatter()),
            removedStoreIds,
            MakeFormattableView(addedHunkChunks, THunkChunkIdFormatter()),
            removedHunkChunkIds,
            retainedTimestamp,
            updateReason);

        tablet->GetStructuredLogger()->OnTabletStoresUpdateCommitted(
            addedStores,
            removedStoreIds,
            std::vector<THunkChunkPtr>(addedHunkChunks.begin(), addedHunkChunks.end()),
            removedHunkChunkIds,
            updateReason,
            allocatedDynamicStoreId,
            transaction->GetId());

        UpdateTabletSnapshot(tablet);

        CheckIfTabletFullyFlushed(tablet);
    }

    void HydraSplitPartition(TReqSplitPartition* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        YT_VERIFY(tablet->IsPhysicallySorted());

        auto mountRevision = request->mount_revision();
        if (static_cast<ui64>(mountRevision) != tablet->GetMountRevision()) {
            return;
        }

        auto partitionId = FromProto<TPartitionId>(request->partition_id());
        auto* partition = tablet->GetPartition(partitionId);

        auto pivotKeys = FromProto<std::vector<TLegacyOwningKey>>(request->pivot_keys());

        int partitionIndex = partition->GetIndex();
        i64 partitionDataSize = partition->GetCompressedDataSize();

        auto storeManager = tablet->GetStoreManager()->AsSorted();
        bool result = storeManager->SplitPartition(partition->GetIndex(), pivotKeys);
        if (!result) {
            YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Partition split failed (%v, PartitionId: %v, Keys: %v)",
                tablet->GetLoggingTag(),
                partitionId,
                JoinToString(pivotKeys, TStringBuf(" .. ")));
            return;
        }

        UpdateTabletSnapshot(tablet);

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Partition split (%v, OriginalPartitionId: %v, "
            "ResultingPartitionIds: %v, DataSize: %v, Keys: %v)",
            tablet->GetLoggingTag(),
            partitionId,
            MakeFormattableView(
                MakeRange(
                    tablet->PartitionList().data() + partitionIndex,
                    tablet->PartitionList().data() + partitionIndex + pivotKeys.size()),
                TPartitionIdFormatter()),
            partitionDataSize,
            JoinToString(pivotKeys, TStringBuf(" .. ")));
    }

    void HydraMergePartitions(TReqMergePartitions* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        YT_VERIFY(tablet->IsPhysicallySorted());

        auto mountRevision = request->mount_revision();
        if (static_cast<ui64>(mountRevision) != tablet->GetMountRevision()) {
            return;
        }

        auto firstPartitionId = FromProto<TPartitionId>(request->partition_id());
        auto* firstPartition = tablet->GetPartition(firstPartitionId);

        int firstPartitionIndex = firstPartition->GetIndex();
        int lastPartitionIndex = firstPartitionIndex + request->partition_count() - 1;

        auto originalPartitionIds = Format("%v",
            MakeFormattableView(
                MakeRange(
                    tablet->PartitionList().data() + firstPartitionIndex,
                    tablet->PartitionList().data() + lastPartitionIndex + 1),
                TPartitionIdFormatter()));

        i64 partitionsDataSize = 0;
        for (int index = firstPartitionIndex; index <= lastPartitionIndex; ++index) {
            const auto& partition = tablet->PartitionList()[index];
            partitionsDataSize += partition->GetCompressedDataSize();
        }

        auto storeManager = tablet->GetStoreManager()->AsSorted();
        storeManager->MergePartitions(
            firstPartition->GetIndex(),
            firstPartition->GetIndex() + request->partition_count() - 1);

        UpdateTabletSnapshot(tablet);

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Partitions merged (%v, OriginalPartitionIds: %v, "
            "ResultingPartitionId: %v, DataSize: %v)",
            tablet->GetLoggingTag(),
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

        YT_VERIFY(tablet->IsPhysicallySorted());

        auto mountRevision = request->mount_revision();
        if (static_cast<ui64>(mountRevision) != tablet->GetMountRevision()) {
            return;
        }

        auto partitionId = FromProto<TPartitionId>(request->partition_id());
        auto* partition = tablet->FindPartition(partitionId);
        if (!partition) {
            return;
        }

        TWireProtocolReader reader(TSharedRef::FromString(request->sample_keys()));
        auto sampleKeys = reader.ReadUnversionedRowset(true);

        auto storeManager = tablet->GetStoreManager()->AsSorted();
        storeManager->UpdatePartitionSampleKeys(partition, sampleKeys);

        UpdateTabletSnapshot(tablet);

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Partition sample keys updated (%v, PartitionId: %v, SampleKeyCount: %v)",
            tablet->GetLoggingTag(),
            partition->GetId(),
            sampleKeys.Size());
    }

    void HydraAddTableReplica(TReqAddTableReplica* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto* replicaInfo = AddTableReplica(tablet, request->replica());
        if (!replicaInfo) {
            return;
        }

        if (!IsRecovery()) {
            StartTableReplicaEpoch(tablet, replicaInfo);
        }
    }

    void HydraRemoveTableReplica(TReqRemoveTableReplica* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto replicaId = FromProto<TTableReplicaId>(request->replica_id());
        RemoveTableReplica(tablet, replicaId);
    }

    // COMPAT(aozeritsky)
    void HydraSetTableReplicaEnabled(TReqSetTableReplicaEnabled* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto replicaId = FromProto<TTableReplicaId>(request->replica_id());
        auto* replicaInfo = tablet->FindReplicaInfo(replicaId);
        if (!replicaInfo) {
            return;
        }

        bool enabled = request->enabled();
        if (enabled) {
            EnableTableReplica(tablet, replicaInfo);
        } else {
            DisableTableReplica(tablet, replicaInfo);
        }
    }

    void HydraAlterTableReplica(TReqAlterTableReplica* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto replicaId = FromProto<TTableReplicaId>(request->replica_id());
        auto* replicaInfo = tablet->FindReplicaInfo(replicaId);
        if (!replicaInfo) {
            return;
        }

        auto enabled = request->has_enabled() ? std::make_optional(request->enabled()) : std::nullopt;
        auto mode = request->has_mode() ? std::make_optional(ETableReplicaMode(request->mode())) : std::nullopt;
        auto atomicity = request->has_atomicity()
            ? std::make_optional(NTransactionClient::EAtomicity(request->atomicity()))
            : std::nullopt;
        auto preserveTimestamps = request->has_preserve_timestamps()
            ? std::make_optional(request->preserve_timestamps())
            : std::nullopt;


        if (enabled) {
            if (*enabled) {
                EnableTableReplica(tablet, replicaInfo);
            } else {
                DisableTableReplica(tablet, replicaInfo);
            }
        }

        if (mode) {
            replicaInfo->SetMode(*mode);
        }

        if (atomicity) {
            replicaInfo->SetAtomicity(*atomicity);
        }

        if (preserveTimestamps) {
            replicaInfo->SetPreserveTimestamps(*preserveTimestamps);
        }

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Table replica updated (%v, ReplicaId: %v, Enabled: %v, Mode: %v, Atomicity: %v, PreserveTimestamps: %v)",
            tablet->GetLoggingTag(),
            replicaInfo->GetId(),
            enabled,
            mode,
            atomicity,
            preserveTimestamps);
    }

    void HydraPrepareReplicateRows(TTransaction* transaction, TReqReplicateRows* request, bool persistent)
    {
        YT_VERIFY(persistent);

        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = GetTabletOrThrow(tabletId);

        auto replicaId = FromProto<TTableReplicaId>(request->replica_id());
        auto* replicaInfo = tablet->GetReplicaInfoOrThrow(replicaId);

        if (replicaInfo->GetState() != ETableReplicaState::Enabled) {
            THROW_ERROR_EXCEPTION("Replica %v is in %Qlv state",
                replicaId,
                replicaInfo->GetState());
        }

        if (replicaInfo->GetPreparedReplicationTransactionId()) {
            THROW_ERROR_EXCEPTION("Cannot prepare rows for replica %v of tablet %v by transaction %v since these are already "
                "prepared by transaction %v",
                transaction->GetId(),
                replicaId,
                tabletId,
                replicaInfo->GetPreparedReplicationTransactionId());
        }

        auto newReplicationRowIndex = request->new_replication_row_index();
        auto newReplicationTimestamp = request->new_replication_timestamp();

        if (request->has_prev_replication_row_index()) {
            auto prevReplicationRowIndex = request->prev_replication_row_index();
            if (replicaInfo->GetCurrentReplicationRowIndex() != prevReplicationRowIndex) {
                THROW_ERROR_EXCEPTION("Cannot prepare rows for replica %v of tablet %v by transaction %v due to current replication row index "
                    "mismatch: %v != %v",
                    transaction->GetId(),
                    replicaId,
                    tabletId,
                    replicaInfo->GetCurrentReplicationRowIndex(),
                    prevReplicationRowIndex);
            }
            YT_VERIFY(newReplicationRowIndex >= prevReplicationRowIndex);
        }

        if (newReplicationRowIndex < replicaInfo->GetCurrentReplicationRowIndex()) {
            THROW_ERROR_EXCEPTION("Cannot prepare rows for replica %v of tablet %v by transaction %v since current replication row index "
                "is already too high: %v > %v",
                transaction->GetId(),
                replicaId,
                tabletId,
                replicaInfo->GetCurrentReplicationRowIndex(),
                newReplicationRowIndex);
        }

        YT_VERIFY(newReplicationRowIndex <= tablet->GetTotalRowCount());
        YT_VERIFY(replicaInfo->GetPreparedReplicationRowIndex() == -1);

        replicaInfo->SetPreparedReplicationRowIndex(newReplicationRowIndex);
        replicaInfo->SetPreparedReplicationTransactionId(transaction->GetId());

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Async replicated rows prepared (TabletId: %v, ReplicaId: %v, TransactionId: %v, "
            "CurrentReplicationRowIndex: %v -> %v, TotalRowCount: %v, CurrentReplicationTimestamp: %llx -> %llx)",
            tabletId,
            replicaId,
            transaction->GetId(),
            replicaInfo->GetCurrentReplicationRowIndex(),
            newReplicationRowIndex,
            tablet->GetTotalRowCount(),
            replicaInfo->GetCurrentReplicationTimestamp(),
            newReplicationTimestamp);

    }

    void HydraCommitReplicateRows(TTransaction* transaction, TReqReplicateRows* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto replicaId = FromProto<TTableReplicaId>(request->replica_id());
        auto* replicaInfo = tablet->FindReplicaInfo(replicaId);
        if (!replicaInfo) {
            return;
        }

        // COMPAT(babenko)
        if (request->has_prev_replication_row_index()) {
            YT_VERIFY(replicaInfo->GetCurrentReplicationRowIndex() == request->prev_replication_row_index());
        }
        YT_VERIFY(replicaInfo->GetPreparedReplicationRowIndex() == request->new_replication_row_index());
        YT_VERIFY(replicaInfo->GetPreparedReplicationTransactionId() == transaction->GetId());
        replicaInfo->SetPreparedReplicationRowIndex(-1);
        replicaInfo->SetPreparedReplicationTransactionId(NullTransactionId);

        auto prevCurrentReplicationRowIndex = replicaInfo->GetCurrentReplicationRowIndex();
        auto prevCurrentReplicationTimestamp = replicaInfo->GetCurrentReplicationTimestamp();
        auto prevTrimmedRowCount = tablet->GetTrimmedRowCount();

        auto newCurrentReplicationRowIndex = request->new_replication_row_index();
        auto newCurrentReplicationTimestamp = request->new_replication_timestamp();

        if (newCurrentReplicationRowIndex < prevCurrentReplicationRowIndex) {
            YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "CurrentReplicationIndex went back (TabletId: %v, ReplicaId: %v, TransactionId: %v, "
                "CurrentReplicationRowIndex: %v -> %v)",
                tabletId,
                replicaId,
                transaction->GetId(),
                prevCurrentReplicationRowIndex,
                newCurrentReplicationRowIndex);
            newCurrentReplicationRowIndex = prevCurrentReplicationRowIndex;
        }
        if (newCurrentReplicationTimestamp < prevCurrentReplicationTimestamp) {
            YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "CurrentReplicationTimestamp went back (TabletId: %v, ReplicaId: %v, TransactionId: %v, "
                "CurrentReplicationTimestamp: %llx -> %llx)",
                tabletId,
                replicaId,
                transaction->GetId(),
                prevCurrentReplicationTimestamp,
                newCurrentReplicationTimestamp);
            newCurrentReplicationTimestamp = prevCurrentReplicationTimestamp;
        }

        replicaInfo->SetCurrentReplicationRowIndex(newCurrentReplicationRowIndex);
        replicaInfo->SetCurrentReplicationTimestamp(newCurrentReplicationTimestamp);

        AdvanceReplicatedTrimmedRowCount(tablet, transaction);

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Async replicated rows committed (TabletId: %v, ReplicaId: %v, TransactionId: %v, "
            "CurrentReplicationRowIndex: %v -> %v, CurrentReplicationTimestamp: %llx -> %llx, TrimmedRowCount: %v -> %v, TotalRowCount: %v)",
            tabletId,
            replicaId,
            transaction->GetId(),
            prevCurrentReplicationRowIndex,
            replicaInfo->GetCurrentReplicationRowIndex(),
            prevCurrentReplicationTimestamp,
            replicaInfo->GetCurrentReplicationTimestamp(),
            prevTrimmedRowCount,
            tablet->GetTrimmedRowCount(),
            tablet->GetTotalRowCount());
    }

    void HydraAbortReplicateRows(TTransaction* transaction, TReqReplicateRows* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto replicaId = FromProto<TTableReplicaId>(request->replica_id());
        auto* replicaInfo = tablet->FindReplicaInfo(replicaId);
        if (!replicaInfo) {
            return;
        }

        if (transaction->GetId() != replicaInfo->GetPreparedReplicationTransactionId()) {
            return;
        }

        replicaInfo->SetPreparedReplicationRowIndex(-1);
        replicaInfo->SetPreparedReplicationTransactionId(NullTransactionId);

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Async replicated rows aborted (TabletId: %v, ReplicaId: %v, TransactionId: %v, "
            "CurrentReplicationRowIndex: %v -> %v, TotalRowCount: %v, CurrentReplicationTimestamp: %llx -> %llx)",
            tabletId,
            replicaId,
            transaction->GetId(),
            replicaInfo->GetCurrentReplicationRowIndex(),
            request->new_replication_row_index(),
            tablet->GetTotalRowCount(),
            replicaInfo->GetCurrentReplicationTimestamp(),
            request->new_replication_timestamp());
    }

    void HydraDecommissionTabletCell(TReqDecommissionTabletCellOnNode* /*request*/)
    {
        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Tablet cell is decommissioning");

        CellLifeStage_ = ETabletCellLifeStage::DecommissioningOnNode;
        Slot_->GetTransactionManager()->Decommission();
        Slot_->GetTransactionSupervisor()->Decommission();
    }

    void OnCheckTabletCellDecommission()
    {
        if (CellLifeStage_ != ETabletCellLifeStage::DecommissioningOnNode) {
            return;
        }

        if (Slot_->GetDynamicOptions()->SuppressTabletCellDecommission.value_or(false)) {
            return;
        }

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Checking if tablet cell is decommissioned "
            "(LifeStage: %v, TabletMapEmpty: %v, TransactionManagerDecommissined: %v, TransactionSupervisorDecommissioned: %v)",
            CellLifeStage_,
            TabletMap_.empty(),
            Slot_->GetTransactionManager()->IsDecommissioned(),
            Slot_->GetTransactionSupervisor()->IsDecommissioned());

        if (!TabletMap_.empty()) {
            return;
        }

        if (!Slot_->GetTransactionManager()->IsDecommissioned()) {
            return;
        }

        if (!Slot_->GetTransactionSupervisor()->IsDecommissioned()) {
            return;
        }

        CreateMutation(Slot_->GetHydraManager(), TReqOnTabletCellDecommissioned())
            ->CommitAndLog(Logger);
    }

    void HydraOnTabletCellDecommissioned(TReqOnTabletCellDecommissioned* /*request*/)
    {
        if (CellLifeStage_ != ETabletCellLifeStage::DecommissioningOnNode) {
            return;
        }

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Tablet cell decommissioned");

        CellLifeStage_ = ETabletCellLifeStage::Decommissioned;

        const auto& hiveManager = Slot_->GetHiveManager();
        auto* mailbox = Slot_->GetMasterMailbox();
        TRspDecommissionTabletCellOnNode response;
        ToProto(response.mutable_cell_id(), Slot_->GetCellId());
        hiveManager->PostMessage(mailbox, response);
    }


    template <class TRequest>
    void PopulateDynamicStoreIdPool(TTablet* tablet, const TRequest* request)
    {
        for (const auto& protoStoreId : request->dynamic_store_ids()) {
            auto storeId = FromProto<TDynamicStoreId>(protoStoreId);
            tablet->PushDynamicStoreIdToPool(storeId);
        }
    }

    void AllocateDynamicStore(TTablet* tablet)
    {
        TReqAllocateDynamicStore req;
        ToProto(req.mutable_tablet_id(), tablet->GetId());
        req.set_mount_revision(tablet->GetMountRevision());
        tablet->SetDynamicStoreIdRequested(true);
        PostMasterMutation(tablet->GetId(), req);
    }

    void HydraOnDynamicStoreAllocated(TRspAllocateDynamicStore* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        auto state = tablet->GetState();
        if (state != ETabletState::Mounted &&
            state != ETabletState::UnmountFlushPending &&
            state != ETabletState::UnmountFlushing &&
            state != ETabletState::FreezeFlushPending &&
            state != ETabletState::FreezeFlushing)
        {
            YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "Dynamic store id sent to a tablet in a wrong state, ignored (%v, State: %v)",
                tablet->GetLoggingTag(),
                state);
            return;
        }

        auto dynamicStoreId = FromProto<TDynamicStoreId>(request->dynamic_store_id());
        tablet->PushDynamicStoreIdToPool(dynamicStoreId);
        tablet->SetDynamicStoreIdRequested(false);

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Dynamic store allocated for a tablet (%v, DynamicStoreId: %v)",
            tablet->GetLoggingTag(),
            dynamicStoreId);
    }

    void ValidateReplicaWritable(TTablet* tablet, const TTableReplicaInfo& replicaInfo)
    {
        auto currentReplicationRowIndex = replicaInfo.GetCurrentReplicationRowIndex();
        auto totalRowCount = tablet->GetTotalRowCount();
        auto delayedLocklessRowCount = tablet->GetDelayedLocklessRowCount();
        switch (replicaInfo.GetMode()) {
            case ETableReplicaMode::Sync: {
                // COMPAT(babenko)
                auto reign = GetCurrentMutationContext()->Request().Reign;
                if (reign < ToUnderlying(ETabletReign::ReplicationBarrier_YT_14346)) {
                    if (currentReplicationRowIndex < totalRowCount) {
                        THROW_ERROR_EXCEPTION("Replica %v of tablet %v is not synchronously writeable since some rows are not replicated yet",
                            replicaInfo.GetId(),
                            tablet->GetId())
                            << TErrorAttribute("current_replication_row_index", currentReplicationRowIndex)
                            << TErrorAttribute("total_row_count", totalRowCount);
                    }
                } else {
                    if (currentReplicationRowIndex < totalRowCount + delayedLocklessRowCount) {
                        THROW_ERROR_EXCEPTION("Replica %v of tablet %v is not synchronously writeable since some rows are not replicated yet",
                            replicaInfo.GetId(),
                            tablet->GetId())
                            << TErrorAttribute("current_replication_row_index", currentReplicationRowIndex)
                            << TErrorAttribute("total_row_count", totalRowCount)
                            << TErrorAttribute("delayed_lockless_row_count", delayedLocklessRowCount);
                    }
                    if (currentReplicationRowIndex > totalRowCount + delayedLocklessRowCount) {
                        YT_LOG_ALERT_IF(IsMutationLoggingEnabled(),
                            "Current replication row index is too high (TabletId: %v, ReplicaId: %v, "
                            "CurrentReplicationRowIndex: %v, TotalRowCount: %v, DelayedLocklessRowCount: %v)",
                            tablet->GetId(),
                            replicaInfo.GetId(),
                            currentReplicationRowIndex,
                            totalRowCount,
                            delayedLocklessRowCount);
                    }
                }
                if (replicaInfo.GetState() != ETableReplicaState::Enabled) {
                    THROW_ERROR_EXCEPTION("Replica %v is not synchronously writeable since it is in %Qlv state",
                        replicaInfo.GetId(),
                        replicaInfo.GetState());
                }
                YT_VERIFY(!replicaInfo.GetPreparedReplicationTransactionId());
                break;
            }

            case ETableReplicaMode::Async:
                if (currentReplicationRowIndex > totalRowCount) {
                    THROW_ERROR_EXCEPTION("Replica %v of tablet %v is not asynchronously writeable: some synchronous writes are still in progress",
                        replicaInfo.GetId(),
                        tablet->GetId())
                        << TErrorAttribute("current_replication_row_index", currentReplicationRowIndex)
                        << TErrorAttribute("total_row_count", totalRowCount);
                }
                break;

            default:
                YT_ABORT();
        }
    }

    static void ValidateSyncReplicaSet(TTablet* tablet, const TSyncReplicaIdList& syncReplicaIds)
    {
        for (auto replicaId : syncReplicaIds) {
            const auto* replicaInfo = tablet->FindReplicaInfo(replicaId);
            if (!replicaInfo) {
                THROW_ERROR_EXCEPTION(
                    NTabletClient::EErrorCode::SyncReplicaIsNotKnown,
                    "Synchronous replica %v is not known for tablet %v",
                    replicaId,
                    tablet->GetId());
            }
            if (replicaInfo->GetMode() != ETableReplicaMode::Sync) {
                THROW_ERROR_EXCEPTION(
                    NTabletClient::EErrorCode::SyncReplicaIsNotInSyncMode,
                    "Replica %v of tablet %v is not in sync mode",
                    replicaId,
                    tablet->GetId());
            }
        }

        for (const auto& [replicaId, replicaInfo] : tablet->Replicas()) {
            if (replicaInfo.GetMode() == ETableReplicaMode::Sync) {
                if (std::find(syncReplicaIds.begin(), syncReplicaIds.end(), replicaId) == syncReplicaIds.end()) {
                    THROW_ERROR_EXCEPTION(
                        NTabletClient::EErrorCode::SyncReplicaIsNotWritten,
                        "Synchronous replica %v of tablet %v is not being written by client",
                        replicaId,
                        tablet->GetId());
                }
            }
        }
    }


    void OnTransactionPrepared(TTransaction* transaction, bool persistent)
    {
        PrepareLockedRows(transaction);

        // The rest only makes sense for persistent prepare.
        // In particular, all writes to replicated tables currently involve 2PC.
        if (!persistent) {
            return;
        }

        TSmallFlatMap<TTableReplicaInfo*, int, 8> replicaToRowCount;
        TSmallFlatMap<TTablet*, int, 8> tabletToRowCount;
        for (const auto& writeRecord : transaction->DelayedLocklessWriteLog()) {
            auto* tablet = GetTabletOrThrow(writeRecord.TabletId);

            if (!tablet->IsReplicated()) {
                continue;
            }

            // TODO(ifsmirnov): No bulk insert into replicated tables. Remove this check?
            const auto& lockManager = tablet->GetLockManager();
            if (auto error = lockManager->ValidateTransactionConflict(transaction->GetStartTimestamp());
                !error.IsOK())
            {
                THROW_ERROR error << TErrorAttribute("tablet_id", tablet->GetId());
            }

            ValidateSyncReplicaSet(tablet, writeRecord.SyncReplicaIds);
            for (auto& [replicaId, replicaInfo] : tablet->Replicas()) {
                ValidateReplicaWritable(tablet, replicaInfo);
                if (replicaInfo.GetMode() == ETableReplicaMode::Sync) {
                    replicaToRowCount[&replicaInfo] += writeRecord.RowCount;
                }
            }

            tabletToRowCount[tablet] += writeRecord.RowCount;
        }

        for (auto [replicaInfo, rowCount] : replicaToRowCount) {
            const auto* tablet = replicaInfo->GetTablet();
            auto oldCurrentReplicationRowIndex = replicaInfo->GetCurrentReplicationRowIndex();
            auto newCurrentReplicationRowIndex = oldCurrentReplicationRowIndex + rowCount;
            replicaInfo->SetCurrentReplicationRowIndex(newCurrentReplicationRowIndex);
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
                "Sync replicated rows prepared (TransactionId: %v, TabletId: %v, ReplicaId: %v, CurrentReplicationRowIndex: %v -> %v, "
                "TotalRowCount: %v)",
                transaction->GetId(),
                tablet->GetId(),
                replicaInfo->GetId(),
                oldCurrentReplicationRowIndex,
                newCurrentReplicationRowIndex,
                tablet->GetTotalRowCount());
        }

        for (auto [tablet, rowCount] : tabletToRowCount) {
            auto oldDelayedLocklessRowCount = tablet->GetDelayedLocklessRowCount();
            auto newDelayedLocklessRowCount = oldDelayedLocklessRowCount + rowCount;
            tablet->SetDelayedLocklessRowCount(newDelayedLocklessRowCount);
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
                "Delayed lockless rows prepared (TransactionId: %v, TabletId: %v, DelayedLocklessRowCount: %v -> %v)",
                transaction->GetId(),
                tablet->GetId(),
                oldDelayedLocklessRowCount,
                newDelayedLocklessRowCount);
        }

        YT_VERIFY(!transaction->GetRowsPrepared());
        transaction->SetRowsPrepared(true);
    }

    void OnTransactionCommitted(TTransaction* transaction) noexcept
    {
        auto commitTimestamp =  transaction->GetCommitTimestamp();

        YT_VERIFY(transaction->PrelockedRows().empty());
        auto& lockedRows = transaction->LockedRows();
        int lockedRowCount = 0;
        for (const auto& rowRef : lockedRows) {
            if (!ValidateAndDiscardRowRef(rowRef)) {
                continue;
            }

            ++lockedRowCount;
            FinishTabletCommit(rowRef.Store->GetTablet(), transaction, commitTimestamp);
            rowRef.StoreManager->CommitRow(transaction, rowRef);
        }
        lockedRows.clear();

        // Check if above CommitRow calls caused store locks to be released.
        CheckIfImmediateLockedTabletsFullyUnlocked(transaction);

        int locklessRowCount = 0;
        SmallVector<TTablet*, 16> locklessTablets;
        for (const auto& record : transaction->ImmediateLocklessWriteLog()) {
            auto* tablet = FindTablet(record.TabletId);
            if (!tablet) {
                continue;
            }

            locklessTablets.push_back(tablet);

            TWriteContext context;
            context.Phase = EWritePhase::Commit;
            context.Transaction = transaction;
            context.CommitTimestamp = commitTimestamp;

            TWireProtocolReader reader(record.Data);

            const auto& storeManager = tablet->GetStoreManager();
            YT_VERIFY(storeManager->ExecuteWrites(&reader, &context));
            YT_VERIFY(context.RowCount == record.RowCount);

            locklessRowCount += context.RowCount;
        }

        for (auto* tablet : locklessTablets) {
            FinishTabletCommit(tablet, transaction, commitTimestamp);
        }

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled() && (lockedRowCount + locklessRowCount > 0),
            "Immediate rows committed (TransactionId: %v, LockedRowCount: %v, LocklessRowCount: %v)",
            transaction->GetId(),
            lockedRowCount,
            locklessRowCount);

        SmallVector<TTableReplicaInfo*, 16> syncReplicas;
        SmallVector<TTablet*, 16> syncReplicaTablets;
        for (const auto& writeRecord : transaction->DelayedLocklessWriteLog()) {
            auto* tablet = FindTablet(writeRecord.TabletId);
            if (!tablet) {
                continue;
            }

            tablet->UpdateLastWriteTimestamp(commitTimestamp);

            if (!writeRecord.SyncReplicaIds.empty()) {
                syncReplicaTablets.push_back(tablet);
            }

            for (auto replicaId : writeRecord.SyncReplicaIds) {
                auto* replicaInfo = tablet->FindReplicaInfo(replicaId);
                if (!replicaInfo) {
                    continue;
                }

                syncReplicas.push_back(replicaInfo);
            }
        }

        SortUnique(syncReplicas);
        for (auto* replicaInfo : syncReplicas) {
            const auto* tablet = replicaInfo->GetTablet();
            auto oldCurrentReplicationTimestamp = replicaInfo->GetCurrentReplicationTimestamp();
            auto newCurrentReplicationTimestamp = std::max(oldCurrentReplicationTimestamp, commitTimestamp);
            replicaInfo->SetCurrentReplicationTimestamp(newCurrentReplicationTimestamp);
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
                "Sync replicated rows committed (TransactionId: %v, TabletId: %v, ReplicaId: %v, CurrentReplicationTimestamp: %llx -> %llx, "
                "TotalRowCount: %v)",
                transaction->GetId(),
                tablet->GetId(),
                replicaInfo->GetId(),
                oldCurrentReplicationTimestamp,
                newCurrentReplicationTimestamp,
                tablet->GetTotalRowCount());
        }

        SortUnique(syncReplicaTablets);
        for (auto* tablet : syncReplicaTablets) {
            AdvanceReplicatedTrimmedRowCount(tablet, transaction);
        }

        if (transaction->DelayedLocklessWriteLog().Empty()) {
            UnlockLockedTablets(transaction);
        }

        auto updateProfileCounters = [&] (const TTransactionWriteLog& log) {
            for (const auto& record : log) {
                auto* tablet = FindTablet(record.TabletId);
                if (!tablet) {
                    continue;
                }

                auto counters = tablet->GetTableProfiler()->GetCommitCounters(GetCurrentProfilingUser());
                counters->RowCount.Increment(record.RowCount);
                counters->DataWeight.Increment(record.DataWeight);
            }
        };
        updateProfileCounters(transaction->ImmediateLockedWriteLog());
        updateProfileCounters(transaction->ImmediateLocklessWriteLog());
        updateProfileCounters(transaction->DelayedLocklessWriteLog());

        ClearTransactionWriteLog(&transaction->ImmediateLockedWriteLog());
        ClearTransactionWriteLog(&transaction->ImmediateLocklessWriteLog());
    }

    void OnTransactionSerialized(TTransaction* transaction) noexcept
    {
        YT_VERIFY(transaction->PrelockedRows().empty());
        YT_VERIFY(transaction->LockedRows().empty());

        if (transaction->DelayedLocklessWriteLog().Empty()) {
            return;
        }

        auto commitTimestamp = transaction->GetCommitTimestamp();

        int rowCount = 0;
        TSmallFlatMap<TTablet*, int, 16> tabletToRowCount;
        for (const auto& record : transaction->DelayedLocklessWriteLog()) {
            auto* tablet = FindTablet(record.TabletId);
            if (!tablet) {
                continue;
            }

            TWriteContext context;
            context.Phase = EWritePhase::Commit;
            context.Transaction = transaction;
            context.CommitTimestamp = commitTimestamp;

            TWireProtocolReader reader(record.Data);

            const auto& storeManager = tablet->GetStoreManager();
            YT_VERIFY(storeManager->ExecuteWrites(&reader, &context));
            YT_VERIFY(context.RowCount == record.RowCount);

            tabletToRowCount[tablet] += record.RowCount;
            rowCount += context.RowCount;
        }

        for (auto [tablet, rowCount] : tabletToRowCount) {
            FinishTabletCommit(tablet, transaction, commitTimestamp);

            if (!tablet->IsReplicated()) {
                continue;
            }

            auto oldDelayedLocklessRowCount = tablet->GetDelayedLocklessRowCount();
            auto newDelayedLocklessRowCount = oldDelayedLocklessRowCount - rowCount;
            tablet->SetDelayedLocklessRowCount(newDelayedLocklessRowCount);
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
                "Delayed lockless rows committed (TransactionId: %v, TabletId: %v, DelayedLocklessRowCount: %v -> %v)",
                transaction->GetId(),
                tablet->GetId(),
                oldDelayedLocklessRowCount,
                newDelayedLocklessRowCount);
        }

        UnlockLockedTablets(transaction);

        ClearTransactionWriteLog(&transaction->DelayedLocklessWriteLog());
    }

    void OnTransactionAborted(TTransaction* transaction)
    {
        YT_VERIFY(transaction->PrelockedRows().empty());
        auto lockedRowCount = transaction->LockedRows().size();
        auto& lockedRows = transaction->LockedRows();
        while (!lockedRows.empty()) {
            auto rowRef = lockedRows.back();
            lockedRows.pop_back();
            if (ValidateAndDiscardRowRef(rowRef)) {
                rowRef.StoreManager->AbortRow(transaction, rowRef);
            }
        }
        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled() && lockedRowCount > 0,
            "Locked rows aborted (TransactionId: %v, RowCount: %v)",
            transaction->GetId(),
            lockedRowCount);

        // Check if above AbortRow calls caused store locks to be released.
        CheckIfImmediateLockedTabletsFullyUnlocked(transaction);

        auto lockedTabletCount = transaction->LockedTablets().size();
        UnlockLockedTablets(transaction);
        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled() && lockedTabletCount > 0,
            "Locked tablets unlocked (TransactionId: %v, TabletCount: %v)",
            transaction->GetId(),
            lockedTabletCount);

        if (transaction->GetRowsPrepared()) {
            TSmallFlatMap<TTableReplicaInfo*, int, 8> replicaToRowCount;
            TSmallFlatMap<TTablet*, int, 8> tabletToRowCount;
            for (const auto& writeRecord : transaction->DelayedLocklessWriteLog()) {
                auto* tablet = FindTablet(writeRecord.TabletId);
                if (!tablet || !tablet->IsReplicated()) {
                    continue;
                }

                for (auto replicaId : writeRecord.SyncReplicaIds) {
                    auto* replicaInfo = tablet->FindReplicaInfo(replicaId);
                    if (!replicaInfo) {
                        continue;
                    }
                    replicaToRowCount[replicaInfo] += writeRecord.RowCount;
                }

                tabletToRowCount[tablet] += writeRecord.RowCount;
            }

            for (auto [replicaInfo, rowCount] : replicaToRowCount) {
                const auto* tablet = replicaInfo->GetTablet();
                auto oldCurrentReplicationRowIndex = replicaInfo->GetCurrentReplicationRowIndex();
                auto newCurrentReplicationRowIndex = oldCurrentReplicationRowIndex - rowCount;
                replicaInfo->SetCurrentReplicationRowIndex(newCurrentReplicationRowIndex);
                YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
                    "Sync replicated rows aborted (TransactionId: %v, TabletId: %v, ReplicaId: %v, CurrentReplicationRowIndex: %v -> %v, "
                    "TotalRowCount: %v)",
                    transaction->GetId(),
                    tablet->GetId(),
                    replicaInfo->GetId(),
                    oldCurrentReplicationRowIndex,
                    newCurrentReplicationRowIndex,
                    tablet->GetTotalRowCount());
            }

            for (auto [tablet, rowCount] : tabletToRowCount) {
                auto oldDelayedLocklessRowCount = tablet->GetDelayedLocklessRowCount();
                auto newDelayedLocklessRowCount = oldDelayedLocklessRowCount - rowCount;
                tablet->SetDelayedLocklessRowCount(newDelayedLocklessRowCount);
                YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
                    "Delayed lockless rows aborted (TransactionId: %v, TabletId: %v, DelayedLocklessRowCount: %v -> %v)",
                    transaction->GetId(),
                    tablet->GetId(),
                    oldDelayedLocklessRowCount,
                    newDelayedLocklessRowCount);
            }
        }

        ClearTransactionWriteLog(&transaction->ImmediateLockedWriteLog());
        ClearTransactionWriteLog(&transaction->ImmediateLocklessWriteLog());
        ClearTransactionWriteLog(&transaction->DelayedLocklessWriteLog());
    }

    void OnTransactionTransientReset(TTransaction* transaction)
    {
        auto& prelockedRows = transaction->PrelockedRows();
        while (!prelockedRows.empty()) {
            auto rowRef = prelockedRows.front();
            prelockedRows.pop();
            if (ValidateAndDiscardRowRef(rowRef)) {
                rowRef.StoreManager->AbortRow(transaction, rowRef);
            }
        }
    }

    void FinishTabletCommit(
        TTablet* tablet,
        TTransaction* transaction,
        TTimestamp timestamp)
    {
        if (transaction &&
            !transaction->GetForeign() &&
            transaction->GetPrepareTimestamp() != NullTimestamp &&
            tablet->GetAtomicity() == EAtomicity::Full &&
            Slot_->GetAutomatonState() == EPeerState::Leading)
        {
            YT_VERIFY(tablet->GetUnflushedTimestamp() <= timestamp);
        }

        tablet->UpdateLastCommitTimestamp(timestamp);

        if (tablet->IsPhysicallyOrdered()) {
            auto oldTotalRowCount = tablet->GetTotalRowCount();
            tablet->UpdateTotalRowCount();
            auto newTotalRowCount = tablet->GetTotalRowCount();
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled() && oldTotalRowCount != newTotalRowCount,
                "Tablet total row count updated (TabletId: %v, TotalRowCount: %v -> %v)",
                tablet->GetId(),
                oldTotalRowCount,
                newTotalRowCount);
        }
    }


    static i64 GetTransactionWriteLogMemoryUsage(const TTransactionWriteLog& writeLog)
    {
        i64 result = 0;
        for (const auto& record : writeLog) {
            result += record.GetByteSize();
        }
        return result;
    }

    void EnqueueTransactionWriteRecord(
        TTransaction* transaction,
        TTransactionWriteLog* writeLog,
        const TTransactionWriteRecord& record,
        TTransactionSignature signature)
    {
        WriteLogsMemoryTrackerGuard_.IncrementSize(record.GetByteSize());
        writeLog->Enqueue(record);
        transaction->SetPersistentSignature(transaction->GetPersistentSignature() + signature);
    }

    void ClearTransactionWriteLog(TTransactionWriteLog* writeLog)
    {
        i64 byteSize = 0;
        for (const auto& record : *writeLog) {
            byteSize += record.GetByteSize();
        }
        WriteLogsMemoryTrackerGuard_.IncrementSize(-byteSize);
        writeLog->Clear();
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
            YT_VERIFY(OrphanedStores_.insert(dynamicStore).second);
            YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Dynamic memory store is orphaned and will be kept "
                "(StoreId: %v, TabletId: %v, LockCount: %v)",
                store->GetId(),
                tablet->GetId(),
                lockCount);
        }
    }

    bool ValidateRowRef(const TSortedDynamicRowRef& rowRef)
    {
        auto* store = rowRef.Store;
        return store->GetStoreState() != EStoreState::Orphaned;
    }

    bool ValidateAndDiscardRowRef(const TSortedDynamicRowRef& rowRef)
    {
        auto* store = rowRef.Store;
        if (store->GetStoreState() != EStoreState::Orphaned) {
            return true;
        }

        auto lockCount = store->Unlock();
        if (lockCount == 0) {
            YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Store unlocked and will be dropped (StoreId: %v)",
                store->GetId());
            YT_VERIFY(OrphanedStores_.erase(store) == 1);
        }

        return false;
    }


    void SetTabletOrphaned(std::unique_ptr<TTablet> tabletHolder)
    {
        auto id = tabletHolder->GetId();
        tabletHolder->SetState(ETabletState::Orphaned);
        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Tablet is orphaned and will be kept (TabletId: %v, LockCount: %v)",
            id,
            tabletHolder->GetTabletLockCount());
        YT_VERIFY(OrphanedTablets_.emplace(id, std::move(tabletHolder)).second);
    }

    i64 LockTablet(TTablet* tablet)
    {
        return tablet->Lock();
    }

    i64 UnlockTablet(TTablet* tablet)
    {
        auto lockCount = tablet->Unlock();
        CheckIfTabletFullyUnlocked(tablet);
        if (tablet->GetState() == ETabletState::Orphaned && lockCount == 0) {
            auto id = tablet->GetId();
            YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Tablet unlocked and will be dropped (TabletId: %v)",
                id);
            YT_VERIFY(OrphanedTablets_.erase(id) == 1);
        }
        return lockCount;
    }


    void PrepareLockedRows(TTransaction* transaction)
    {
        auto prepareRow = [&] (const TSortedDynamicRowRef& rowRef) {
            // NB: Don't call ValidateAndDiscardRowRef, row refs are just scanned.
            if (ValidateRowRef(rowRef)) {
                rowRef.StoreManager->PrepareRow(transaction, rowRef);
            }
        };

        auto lockedRowCount = transaction->LockedRows().size();
        auto prelockedRowCount = transaction->PrelockedRows().size();

        for (const auto& rowRef : transaction->LockedRows()) {
            prepareRow(rowRef);
        }

        for (auto it = transaction->PrelockedRows().begin();
             it != transaction->PrelockedRows().end();
             transaction->PrelockedRows().move_forward(it))
        {
            prepareRow(*it);
        }

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled() && (lockedRowCount + prelockedRowCount > 0),
            "Locked rows prepared (TransactionId: %v, LockedRowCount: %v, PrelockedRowCount: %v)",
            transaction->GetId(),
            lockedRowCount,
            prelockedRowCount);
    }


    void UnlockLockedTablets(TTransaction* transaction)
    {
        auto& tablets = transaction->LockedTablets();
        while (!tablets.empty()) {
            auto* tablet = tablets.back();
            tablets.pop_back();
            UnlockTablet(tablet);
        }
    }


    void CheckIfImmediateLockedTabletsFullyUnlocked(TTransaction* transaction)
    {
        for (const auto& record : transaction->ImmediateLockedWriteLog()) {
            auto* tablet = FindTablet(record.TabletId);
            if (!tablet) {
                continue;
            }

            CheckIfTabletFullyUnlocked(tablet);
        }
    }

    void CheckIfTabletFullyUnlocked(TTablet* tablet)
    {
        if (!IsLeader()) {
            return;
        }

        if (tablet->GetTabletLockCount() > 0) {
            return;
        }

        if (tablet->GetStoreManager()->HasActiveLocks()) {
            return;
        }

        NTracing::TNullTraceContextGuard guard;

        {
            TReqReportTabletLocked request;
            ToProto(request.mutable_tablet_id(), tablet->GetId());
            CommitTabletMutation(request);
        }

        auto state = tablet->GetState();
        if (state != ETabletState::UnmountWaitingForLocks && state != ETabletState::FreezeWaitingForLocks) {
            return;
        }

        ETabletState newTransientState;
        ETabletState newPersistentState;
        switch (state) {
            case ETabletState::UnmountWaitingForLocks:
                newTransientState = ETabletState::UnmountFlushPending;
                newPersistentState = ETabletState::UnmountFlushing;
                break;
            case ETabletState::FreezeWaitingForLocks:
                newTransientState = ETabletState::FreezeFlushPending;
                newPersistentState = ETabletState::FreezeFlushing;
                break;
            default:
                YT_ABORT();
        }
        tablet->SetState(newTransientState);

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "All tablet locks released (%v, NewState: %v)",
            tablet->GetLoggingTag(),
            newTransientState);

        {
            TReqSetTabletState request;
            ToProto(request.mutable_tablet_id(), tablet->GetId());
            request.set_mount_revision(tablet->GetMountRevision());
            request.set_state(static_cast<int>(newPersistentState));
            CommitTabletMutation(request);
        }
    }

    void CheckIfTabletFullyFlushed(TTablet* tablet)
    {
        if (!IsLeader()) {
            return;
        }

        auto state = tablet->GetState();
        if (state != ETabletState::UnmountFlushing && state != ETabletState::FreezeFlushing) {
            return;
        }

        if (tablet->GetStoreManager()->HasUnflushedStores()) {
            return;
        }

        ETabletState newTransientState;
        ETabletState newPersistentState;
        switch (state) {
            case ETabletState::UnmountFlushing:
                newTransientState = ETabletState::UnmountPending;
                newPersistentState = ETabletState::Unmounted;
                break;
            case ETabletState::FreezeFlushing:
                newTransientState = ETabletState::FreezePending;
                newPersistentState = ETabletState::Frozen;
                break;
            default:
                YT_ABORT();
        }
        tablet->SetState(newTransientState);

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "All tablet stores flushed (%v, NewState: %v)",
            tablet->GetLoggingTag(),
            newTransientState);

        TReqSetTabletState request;
        ToProto(request.mutable_tablet_id(), tablet->GetId());
        request.set_mount_revision(tablet->GetMountRevision());
        request.set_state(static_cast<int>(newPersistentState));
        CommitTabletMutation(request);
    }

    void CommitTabletMutation(const ::google::protobuf::MessageLite& message)
    {
        auto mutation = CreateMutation(Slot_->GetHydraManager(), message);
        Slot_->GetEpochAutomatonInvoker()->Invoke(BIND([=, this_ = MakeStrong(this), mutation = std::move(mutation)] {
            mutation->CommitAndLog(Logger);
        }));
    }

    void PostMasterMutation(TTabletId tabletId, const ::google::protobuf::MessageLite& message)
    {
        const auto& hiveManager = Slot_->GetHiveManager();
        auto* mailbox = hiveManager->GetOrCreateMailbox(Bootstrap_->GetCellId(CellTagFromId(tabletId)));
        if (!mailbox) {
            mailbox = Slot_->GetMasterMailbox();
        }
        hiveManager->PostMessage(mailbox, message);
    }


    void StartTabletEpoch(TTablet* tablet)
    {
        const auto& storeManager = tablet->GetStoreManager();
        storeManager->StartEpoch(Slot_);

        const auto& snapshotStore = Bootstrap_->GetTabletSnapshotStore();
        snapshotStore->RegisterTabletSnapshot(Slot_, tablet);

        for (auto& [replicaId, replicaInfo] : tablet->Replicas()) {
            StartTableReplicaEpoch(tablet, &replicaInfo);
        }
    }

    void StopTabletEpoch(TTablet* tablet)
    {
        const auto& storeManager = tablet->GetStoreManager();
        if (storeManager) {
            // Store Manager could be null if snapshot loading is aborted.
            storeManager->StopEpoch();
        }

        const auto& snapshotStore = Bootstrap_->GetTabletSnapshotStore();
        snapshotStore->UnregisterTabletSnapshot(Slot_, tablet);

        for (auto& [replicaId, replicaInfo] : tablet->Replicas()) {
            StopTableReplicaEpoch(&replicaInfo);
        }
    }


    void StartTableReplicaEpoch(TTablet* tablet, TTableReplicaInfo* replicaInfo)
    {
        YT_VERIFY(!replicaInfo->GetReplicator());

        if (IsLeader()) {
            auto replicator = New<TTableReplicator>(
                Config_,
                tablet,
                replicaInfo,
                Bootstrap_->GetMasterClient()->GetNativeConnection(),
                Slot_,
                Bootstrap_->GetTabletSnapshotStore(),
                Bootstrap_->GetHintManager(),
                CreateSerializedInvoker(Bootstrap_->GetTableReplicatorPoolInvoker()),
                EWorkloadCategory::SystemTabletReplication,
                Bootstrap_->GetOutThrottler(EWorkloadCategory::SystemTabletReplication));
            replicaInfo->SetReplicator(replicator);

            if (replicaInfo->GetState() == ETableReplicaState::Enabled) {
                replicator->Enable();
            }
        }
    }

    void StopTableReplicaEpoch(TTableReplicaInfo* replicaInfo)
    {
        if (!replicaInfo->GetReplicator()) {
            return;
        }
        replicaInfo->GetReplicator()->Disable();
        replicaInfo->SetReplicator(nullptr);
    }


    void SetBackingStore(TTablet* tablet, const IChunkStorePtr& store, const IDynamicStorePtr& backingStore)
    {
        store->SetBackingStore(backingStore);
        YT_LOG_DEBUG("Backing store set (%v, StoreId: %v, BackingStoreId: %v, BackingDynamicMemoryUsage: %v)",
            tablet->GetLoggingTag(),
            store->GetId(),
            backingStore->GetId(),
            backingStore->GetDynamicMemoryUsage());
        tablet->GetStructuredLogger()->OnBackingStoreSet(store, backingStore);

        TDelayedExecutor::Submit(
            // NB: Submit the callback via the regular automaton invoker, not the epoch one since
            // we need the store to be released even if the epoch ends.
            BIND(&TTabletManager::TImpl::ReleaseBackingStoreWeak, MakeWeak(this), MakeWeak(store))
                .Via(Slot_->GetAutomatonInvoker()),
            tablet->GetSettings().MountConfig->BackingStoreRetentionTime);
    }

    void ReleaseBackingStoreWeak(const TWeakPtr<IChunkStore>& storeWeak)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (auto store = storeWeak.Lock()) {
            ReleaseBackingStore(store);
        }
    }

    void BuildTabletOrchidYson(TTablet* tablet, IYsonConsumer* consumer)
    {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("table_id").Value(tablet->GetTableId())
                .Item("state").Value(tablet->GetState())
                .Item("hash_table_size").Value(tablet->GetHashTableSize())
                .Item("overlapping_store_count").Value(tablet->GetOverlappingStoreCount())
                .Item("retained_timestamp").Value(tablet->GetRetainedTimestamp())
                .Item("config")
                    .BeginAttributes()
                        .Item("opaque").Value(true)
                    .EndAttributes()
                    .Value(tablet->GetSettings().MountConfig)
                .Item("store_writer_config")
                    .BeginAttributes()
                        .Item("opaque").Value(true)
                    .EndAttributes()
                    .Value(tablet->GetSettings().StoreWriterConfig)
                .Item("store_writer_options")
                    .BeginAttributes()
                        .Item("opaque").Value(true)
                    .EndAttributes()
                    .Value(tablet->GetSettings().StoreWriterOptions)
                .Item("hunk_writer_config")
                    .BeginAttributes()
                        .Item("opaque").Value(true)
                    .EndAttributes()
                    .Value(tablet->GetSettings().HunkWriterConfig)
                .Item("hunk_writer_options")
                    .BeginAttributes()
                        .Item("opaque").Value(true)
                    .EndAttributes()
                    .Value(tablet->GetSettings().HunkWriterOptions)
                .Item("store_reader_config")
                    .BeginAttributes()
                        .Item("opaque").Value(true)
                    .EndAttributes()
                    .Value(tablet->GetSettings().StoreReaderConfig)
                .Item("hunk_reader_config")
                    .BeginAttributes()
                        .Item("opaque").Value(true)
                    .EndAttributes()
                    .Value(tablet->GetSettings().HunkReaderConfig)
                .DoIf(tablet->IsPhysicallySorted(), [&] (auto fluent) {
                    fluent
                        .Item("pivot_key").Value(tablet->GetPivotKey())
                        .Item("next_pivot_key").Value(tablet->GetNextPivotKey())
                        .Item("eden").DoMap(BIND(&TImpl::BuildPartitionOrchidYson, Unretained(this), tablet->GetEden()))
                        .Item("partitions").DoListFor(
                            tablet->PartitionList(), [&] (auto fluent, const std::unique_ptr<TPartition>& partition) {
                                fluent
                                    .Item()
                                    .DoMap(BIND(&TImpl::BuildPartitionOrchidYson, Unretained(this), partition.get()));
                            });
                })
                .DoIf(tablet->IsPhysicallyOrdered(), [&] (auto fluent) {
                    fluent
                        .Item("stores").DoMapFor(
                            tablet->StoreIdMap(),
                            [&] (auto fluent, const auto& pair) {
                                const auto& [storeId, store] = pair;
                                fluent
                                    .Item(ToString(storeId))
                                    .Do(BIND(&TImpl::BuildStoreOrchidYson, Unretained(this), store));
                            })
                        .Item("total_row_count").Value(tablet->GetTotalRowCount())
                        .Item("trimmed_row_count").Value(tablet->GetTrimmedRowCount());
                })
                .Item("hunk_chunks").DoMapFor(tablet->HunkChunkMap(), [&] (auto fluent, const auto& pair) {
                    const auto& [chunkId, hunkChunk] = pair;
                    fluent
                        .Item(ToString(chunkId))
                        .Do(BIND(&TImpl::BuildHunkChunkOrchidYson, Unretained(this), hunkChunk));
                })
                .DoIf(tablet->IsReplicated(), [&] (auto fluent) {
                    fluent
                        .Item("replicas").DoMapFor(
                            tablet->Replicas(),
                            [&] (auto fluent, const auto& pair) {
                                const auto& [replicaId, replica] = pair;
                                fluent
                                    .Item(ToString(replicaId))
                                    .Do(BIND(&TImpl::BuildReplicaOrchidYson, Unretained(this), replica));
                            });
                })
                .DoIf(tablet->IsPhysicallySorted(), [&] (auto fluent) {
                    fluent
                        .Item("dynamic_table_locks").DoMap(
                            BIND(&TLockManager::BuildOrchidYson, tablet->GetLockManager()));
                })
                .Item("errors").DoListFor(
                    TEnumTraits<ETabletBackgroundActivity>::GetDomainValues(),
                    [&] (auto fluent, auto activity) {
                        auto error = tablet->RuntimeData()->Errors[activity].Load();
                        if (!error.IsOK()) {
                            fluent
                                .Item().Value(error);
                        }
                    })
                .Item("replication_errors").DoMapFor(
                    tablet->Replicas(),
                    [&] (auto fluent, const auto& replica) {
                        auto replicaId = replica.first;
                        auto error = replica.second.GetError();
                        if (!error.IsOK()) {
                            fluent
                                .Item(ToString(replicaId)).Value(error);
                        }
                    })
                .DoIf(tablet->GetSettings().MountConfig->EnableDynamicStoreRead, [&] (auto fluent) {
                    fluent
                        .Item("dynamic_store_id_pool")
                            .BeginAttributes()
                                .Item("opaque").Value(true)
                            .EndAttributes()
                            .DoListFor(
                                tablet->DynamicStoreIdPool(),
                                [&] (auto fluent, auto dynamicStoreId) {
                                    fluent
                                        .Item().Value(dynamicStoreId);
                                });
                })
            .EndMap();
    }

    void BuildPartitionOrchidYson(TPartition* partition, TFluentMap fluent)
    {
        fluent
            .Item("id").Value(partition->GetId())
            .Item("state").Value(partition->GetState())
            .Item("pivot_key").Value(partition->GetPivotKey())
            .Item("next_pivot_key").Value(partition->GetNextPivotKey())
            .Item("sample_key_count").Value(partition->GetSampleKeys()->Keys.Size())
            .Item("sampling_time").Value(partition->GetSamplingTime())
            .Item("sampling_request_time").Value(partition->GetSamplingRequestTime())
            .Item("compaction_time").Value(partition->GetCompactionTime())
            .Item("allowed_split_time").Value(partition->GetAllowedSplitTime())
            .Item("uncompressed_data_size").Value(partition->GetUncompressedDataSize())
            .Item("compressed_data_size").Value(partition->GetCompressedDataSize())
            .Item("unmerged_row_count").Value(partition->GetUnmergedRowCount())
            .Item("stores").DoMapFor(partition->Stores(), [&] (auto fluent, const IStorePtr& store) {
                fluent
                    .Item(ToString(store->GetId()))
                    .Do(BIND(&TImpl::BuildStoreOrchidYson, Unretained(this), store));
            })
            .DoIf(partition->IsImmediateSplitRequested(), [&] (auto fluent) {
                fluent
                    .Item("immediate_split_keys").DoListFor(
                        partition->PivotKeysForImmediateSplit(),
                        [&] (auto fluent, const TLegacyOwningKey& key)
                    {
                        fluent
                            .Item().Value(key);
                    });

            });
    }

    void BuildStoreOrchidYson(const IStorePtr& store, TFluentAny fluent)
    {
        fluent
            .BeginAttributes()
                .Item("opaque").Value(true)
            .EndAttributes()
            .BeginMap()
                .Do(BIND(&IStore::BuildOrchidYson, store))
            .EndMap();
    }

    void BuildHunkChunkOrchidYson(const THunkChunkPtr& hunkChunk, TFluentAny fluent)
    {
        fluent
            .BeginAttributes()
                .Item("opaque").Value(true)
            .EndAttributes()
            .BeginMap()
                .Item("hunk_count").Value(hunkChunk->GetHunkCount())
                .Item("total_hunk_length").Value(hunkChunk->GetTotalHunkLength())
                .Item("referenced_hunk_count").Value(hunkChunk->GetReferencedHunkCount())
                .Item("referenced_total_hunk_length").Value(hunkChunk->GetReferencedTotalHunkLength())
                .Item("store_ref_count").Value(hunkChunk->GetStoreRefCount())
                .Item("prepared_store_ref_count").Value(hunkChunk->GetPreparedStoreRefCount())
                .Item("dangling").Value(hunkChunk->IsDangling())
            .EndMap();
    }

    void BuildReplicaOrchidYson(const TTableReplicaInfo& replica, TFluentAny fluent)
    {
        fluent
            .BeginMap()
                .Item("cluster_name").Value(replica.GetClusterName())
                .Item("replica_path").Value(replica.GetReplicaPath())
                .Item("state").Value(replica.GetState())
                .Item("mode").Value(replica.GetMode())
                .Item("atomicity").Value(replica.GetAtomicity())
                .Item("preserve_timestamps").Value(replica.GetPreserveTimestamps())
                .Item("start_replication_timestamp").Value(replica.GetStartReplicationTimestamp())
                .Item("current_replication_row_index").Value(replica.GetCurrentReplicationRowIndex())
                .Item("current_replication_timestamp").Value(replica.GetCurrentReplicationTimestamp())
                .Item("prepared_replication_transaction").Value(replica.GetPreparedReplicationTransactionId())
                .Item("prepared_replication_row_index").Value(replica.GetPreparedReplicationRowIndex())
            .EndMap();
    }


    void ValidateMemoryLimit(const std::optional<TString>& poolTag)
    {
        if (Bootstrap_->GetSlotManager()->IsOutOfMemory(poolTag)) {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::AllWritesDisabled,
                "Node is out of tablet memory, all writes disabled");
        }
    }

    void ValidateClientTimestamp(TTransactionId transactionId)
    {
        auto clientTimestamp = TimestampFromTransactionId(transactionId);
        auto serverTimestamp = Bootstrap_
            ->GetMasterConnection()
            ->GetTimestampProvider()
            ->GetLatestTimestamp();
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
        const auto& mountConfig = tablet->GetSettings().MountConfig;
        auto storeCount = std::ssize(tablet->StoreIdMap());
        auto storeLimit = mountConfig->MaxStoresPerTablet;
        if (storeCount >= storeLimit) {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::AllWritesDisabled,
                "Too many stores in tablet, all writes disabled")
                << TErrorAttribute("tablet_id", tablet->GetId())
                << TErrorAttribute("table_path", tablet->GetTablePath())
                << TErrorAttribute("store_count", storeCount)
                << TErrorAttribute("store_limit", storeLimit);
        }

        auto overlappingStoreCount = tablet->GetOverlappingStoreCount();
        auto overlappingStoreLimit = mountConfig->MaxOverlappingStoreCount;
        if (overlappingStoreCount >= overlappingStoreLimit) {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::AllWritesDisabled,
                "Too many overlapping stores in tablet, all writes disabled")
                << TErrorAttribute("tablet_id", tablet->GetId())
                << TErrorAttribute("table_path", tablet->GetTablePath())
                << TErrorAttribute("overlapping_store_count", overlappingStoreCount)
                << TErrorAttribute("overlapping_store_limit", overlappingStoreLimit);
        }

        auto edenStoreCount = tablet->GetEdenStoreCount();
        auto edenStoreCountLimit = mountConfig->MaxEdenStoresPerTablet;
        if (edenStoreCount >= edenStoreCountLimit) {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::AllWritesDisabled,
                "Too many eden stores in tablet, all writes disabled")
                << TErrorAttribute("tablet_id", tablet->GetId())
                << TErrorAttribute("table_path", tablet->GetTablePath())
                << TErrorAttribute("eden_store_count", edenStoreCount)
                << TErrorAttribute("eden_store_limit", edenStoreCountLimit);
        }

        auto overflow = tablet->GetStoreManager()->CheckOverflow();
        if (!overflow.IsOK()) {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::AllWritesDisabled,
                "Active store is overflown, all writes disabled")
                << TErrorAttribute("tablet_id", tablet->GetId())
                << TErrorAttribute("table_path", tablet->GetTablePath())
                << overflow;
        }
    }


    void UpdateTabletSnapshot(TTablet* tablet, std::optional<TLockManagerEpoch> epoch = std::nullopt)
    {
        if (!IsRecovery()) {
            const auto& snapshotStore = Bootstrap_->GetTabletSnapshotStore();
            snapshotStore->RegisterTabletSnapshot(Slot_, tablet, epoch);
        }
    }

    void ValidateTabletMounted(TTablet* tablet)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (tablet->GetState() != ETabletState::Mounted) {
            THROW_ERROR_EXCEPTION(
                NTabletClient::EErrorCode::TabletNotMounted,
                "Tablet %v is not in %Qlv state",
                tablet->GetId(),
                ETabletState::Mounted)
                << TErrorAttribute("tablet_id", tablet->GetId())
                << TErrorAttribute("table_path", tablet->GetTablePath())
                << TErrorAttribute("is_tablet_unmounted", tablet->GetState() == ETabletState::Unmounted);
        }
    }

    void ValidateTransactionActive(TTransaction* transaction)
    {
        if (transaction->GetState() != ETransactionState::Active) {
            transaction->ThrowInvalidState();
        }
    }


    template <class TRequest>
    NTabletNode::TTableSettings DeserializeTableSettings(TRequest* request, TTabletId tabletId)
    {
        // COMPAT(babenko)
        if (request->has_table_settings()) {
            const auto& tableSettings = request->table_settings();
            return {
                .MountConfig = DeserializeTableMountConfig(TYsonString(tableSettings.mount_config()), tabletId),
                .StoreReaderConfig = DeserializeTabletStoreReaderConfig(TYsonString(tableSettings.store_reader_config()), tabletId),
                .HunkReaderConfig = DeserializeTabletHunkReaderConfig(TYsonString(tableSettings.hunk_reader_config()), tabletId),
                .StoreWriterConfig = DeserializeTabletStoreWriterConfig(TYsonString(tableSettings.store_writer_config()), tabletId),
                .StoreWriterOptions = DeserializeTabletStoreWriterOptions(TYsonString(tableSettings.store_writer_options()), tabletId),
                .HunkWriterConfig = DeserializeTabletHunkWriterConfig(TYsonString(tableSettings.hunk_writer_config()), tabletId),
                .HunkWriterOptions = DeserializeTabletHunkWriterOptions(TYsonString(tableSettings.hunk_writer_options()), tabletId)
            };
        } else {
            auto mountConfig = DeserializeTableMountConfig(TYsonString(request->mount_config()), tabletId);
            auto storeReaderConfig = DeserializeTabletStoreReaderConfig(TYsonString(request->store_reader_config()), tabletId);
            auto storeWriterConfig = DeserializeTabletStoreWriterConfig(TYsonString(request->store_writer_config()), tabletId);
            auto storeWriterOptions = DeserializeTabletStoreWriterOptions(TYsonString(request->store_writer_options()), tabletId);
            return {
                .MountConfig = mountConfig,
                .StoreReaderConfig = storeReaderConfig,
                .HunkReaderConfig = New<TTabletHunkReaderConfig>(),
                .StoreWriterConfig = storeWriterConfig,
                .StoreWriterOptions = storeWriterOptions,
                .HunkWriterConfig = New<TTabletHunkWriterConfig>(),
                .HunkWriterOptions = TTablet::CreateFallbackHunkWriterOptions(storeWriterOptions)
            };
        }
    }

    TTableMountConfigPtr DeserializeTableMountConfig(const TYsonString& str, TTabletId tabletId)
    {
        try {
            return ConvertTo<TTableMountConfigPtr>(str);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR_IF(IsMutationLoggingEnabled(), ex, "Error deserializing tablet mount config (TabletId: %v)",
                 tabletId);
            return New<TTableMountConfig>();
        }
    }

    TTabletStoreReaderConfigPtr DeserializeTabletStoreReaderConfig(const TYsonString& str, TTabletId tabletId)
    {
        try {
            return ConvertTo<TTabletStoreReaderConfigPtr>(str);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR_IF(IsMutationLoggingEnabled(), ex, "Error deserializing store reader config (TabletId: %v)",
                 tabletId);
            return New<TTabletStoreReaderConfig>();
        }
    }

    TTabletHunkReaderConfigPtr DeserializeTabletHunkReaderConfig(const TYsonString& str, TTabletId tabletId)
    {
        try {
            return ConvertTo<TTabletHunkReaderConfigPtr>(str);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR_IF(IsMutationLoggingEnabled(), ex, "Error deserializing hunk reader config (TabletId: %v)",
                 tabletId);
            return New<TTabletHunkReaderConfig>();
        }
    }

    TTabletStoreWriterConfigPtr DeserializeTabletStoreWriterConfig(const TYsonString& str, TTabletId tabletId)
    {
        try {
            return ConvertTo<TTabletStoreWriterConfigPtr>(str);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR_IF(IsMutationLoggingEnabled(), ex, "Error deserializing store writer config (TabletId: %v)",
                 tabletId);
            return New<TTabletStoreWriterConfig>();
        }
    }

    TTabletStoreWriterOptionsPtr DeserializeTabletStoreWriterOptions(const TYsonString& str, TTabletId tabletId)
    {
        try {
            return ConvertTo<TTabletStoreWriterOptionsPtr>(str);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR_IF(IsMutationLoggingEnabled(), ex, "Error deserializing store writer options (TabletId: %v)",
                 tabletId);
            return New<TTabletStoreWriterOptions>();
        }
    }

    TTabletHunkWriterConfigPtr DeserializeTabletHunkWriterConfig(const TYsonString& str, const TTabletId tabletId)
    {
        try {
            return ConvertTo<TTabletHunkWriterConfigPtr>(str);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR_IF(IsMutationLoggingEnabled(), ex, "Error deserializing hunk writer config (TabletId: %v)",
                 tabletId);
            return New<TTabletHunkWriterConfig>();
        }
    }

    TTabletHunkWriterOptionsPtr DeserializeTabletHunkWriterOptions(const TYsonString& str, TTabletId tabletId)
    {
        try {
            return ConvertTo<TTabletHunkWriterOptionsPtr>(str);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR_IF(IsMutationLoggingEnabled(), ex, "Error deserializing hunk writer options (TabletId: %v)",
                 tabletId);
            return New<TTabletHunkWriterOptions>();
        }
    }



    IStoreManagerPtr CreateStoreManager(TTablet* tablet)
    {
        if (tablet->IsReplicated()) {
            return DoCreateStoreManager<TReplicatedStoreManager>(tablet);
        } else {
            if (tablet->IsPhysicallySorted()) {
                return DoCreateStoreManager<TSortedStoreManager>(tablet);
            } else {
                return DoCreateStoreManager<TOrderedStoreManager>(tablet);
            }
        }
    }

    template <class TImpl>
    IStoreManagerPtr DoCreateStoreManager(TTablet* tablet)
    {
        return New<TImpl>(
            Config_,
            tablet,
            &TabletContext_,
            Slot_->GetHydraManager(),
            Bootstrap_->GetInMemoryManager(),
            Bootstrap_->GetMasterClient());
    }


    IStorePtr CreateStore(
        TTablet* tablet,
        EStoreType type,
        TStoreId storeId,
        const TAddStoreDescriptor* descriptor)
    {
        auto store = DoCreateStore(tablet, type, storeId, descriptor);
        store->SetMemoryTracker(Bootstrap_->GetMemoryUsageTracker());
        return store;
    }

    TIntrusivePtr<TStoreBase> DoCreateStore(
        TTablet* tablet,
        EStoreType type,
        TStoreId storeId,
        const TAddStoreDescriptor* descriptor)
    {
        switch (type) {
            case EStoreType::SortedChunk: {
                NChunkClient::TLegacyReadRange readRange;
                TChunkId chunkId;
                auto chunkTimestamp = NullTimestamp;

                if (descriptor) {
                    if (descriptor->has_chunk_view_descriptor()) {
                        const auto& chunkViewDescriptor = descriptor->chunk_view_descriptor();
                        if (chunkViewDescriptor.has_read_range()) {
                            readRange = FromProto<NChunkClient::TLegacyReadRange>(chunkViewDescriptor.read_range());
                        }
                        if (chunkViewDescriptor.has_timestamp()) {
                            chunkTimestamp = static_cast<TTimestamp>(chunkViewDescriptor.timestamp());
                        }
                        chunkId = FromProto<TChunkId>(chunkViewDescriptor.underlying_chunk_id());
                    } else {
                        chunkId = storeId;
                    }
                } else {
                    YT_VERIFY(IsRecovery());
                }

                return New<TSortedChunkStore>(
                    Bootstrap_,
                    Config_,
                    storeId,
                    chunkId,
                    readRange,
                    chunkTimestamp,
                    tablet,
                    descriptor,
                    Bootstrap_->GetBlockCache(),
                    Bootstrap_->GetVersionedChunkMetaManager(),
                    Bootstrap_->GetChunkRegistry(),
                    Bootstrap_->GetChunkBlockManager(),
                    Bootstrap_->GetMasterClient(),
                    Bootstrap_->GetLocalDescriptor());
            }

            case EStoreType::SortedDynamic:
                return New<TSortedDynamicStore>(
                    Config_,
                    storeId,
                    tablet);

            case EStoreType::OrderedChunk: {
                if (!IsRecovery()) {
                    YT_VERIFY(descriptor);
                    YT_VERIFY(!descriptor->has_chunk_view_descriptor());
                }

                return New<TOrderedChunkStore>(
                    Bootstrap_,
                    Config_,
                    storeId,
                    tablet,
                    descriptor,
                    Bootstrap_->GetBlockCache(),
                    Bootstrap_->GetVersionedChunkMetaManager(),
                    Bootstrap_->GetChunkRegistry(),
                    Bootstrap_->GetChunkBlockManager(),
                    Bootstrap_->GetMasterClient(),
                    Bootstrap_->GetLocalDescriptor());
            }

            case EStoreType::OrderedDynamic:
                return New<TOrderedDynamicStore>(
                    Config_,
                    storeId,
                    tablet);

            default:
                YT_ABORT();
        }
    }


    THunkChunkPtr CreateHunkChunk(
        TTablet* /*tablet*/,
        TChunkId chunkId,
        const TAddHunkChunkDescriptor* descriptor)
    {
        return New<THunkChunk>(
            chunkId,
            descriptor);
    }


    TTableReplicaInfo* AddTableReplica(TTablet* tablet, const TTableReplicaDescriptor& descriptor)
    {
        auto replicaId = FromProto<TTableReplicaId>(descriptor.replica_id());
        auto& replicas = tablet->Replicas();
        if (replicas.find(replicaId) != replicas.end()) {
            YT_LOG_WARNING_IF(IsMutationLoggingEnabled(), "Requested to add an already existing table replica (TabletId: %v, ReplicaId: %v)",
                tablet->GetId(),
                replicaId);
            return nullptr;
        }

        auto [replicaIt, replicaInserted] = replicas.emplace(replicaId, TTableReplicaInfo(tablet, replicaId));
        YT_VERIFY(replicaInserted);
        auto& replicaInfo = replicaIt->second;

        replicaInfo.SetClusterName(descriptor.cluster_name());
        replicaInfo.SetReplicaPath(descriptor.replica_path());
        replicaInfo.SetStartReplicationTimestamp(descriptor.start_replication_timestamp());
        replicaInfo.SetState(ETableReplicaState::Disabled);
        replicaInfo.SetMode(ETableReplicaMode(descriptor.mode()));
        if (descriptor.has_atomicity()) {
            replicaInfo.SetAtomicity(NTransactionClient::EAtomicity(descriptor.atomicity()));
        }
        if (descriptor.has_preserve_timestamps()) {
            replicaInfo.SetPreserveTimestamps(descriptor.preserve_timestamps());
        }
        replicaInfo.MergeFromStatistics(descriptor.statistics());

        tablet->UpdateReplicaCounters();
        UpdateTabletSnapshot(tablet);

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Table replica added (%v, ReplicaId: %v, ClusterName: %v, ReplicaPath: %v, "
            "Mode: %v, StartReplicationTimestamp: %llx, CurrentReplicationRowIndex: %v, CurrentReplicationTimestamp: %llx)",
            tablet->GetLoggingTag(),
            replicaId,
            replicaInfo.GetClusterName(),
            replicaInfo.GetReplicaPath(),
            replicaInfo.GetMode(),
            replicaInfo.GetStartReplicationTimestamp(),
            replicaInfo.GetCurrentReplicationRowIndex(),
            replicaInfo.GetCurrentReplicationTimestamp());

        return &replicaInfo;
    }

    void RemoveTableReplica(TTablet* tablet, TTableReplicaId replicaId)
    {
        auto& replicas = tablet->Replicas();
        auto it = replicas.find(replicaId);
        if (it == replicas.end()) {
            YT_LOG_WARNING_IF(IsMutationLoggingEnabled(), "Requested to remove a non-existing table replica (TabletId: %v, ReplicaId: %v)",
                tablet->GetId(),
                replicaId);
            return;
        }

        auto& replicaInfo = it->second;

        if (!IsRecovery()) {
            StopTableReplicaEpoch(&replicaInfo);
        }

        replicas.erase(it);

        AdvanceReplicatedTrimmedRowCount(tablet, nullptr);
        UpdateTabletSnapshot(tablet);

        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Table replica removed (%v, ReplicaId: %v)",
            tablet->GetLoggingTag(),
            replicaId);
    }


    void EnableTableReplica(TTablet* tablet, TTableReplicaInfo* replicaInfo)
    {
        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Table replica enabled (%v, ReplicaId: %v)",
            tablet->GetLoggingTag(),
            replicaInfo->GetId());

        replicaInfo->SetState(ETableReplicaState::Enabled);

        if (IsLeader()) {
            replicaInfo->GetReplicator()->Enable();
        }

        {
            TRspEnableTableReplica response;
            ToProto(response.mutable_tablet_id(), tablet->GetId());
            ToProto(response.mutable_replica_id(), replicaInfo->GetId());
            response.set_mount_revision(tablet->GetMountRevision());
            PostMasterMutation(tablet->GetId(), response);
        }
    }

    void DisableTableReplica(TTablet* tablet, TTableReplicaInfo* replicaInfo)
    {
        YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Table replica disabled (%v, ReplicaId: %v, "
            "CurrentReplicationRowIndex: %v, CurrentReplicationTimestamp: %llx)",
            tablet->GetLoggingTag(),
            replicaInfo->GetId(),
            replicaInfo->GetCurrentReplicationRowIndex(),
            replicaInfo->GetCurrentReplicationTimestamp());

        replicaInfo->SetState(ETableReplicaState::Disabled);
        replicaInfo->SetError(TError());

        if (IsLeader()) {
            replicaInfo->GetReplicator()->Disable();
        }

        PostTableReplicaStatistics(tablet, *replicaInfo);

        {
            TRspDisableTableReplica response;
            ToProto(response.mutable_tablet_id(), tablet->GetId());
            ToProto(response.mutable_replica_id(), replicaInfo->GetId());
            response.set_mount_revision(tablet->GetMountRevision());
            PostMasterMutation(tablet->GetId(), response);
        }
    }

    void PostTableReplicaStatistics(TTablet* tablet, const TTableReplicaInfo& replicaInfo)
    {
        TReqUpdateTableReplicaStatistics request;
        ToProto(request.mutable_tablet_id(), tablet->GetId());
        ToProto(request.mutable_replica_id(), replicaInfo.GetId());
        request.set_mount_revision(tablet->GetMountRevision());
        replicaInfo.PopulateStatistics(request.mutable_statistics());
        PostMasterMutation(tablet->GetId(), request);
    }


    void UpdateTrimmedRowCount(TTablet* tablet, i64 trimmedRowCount)
    {
        auto prevTrimmedRowCount = tablet->GetTrimmedRowCount();
        if (trimmedRowCount <= prevTrimmedRowCount) {
            return;
        }
        tablet->SetTrimmedRowCount(trimmedRowCount);

        {
            TReqUpdateTabletTrimmedRowCount masterRequest;
            ToProto(masterRequest.mutable_tablet_id(), tablet->GetId());
            masterRequest.set_mount_revision(tablet->GetMountRevision());
            masterRequest.set_trimmed_row_count(trimmedRowCount);
            PostMasterMutation(tablet->GetId(), masterRequest);
        }

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Rows trimmed (TabletId: %v, TrimmedRowCount: %v -> %v)",
            tablet->GetId(),
            prevTrimmedRowCount,
            trimmedRowCount);
    }

    void AdvanceReplicatedTrimmedRowCount(TTablet* tablet, TTransaction* transaction)
    {
        YT_VERIFY(tablet->IsReplicated());

        if (tablet->Replicas().empty()) {
            return;
        }

        auto minReplicationRowIndex = std::numeric_limits<i64>::max();
        for (const auto& [replicaId, replicaInfo] : tablet->Replicas()) {
            minReplicationRowIndex = std::min(minReplicationRowIndex, replicaInfo.GetCurrentReplicationRowIndex());
        }

        const auto& storeRowIndexMap = tablet->StoreRowIndexMap();
        if (storeRowIndexMap.empty()) {
            return;
        }

        const auto& mountConfig = tablet->GetSettings().MountConfig;
        auto retentionDeadline = transaction
            ? TimestampToInstant(transaction->GetCommitTimestamp()).first - mountConfig->MinReplicationLogTtl
            : TInstant::Max();
        auto it = storeRowIndexMap.find(tablet->GetTrimmedRowCount());
        while (it != storeRowIndexMap.end()) {
            const auto& store = it->second;
            if (store->IsDynamic()) {
                break;
            }
            if (minReplicationRowIndex < store->GetStartingRowIndex() + store->GetRowCount()) {
                break;
            }
            if (TimestampToInstant(store->GetMaxTimestamp()).first > retentionDeadline) {
                break;
            }
            ++it;
        }

        i64 trimmedRowCount;
        if (it == storeRowIndexMap.end()) {
            // Looks like a full trim.
            // Typically we have a sentinel dynamic store at the end but during unmount this one may be missing.
            YT_VERIFY(!storeRowIndexMap.empty());
            const auto& lastStore = storeRowIndexMap.rbegin()->second;
            YT_VERIFY(minReplicationRowIndex == lastStore->GetStartingRowIndex() + lastStore->GetRowCount());
            trimmedRowCount = minReplicationRowIndex;
        } else {
            trimmedRowCount = it->second->GetStartingRowIndex();
        }

        YT_VERIFY(tablet->GetTrimmedRowCount() <= trimmedRowCount);
        UpdateTrimmedRowCount(tablet, trimmedRowCount);
    }


    void OnStoresUpdateCommitSemaphoreAcquired(
        TTablet* tablet,
        const ITransactionPtr& transaction,
        TPromise<void> promise,
        TAsyncSemaphoreGuard /*guard*/)
    {
        try {
            YT_LOG_DEBUG("Started committing tablet stores update transaction (%v, TransactionId: %v)",
                tablet->GetLoggingTag(),
                transaction->GetId());

            WaitFor(transaction->Commit())
                .ThrowOnError();

            YT_LOG_DEBUG("Tablet stores update transaction committed (%v, TransactionId: %v)",
                tablet->GetLoggingTag(),
                transaction->GetId());

            promise.Set();
        } catch (const std::exception& ex) {
            promise.Set(TError(ex));
        }
    }
};

DEFINE_ENTITY_MAP_ACCESSORS(TTabletManager::TImpl, Tablet, TTablet, TabletMap_)

////////////////////////////////////////////////////////////////////////////////

TTabletManager::TTabletManager(
    TTabletManagerConfigPtr config,
    ITabletSlotPtr slot,
    IBootstrap* bootstrap)
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

TTablet* TTabletManager::GetTabletOrThrow(TTabletId id)
{
    return Impl_->GetTabletOrThrow(id);
}

void TTabletManager::Write(
    TTabletSnapshotPtr tabletSnapshot,
    TTransactionId transactionId,
    TTimestamp transactionStartTimestamp,
    TDuration transactionTimeout,
    TTransactionSignature signature,
    int rowCount,
    size_t dataWeight,
    bool versioned,
    const TSyncReplicaIdList& syncReplicaIds,
    TWireProtocolReader* reader,
    TFuture<void>* commitResult)
{
    return Impl_->Write(
        std::move(tabletSnapshot),
        transactionId,
        transactionStartTimestamp,
        transactionTimeout,
        signature,
        rowCount,
        dataWeight,
        versioned,
        syncReplicaIds,
        reader,
        commitResult);
}

TFuture<void> TTabletManager::Trim(
    TTabletSnapshotPtr tabletSnapshot,
    i64 trimmedRowCount)
{
    return Impl_->Trim(
        std::move(tabletSnapshot),
        trimmedRowCount);
}

void TTabletManager::ScheduleStoreRotation(TTablet* tablet)
{
    Impl_->ScheduleStoreRotation(tablet);
}

TFuture<void> TTabletManager::CommitTabletStoresUpdateTransaction(
    TTablet* tablet,
    const ITransactionPtr& transaction)
{
    return Impl_->CommitTabletStoresUpdateTransaction(tablet, transaction);
}

void TTabletManager::ReleaseBackingStore(const IChunkStorePtr& store)
{
    Impl_->ReleaseBackingStore(store);
}

IYPathServicePtr TTabletManager::GetOrchidService()
{
    return Impl_->GetOrchidService();
}

ETabletCellLifeStage TTabletManager::GetTabletCellLifeStage() const
{
    return Impl_->GetTabletCellLifeStage();
}

DELEGATE_ENTITY_MAP_ACCESSORS(TTabletManager, Tablet, TTablet, *Impl_)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
