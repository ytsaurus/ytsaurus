#include "in_memory_manager.h"
#include "partition.h"
#include "private.h"
#include "public.h"
#include "slot_manager.h"
#include "sorted_chunk_store.h"
#include "store_compactor.h"
#include "store_manager.h"
#include "structured_logger.h"
#include "tablet.h"
#include "tablet_manager.h"
#include "tablet_profiling.h"
#include "tablet_reader.h"
#include "tablet_slot.h"
#include "tablet_snapshot_store.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/server/node/data_node/legacy_master_connector.h>

#include <yt/yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/transaction.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/yt/ytlib/chunk_client/config.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/ytlib/table_client/versioned_chunk_writer.h>

#include <yt/yt/ytlib/tablet_client/config.h>

#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/client/table_client/versioned_reader.h>
#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>
#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/async_semaphore.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/profiling/profiler.h>
#include <yt/yt/core/profiling/profile_manager.h>

#include <yt/yt/core/ytree/helpers.h>
#include <yt/yt/core/ytree/virtual.h>

#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/heap.h>

#include <yt/yt/core/yson/consumer.h>

#include <yt/yt/core/ytalloc/memory_zone.h>

namespace NYT::NTabletNode {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NHydra;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTabletNode::NProto;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NYTAlloc;
using namespace NYson;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const size_t MaxRowsPerRead = 65536;
static const size_t MaxRowsPerWrite = 65536;

static const size_t FinishedQueueSize = 100;

////////////////////////////////////////////////////////////////////////////////

/*!
 * Ultimately, the goal of the compactor is to control the overlapping store count
 * by performing compactions and partitionings. A compaction operates within a partition,
 * replacing a set of stores with a newly baked one. A partitioning operates on the Eden,
 * replacing a set of Eden stores with a set of partition-bound stores.
 */
class TStoreCompactor
    : public IStoreCompactor
{
public:
    explicit TStoreCompactor(NClusterNode::TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
        , Config_(bootstrap->GetConfig()->TabletNode->StoreCompactor)
        , ThreadPool_(New<TThreadPool>(Config_->ThreadPoolSize, "StoreCompact"))
        , PartitioningSemaphore_(New<TProfiledAsyncSemaphore>(
            Config_->MaxConcurrentPartitionings,
            Profiler_.Gauge("/running_partitionings")))
        , CompactionSemaphore_(New<TProfiledAsyncSemaphore>(
            Config_->MaxConcurrentCompactions,
            Profiler_.Gauge("/running_compactions")))
        , OrchidService_(CreateOrchidService())
    { }

    virtual void Start() override
    {
        const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
        dynamicConfigManager->SubscribeConfigChanged(BIND(&TStoreCompactor::OnDynamicConfigChanged, MakeWeak(this)));

        const auto& slotManager = Bootstrap_->GetTabletSlotManager();
        slotManager->SubscribeBeginSlotScan(BIND(&TStoreCompactor::OnBeginSlotScan, MakeWeak(this)));
        slotManager->SubscribeScanSlot(BIND(&TStoreCompactor::OnScanSlot, MakeWeak(this)));
        slotManager->SubscribeEndSlotScan(BIND(&TStoreCompactor::OnEndSlotScan, MakeWeak(this)));
    }

    virtual IYPathServicePtr GetOrchidService() override
    {
        return OrchidService_;
    }

private:
    NClusterNode::TBootstrap* const Bootstrap_;
    const TStoreCompactorConfigPtr Config_;

    const TProfiler Profiler_ = TabletNodeProfiler.WithPrefix("/store_compactor");
    const TGauge FeasiblePartitioningsCounter_ = Profiler_.Gauge("/feasible_partitionings");
    const TGauge FeasibleCompactionsCounter_ = Profiler_.Gauge("/feasible_compactions");
    const TCounter ScheduledPartitioningsCounter_ = Profiler_.Counter("/scheduled_partitionings");
    const TCounter ScheduledCompactionsCounter_ = Profiler_.Counter("/scheduled_compactions");
    const TEventTimer ScanTimer_ = Profiler_.Timer("/scan_time");

    const TThreadPoolPtr ThreadPool_;
    const TAsyncSemaphorePtr PartitioningSemaphore_;
    const TAsyncSemaphorePtr CompactionSemaphore_;

    IYPathServicePtr CreateOrchidService()
    {
        return New<TCompositeMapService>()
            ->AddAttribute(EInternedAttributeKey::Opaque, BIND([] (IYsonConsumer* consumer) {
                BuildYsonFluently(consumer)
                    .Value(true);
            }))
            ->AddChild("compaction_tasks", IYPathService::FromProducer(
                BIND(&TOrchidServiceManager::GetTasks, MakeWeak(CompactionOrchidServiceManager_))))
            ->AddChild("partitioning_tasks", IYPathService::FromProducer(
                BIND(&TOrchidServiceManager::GetTasks, MakeWeak(PartitioningOrchidServiceManager_))))
            ->Via(Bootstrap_->GetControlInvoker());
    }

    template <typename T>
    static auto GetOrderingTuple(const T& task)
    {
        return std::make_tuple(
            !task->DiscardStores,
            task->Slack + task->FutureEffect,
            -task->Effect,
            -task->GetStoreCount(),
            task->Random);
    }

    struct TTask
    {
        TTabletSlotPtr Slot;
        IInvokerPtr Invoker;

        TTabletId TabletId;
        TRevision MountRevision;
        TString TabletLoggingTag;
        TPartitionId PartitionId;
        std::vector<TStoreId> StoreIds;

        // True if the all chunks should be discarded. The task does not
        // require reading and writing chunks.
        bool DiscardStores = false;
        // Overlapping stores slack for the task.
        // That is, the remaining number of stores in the partition till
        // the tablet hits MOSC limit.
        // Small values indicate that the tablet is in a critical state.
        int Slack = 0;
        // Guaranteed effect on the slack if this task will be done.
        // This is a conservative estimate.
        int Effect = 0;
        // Future effect on the slack due to concurrent tasks.
        // This quantity is memoized to provide a valid comparison operator.
        int FutureEffect = 0;
        // A random number to deterministically break ties.
        ui64 Random = RandomNumber<size_t>();

        // These fields are filled upon task invocation.
        TWeakPtr<TStoreCompactor> Owner;
        TAsyncSemaphoreGuard SemaphoreGuard;

        TTask() = delete;
        TTask(const TTask&) = delete;
        TTask& operator=(const TTask&) = delete;
        TTask(TTask&&) = delete;
        TTask& operator=(TTask&&) = delete;

        TTask(
            TTabletSlot* slot,
            const TTablet* tablet,
            const TPartition* partition,
            std::vector<TStoreId> stores)
            : Slot(slot)
            , Invoker(tablet->GetEpochAutomatonInvoker())
            , TabletId(tablet->GetId())
            , MountRevision(tablet->GetMountRevision())
            , TabletLoggingTag(tablet->GetLoggingTag())
            , PartitionId(partition->GetId())
            , StoreIds(std::move(stores))
        { }

        ~TTask()
        {
            if (auto owner = Owner.Lock()) {
                owner->ChangeFutureEffect(TabletId, -Effect);
            }
        }

        auto GetStoreCount()
        {
            return StoreIds.size();
        }

        void Prepare(TStoreCompactor* owner, TAsyncSemaphoreGuard&& semaphoreGuard)
        {
            Owner = MakeWeak(owner);
            SemaphoreGuard = std::move(semaphoreGuard);

            owner->ChangeFutureEffect(TabletId, Effect);
        }

        void StoreToStructuredLog(TFluentMap fluent)
        {
            fluent
                .Item("partition_id").Value(PartitionId)
                .Item("store_ids").DoListFor(StoreIds, [&] (TFluentList fluent, TStoreId storeId) {
                    fluent
                        .Item().Value(storeId);
                })
                .Item("discard_stores").Value(DiscardStores)
                .Item("slack").Value(Slack)
                .Item("effect").Value(Effect)
                .Item("future_effect").Value(FutureEffect)
                .Item("random").Value(Random);
        }

        static inline bool Comparer(
            const std::unique_ptr<TTask>& lhs,
            const std::unique_ptr<TTask>& rhs)
        {
            return TStoreCompactor::GetOrderingTuple(lhs) < TStoreCompactor::GetOrderingTuple(rhs);
        }
    };

    class TOrchidServiceManager
        : public TRefCounted
    {
    public:
        void MarkTaskAsFinished()
        {
            auto guard = Guard(QueueSpinLock_);

            YT_VERIFY(FinishedTaskQueue_.size() <= FinishedQueueSize);
            YT_VERIFY(!TaskQueue_.empty());

            if (FinishedTaskQueue_.size() == FinishedQueueSize) {
                FinishedTaskQueue_.pop_front();
            }

            FinishedTaskQueue_.push_back(TaskQueue_.front());
            TaskQueue_.pop_front();
        }

        void ResetTaskQueue(
            std::vector<std::unique_ptr<TTask>>::iterator begin,
            std::vector<std::unique_ptr<TTask>>::iterator end)
        {
            auto guard = Guard(QueueSpinLock_);

            TaskQueue_.clear();
            for (auto it = begin; it != end; ++it) {
                TaskQueue_.push_back(New<TTaskInfo>(*it->get()));
            }
        }

        void GetTasks(IYsonConsumer* consumer) const
        {
            auto guard = Guard(QueueSpinLock_);
            auto taskQueue = TaskQueue_;
            auto finishedTaskQueue = FinishedTaskQueue_;
            guard.Release();

            std::sort(taskQueue.begin(), taskQueue.end(), Comparer);

            BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("task_count").Value(taskQueue.size())
                    .Item("finished_task_count").Value(finishedTaskQueue.size())
                    .Item("pending_tasks")
                        .DoListFor(
                            taskQueue,
                            [&](TFluentList fluent, const auto& task) {
                                task->Serialize(fluent);
                            })
                    .Item("finished_tasks")
                        .DoListFor(
                            finishedTaskQueue,
                            [&](TFluentList fluent, const auto& task) {
                                task->Serialize(fluent);
                            })
                .EndMap();
        }

    private:
        struct TTaskInfo
            : public TRefCounted
        {
            explicit TTaskInfo(const TTask& task)
                : TabletId(task.TabletId)
                , MountRevision(task.MountRevision)
                , PartitionId(task.PartitionId)
                , StoreCount(task.StoreIds.size())
                , DiscardStores(task.DiscardStores)
                , Slack(task.Slack)
                , Effect(task.Effect)
                , FutureEffect(task.FutureEffect)
                , Random(task.Random)
            { }

            TTabletId TabletId;
            TRevision MountRevision;
            TPartitionId PartitionId;
            int StoreCount;

            bool DiscardStores;
            int Slack;
            int Effect;
            int FutureEffect;
            ui64 Random;

            auto GetStoreCount()
            {
                return StoreCount;
            }

            void Serialize(TFluentList fluent) const
            {
                fluent.Item()
                .BeginMap()
                    .Item("tablet_id").Value(TabletId)
                    .Item("mount_revision").Value(MountRevision)
                    .Item("partition_id").Value(PartitionId)
                    .Item("store_count").Value(StoreCount)
                    .Item("task_priority")
                        .BeginMap()
                            .Item("discard_stores").Value(DiscardStores)
                            .Item("slack").Value(Slack)
                            .Item("future_effect").Value(FutureEffect)
                            .Item("effect").Value(Effect)
                            .Item("random").Value(Random)
                        .EndMap()
                 .EndMap();
            }
        };

        using TTaskInfoPtr = TIntrusivePtr<TTaskInfo>;

        YT_DECLARE_SPINLOCK(TAdaptiveLock, QueueSpinLock_);
        std::deque<TTaskInfoPtr> TaskQueue_;
        std::deque<TTaskInfoPtr> FinishedTaskQueue_;

        static inline bool Comparer(const TTaskInfoPtr& lhs, const TTaskInfoPtr& rhs)
        {
            return TStoreCompactor::GetOrderingTuple(lhs) < TStoreCompactor::GetOrderingTuple(rhs);
        }
    };

    using TOrchidServiceManagerPtr = TIntrusivePtr<TOrchidServiceManager>;

    // Variables below contain per-iteration state for slot scan.
    YT_DECLARE_SPINLOCK(TAdaptiveLock, ScanSpinLock_);
    bool ScanForPartitioning_;
    bool ScanForCompactions_;
    std::vector<std::unique_ptr<TTask>> PartitioningCandidates_;
    std::vector<std::unique_ptr<TTask>> CompactionCandidates_;

    // Variables below are actually used during the scheduling.
    YT_DECLARE_SPINLOCK(TAdaptiveLock, TaskSpinLock_);
    std::vector<std::unique_ptr<TTask>> PartitioningTasks_; // Min-heap.
    size_t PartitioningTaskIndex_ = 0; // Heap end boundary.
    std::vector<std::unique_ptr<TTask>> CompactionTasks_; // Min-heap.
    size_t CompactionTaskIndex_ = 0; // Heap end boundary.

    // These are for the future accounting.
    YT_DECLARE_SPINLOCK(TReaderWriterSpinLock, FutureEffectLock_);
    THashMap<TTabletId, int> FutureEffect_;

    const TOrchidServiceManagerPtr CompactionOrchidServiceManager_ = New<TOrchidServiceManager>();
    const TOrchidServiceManagerPtr PartitioningOrchidServiceManager_ = New<TOrchidServiceManager>();
    IYPathServicePtr OrchidService_;


    void OnDynamicConfigChanged(
        const NClusterNode::TClusterNodeDynamicConfigPtr& /* oldNodeConfig */,
        const NClusterNode::TClusterNodeDynamicConfigPtr& newNodeConfig)
    {
        const auto& config = newNodeConfig->TabletNode->StoreCompactor;
        ThreadPool_->Configure(config->ThreadPoolSize.value_or(Config_->ThreadPoolSize));
        PartitioningSemaphore_->SetTotal(config->MaxConcurrentPartitionings.value_or(Config_->MaxConcurrentPartitionings));
        CompactionSemaphore_->SetTotal(config->MaxConcurrentPartitionings.value_or(Config_->MaxConcurrentCompactions));
    }

    void OnBeginSlotScan()
    {
        // NB: Strictly speaking, redundant.
        auto guard = Guard(ScanSpinLock_);

        // Save some scheduling resources by skipping unnecessary work.
        ScanForPartitioning_ = PartitioningSemaphore_->IsReady();
        ScanForCompactions_ = CompactionSemaphore_->IsReady();
        PartitioningCandidates_.clear(); // Though must be clear already.
        CompactionCandidates_.clear(); // Though must be clear already.
    }

    void OnScanSlot(const TTabletSlotPtr& slot)
    {
        TEventTimer timerGuard(ScanTimer_);

        const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
        auto dynamicConfig = dynamicConfigManager->GetConfig()->TabletNode->StoreCompactor;
        if (!dynamicConfig->Enable) {
            return;
        }

        if (slot->GetAutomatonState() != EPeerState::Leading) {
            return;
        }

        const auto& tabletManager = slot->GetTabletManager();
        for (auto [tabletId, tablet] : tabletManager->Tablets()) {
            ScanTablet(slot.Get(), tablet);
        }
    }

    void OnEndSlotScan()
    {
        // NB: Strictly speaking, redundant.
        auto guard = Guard(ScanSpinLock_);

        if (ScanForPartitioning_) {
            PickMorePartitionings(guard);
            ScheduleMorePartitionings();
        }

        if (ScanForCompactions_) {
            PickMoreCompactions(guard);
            ScheduleMoreCompactions();
        }
    }

    void ScanTablet(TTabletSlot* slot, TTablet* tablet)
    {
        if (tablet->GetState() != ETabletState::Mounted) {
            return;
        }

        if (!tablet->IsPhysicallySorted()) {
            return;
        }

        const auto& config = tablet->GetConfig();
        if (!config->EnableCompactionAndPartitioning) {
            return;
        }

        ScanEdenForPartitioning(slot, tablet->GetEden());
        ScanPartitionForCompaction(slot, tablet->GetEden());

        for (auto& partition : tablet->PartitionList()) {
            ScanPartitionForCompaction(slot, partition.get());
        }
    }

    bool ScanEdenForPartitioning(TTabletSlot* slot, TPartition* eden)
    {
        if (!ScanForPartitioning_ || eden->GetState() != EPartitionState::Normal) {
            return false;
        }

        const auto* tablet = eden->GetTablet();

        auto stores = PickStoresForPartitioning(eden);
        if (stores.empty()) {
            return false;
        }

        auto candidate = std::make_unique<TTask>(slot, tablet, eden, std::move(stores));
        // We aim to improve OSC; partitioning unconditionally improves OSC (given at least two stores).
        // So we consider how constrained is the tablet, and how many stores we consider for partitioning.
        const int overlappingStoreLimit = GetOverlappingStoreLimit(tablet->GetConfig());
        const int overlappingStoreCount = tablet->GetOverlappingStoreCount();
        candidate->Slack = std::max(0, overlappingStoreLimit - overlappingStoreCount);
        candidate->Effect = candidate->StoreIds.size() - 1;
        candidate->FutureEffect = GetFutureEffect(tablet->GetId());

        tablet->GetStructuredLogger()->LogEvent("partitioning_candidate")
            .Do(BIND(&TTask::StoreToStructuredLog, candidate.get()));

        {
            auto guard = Guard(ScanSpinLock_);
            PartitioningCandidates_.push_back(std::move(candidate));
        }

        return true;
    }

    bool TryDiscardExpiredPartition(TTabletSlot* slot, TPartition* partition)
    {
        if (partition->IsEden()) {
            return false;
        }

        const auto* tablet = partition->GetTablet();

        const auto& config = tablet->GetConfig();
        if (!config->EnableDiscardingExpiredPartitions || config->MinDataVersions != 0) {
            return false;
        }

        for (const auto& store : partition->Stores()) {
            if (store->AsSortedChunk()->GetCompactionState() != EStoreCompactionState::None) {
                return false;
            }
        }

        auto timestampProvider = Bootstrap_
            ->GetMasterClient()
            ->GetNativeConnection()
            ->GetTimestampProvider();
        auto currentTimestamp = timestampProvider->GetLatestTimestamp();

        auto partitionMaxTimestamp = NullTimestamp;
        for (const auto& store : partition->Stores()) {
            partitionMaxTimestamp = std::max(partitionMaxTimestamp, store->GetMaxTimestamp());
        }

        if (partitionMaxTimestamp >= currentTimestamp ||
            TimestampDiffToDuration(partitionMaxTimestamp, currentTimestamp).first <= config->MaxDataTtl)
        {
            return false;
        }

        auto majorTimestamp = currentTimestamp;
        for (const auto& store : tablet->GetEden()->Stores()) {
            majorTimestamp = std::min(majorTimestamp, store->GetMinTimestamp());
        }

        if (partitionMaxTimestamp >= majorTimestamp) {
            return false;
        }

        std::vector<TStoreId> stores;
        for (const auto& store : partition->Stores()) {
            stores.push_back(store->GetId());
        }
        auto candidate = std::make_unique<TTask>(slot, tablet, partition, std::move(stores));
        candidate->DiscardStores = true;

        const auto& Logger = TabletNodeLogger;
        YT_LOG_DEBUG("Found partition with expired stores (%v, PartitionId: %v, PartitionIndex: %v, "
            "PartitionMaxTimestamp: %v, MajorTimestamp: %v, StoreCount: %v)",
            tablet->GetLoggingTag(),
            partition->GetId(),
            partition->GetIndex(),
            partitionMaxTimestamp,
            majorTimestamp,
            partition->Stores().size());

        tablet->GetStructuredLogger()->LogEvent("discard_stores_candidate")
            .Do(BIND(&TTask::StoreToStructuredLog, candidate.get()));

        {
            auto guard = Guard(ScanSpinLock_);
            CompactionCandidates_.push_back(std::move(candidate));
        }

        return true;
    }

    bool ScanPartitionForCompaction(TTabletSlot* slot, TPartition* partition)
    {
        if (!ScanForCompactions_ ||
            partition->GetState() != EPartitionState::Normal ||
            partition->IsImmediateSplitRequested() ||
            partition->Stores().empty())
        {
            return false;
        }

        const auto* tablet = partition->GetTablet();

        if (TryDiscardExpiredPartition(slot, partition)) {
            return true;
        }


        auto stores = PickStoresForCompaction(partition);
        if (stores.empty()) {
            return false;
        }

        auto candidate = std::make_unique<TTask>(slot, tablet, partition, std::move(stores));
        // We aim to improve OSC; compaction improves OSC _only_ if the partition contributes towards OSC.
        // So we consider how constrained is the partition, and how many stores we consider for compaction.
        const int overlappingStoreLimit = GetOverlappingStoreLimit(tablet->GetConfig());
        const int overlappingStoreCount = tablet->GetOverlappingStoreCount();
        if (partition->IsEden()) {
            // Normalized eden store count dominates when number of eden stores is too close to its limit.
            int normalizedEdenStoreCount = tablet->GetEdenStoreCount() * overlappingStoreLimit /
                tablet->GetConfig()->MaxEdenStoresPerTablet;
            int overlappingStoreLimitSlackness = overlappingStoreLimit -
                std::max(overlappingStoreCount, normalizedEdenStoreCount);

            candidate->Slack = std::max(0, overlappingStoreLimitSlackness);
            candidate->Effect = candidate->StoreIds.size() - 1;
        } else {
            // For critical partitions, this is equivalent to MOSC-OSC; for unconstrained -- includes extra slack.
            const int edenOverlappingStoreCount = tablet->GetEdenOverlappingStoreCount();
            const int partitionStoreCount = static_cast<int>(partition->Stores().size());
            candidate->Slack = std::max(0, overlappingStoreLimit - edenOverlappingStoreCount - partitionStoreCount);
            if (tablet->GetCriticalPartitionCount() == 1 &&
                edenOverlappingStoreCount + partitionStoreCount == overlappingStoreCount)
            {
                candidate->Effect = candidate->StoreIds.size() - 1;
            }
        }
        candidate->FutureEffect = GetFutureEffect(tablet->GetId());

        tablet->GetStructuredLogger()->LogEvent("compaction_candidate")
            .Do(BIND(&TTask::StoreToStructuredLog, candidate.get()));

        {
            auto guard = Guard(ScanSpinLock_);
            CompactionCandidates_.push_back(std::move(candidate));
        }

        return true;
    }


    std::vector<TStoreId> PickStoresForPartitioning(TPartition* eden)
    {
        std::vector<TStoreId> finalists;

        const auto* tablet = eden->GetTablet();
        const auto& storeManager = tablet->GetStoreManager();
        const auto& config = tablet->GetConfig();

        std::vector<TSortedChunkStorePtr> candidates;

        for (const auto& store : eden->Stores()) {
            if (!storeManager->IsStoreCompactable(store)) {
                continue;
            }

            auto candidate = store->AsSortedChunk();
            candidates.push_back(candidate);

            if (IsCompactionForced(candidate) ||
                IsPeriodicCompactionNeeded(candidate) ||
                IsStoreOutOfTabletRange(candidate, tablet))
            {
                finalists.push_back(candidate->GetId());
            }

            if (finalists.size() >= config->MaxPartitioningStoreCount) {
                break;
            }
        }

        // Check for forced candidates.
        if (!finalists.empty()) {
            return finalists;
        }

        // Sort by decreasing data size.
        std::sort(
            candidates.begin(),
            candidates.end(),
            [] (const TSortedChunkStorePtr& lhs, const TSortedChunkStorePtr& rhs) {
                return lhs->GetCompressedDataSize() > rhs->GetCompressedDataSize();
            });

        i64 dataSizeSum = 0;
        int bestStoreCount = -1;
        for (int i = 0; i < candidates.size(); ++i) {
            dataSizeSum += candidates[i]->GetCompressedDataSize();
            int storeCount = i + 1;
            if (storeCount >= config->MinPartitioningStoreCount &&
                storeCount <= config->MaxPartitioningStoreCount &&
                dataSizeSum >= config->MinPartitioningDataSize &&
                // Ignore max_partitioning_data_size limit for a minimal set of stores.
                (dataSizeSum <= config->MaxPartitioningDataSize || storeCount == config->MinPartitioningStoreCount))
            {
                // Prefer to partition more data.
                bestStoreCount = storeCount;
            }
        }

        if (bestStoreCount > 0) {
            finalists.reserve(bestStoreCount);
            for (int i = 0; i < bestStoreCount; ++i) {
                finalists.push_back(candidates[i]->GetId());
            }
        }

        return finalists;
    }

    std::vector<TStoreId> PickStoresForCompaction(TPartition* partition)
    {
        std::vector<TStoreId> finalists;

        const auto* tablet = partition->GetTablet();
        const auto& storeManager = tablet->GetStoreManager();
        const auto& config = tablet->GetConfig();

        auto Logger = TabletNodeLogger;
        Logger.AddTag("%v, PartitionId: %v",
            tablet->GetLoggingTag(),
            partition->GetId());

        YT_LOG_DEBUG_IF(config->EnableLsmVerboseLogging,
            "Picking stores for compaction");

        std::vector<TSortedChunkStorePtr> candidates;

        for (const auto& store : partition->Stores()) {
            if (!storeManager->IsStoreCompactable(store)) {
                continue;
            }

            // Don't compact large Eden stores.
            if (partition->IsEden() && store->GetCompressedDataSize() >= config->MinPartitioningDataSize) {
                continue;
            }

            auto candidate = store->AsSortedChunk();
            candidates.push_back(candidate);

            if (IsCompactionForced(candidate) ||
                IsPeriodicCompactionNeeded(candidate) ||
                IsStoreOutOfTabletRange(candidate, tablet))
            {
                finalists.push_back(candidate->GetId());

                if (config->EnableLsmVerboseLogging) {
                    TString reason;
                    if (IsCompactionForced(candidate)) {
                        reason = "forced compaction";
                    } else if (IsPeriodicCompactionNeeded(candidate)) {
                        reason = "periodic compaction";
                    } else {
                        reason = "store is out of tablet range";
                    }
                    YT_LOG_DEBUG("Finalist store picked out of order (StoreId: %v, Reason: %v)",
                        candidate->GetId(),
                        reason);
                }
            }

            if (finalists.size() >= config->MaxCompactionStoreCount) {
                break;
            }
        }

        // Check for forced candidates.
        if (!finalists.empty()) {
            return finalists;
        }

        // Sort by increasing data size.
        std::sort(
            candidates.begin(),
            candidates.end(),
            [] (TSortedChunkStorePtr lhs, TSortedChunkStorePtr rhs) {
                return lhs->GetCompressedDataSize() < rhs->GetCompressedDataSize();
            });

        int overlappingStoreCount;
        if (partition->IsEden()) {
            overlappingStoreCount = tablet->GetOverlappingStoreCount();
        } else {
            overlappingStoreCount = partition->Stores().size() + tablet->GetEdenOverlappingStoreCount();
        }
        // Partition is critical if it contributes towards the OSC, and MOSC is reached.
        bool criticalPartition = overlappingStoreCount >= GetOverlappingStoreLimit(config);

        if (criticalPartition) {
            YT_LOG_DEBUG_IF(config->EnableLsmVerboseLogging,
                "Partition is critical, picking as many stores as possible");
        }

        for (int i = 0; i < candidates.size(); ++i) {
            i64 dataSizeSum = 0;
            int j = i;
            while (j < candidates.size()) {
                int storeCount = j - i;
                if (storeCount > config->MaxCompactionStoreCount) {
                   break;
                }
                i64 dataSize = candidates[j]->GetCompressedDataSize();
                if (!criticalPartition &&
                    dataSize > config->CompactionDataSizeBase &&
                    dataSizeSum > 0 && dataSize > dataSizeSum * config->CompactionDataSizeRatio) {
                    break;
                }
                dataSizeSum += dataSize;
                ++j;
            }

            int storeCount = j - i;
            if (storeCount >= config->MinCompactionStoreCount) {
                finalists.reserve(storeCount);
                while (i < j) {
                    finalists.push_back(candidates[i]->GetId());
                    ++i;
                }
                YT_LOG_DEBUG_IF(config->EnableLsmVerboseLogging,
                    "Picked stores for compaction (DataSize: %v, StoreId: %v)",
                    dataSizeSum,
                    MakeFormattableView(
                        MakeRange(finalists),
                        TDefaultFormatter{}));
                break;
            }
        }

        return finalists;
    }

    static TTimestamp ComputeMajorTimestamp(
        TPartition* partition,
        const std::vector<TSortedChunkStorePtr>& stores)
    {
        auto result = MaxTimestamp;
        auto handleStore = [&] (const ISortedStorePtr& store) {
            result = std::min(result, store->GetMinTimestamp());
        };

        auto* tablet = partition->GetTablet();
        auto* eden = tablet->GetEden();

        for (const auto& store : eden->Stores()) {
            handleStore(store);
        }

        for (const auto& store : partition->Stores()) {
            if (store->GetType() == EStoreType::SortedChunk) {
                if (std::find(stores.begin(), stores.end(), store->AsSortedChunk()) == stores.end()) {
                    handleStore(store);
                }
            }
        }

        return result;
    }

    void PickMoreTasks(
        std::vector<std::unique_ptr<TTask>>* candidates,
        std::vector<std::unique_ptr<TTask>>* tasks,
        size_t* index,
        const TOrchidServiceManagerPtr& orchidServiceManager,
        const NProfiling::TGauge& counter)
    {
        counter.Update(candidates->size());

        MakeHeap(candidates->begin(), candidates->end(), TTask::Comparer);

        {
            auto guard = Guard(TaskSpinLock_);
            orchidServiceManager->ResetTaskQueue(candidates->begin(), candidates->end());
            tasks->swap(*candidates);
            *index = tasks->size();
        }

        candidates->clear();
    }

    void PickMorePartitionings(TSpinlockGuard<TAdaptiveLock>& /*guard*/)
    {
        PickMoreTasks(
            &PartitioningCandidates_,
            &PartitioningTasks_,
            &PartitioningTaskIndex_,
            PartitioningOrchidServiceManager_,
            FeasiblePartitioningsCounter_);
    }

    void PickMoreCompactions(TSpinlockGuard<TAdaptiveLock>& /*guard*/)
    {
        PickMoreTasks(
            &CompactionCandidates_,
            &CompactionTasks_,
            &CompactionTaskIndex_,
            CompactionOrchidServiceManager_,
            FeasibleCompactionsCounter_);
    }

    void ScheduleMoreTasks(
        std::vector<std::unique_ptr<TTask>>* tasks,
        size_t* index,
        const TOrchidServiceManagerPtr& orchidServiceManager,
        const TAsyncSemaphorePtr& semaphore,
        const TCounter& counter,
        void (TStoreCompactor::*action)(TTask*))
    {
        auto taskGuard = Guard(TaskSpinLock_);

        size_t scheduled = 0;

        while (true) {
            if (*index == 0) {
                break;
            }

            auto semaphoreGuard = TAsyncSemaphoreGuard::TryAcquire(semaphore);
            if (!semaphoreGuard) {
                break;
            }

            // Check if we have to fix the heap. If the smallest element is okay, we just keep going.
            // Hopefully, we will rarely decide to operate on the same tablet.
            {
                auto guard = ReaderGuard(FutureEffectLock_);
                auto&& firstTask = tasks->at(0);
                if (firstTask->FutureEffect != LockedGetFutureEffect(guard, firstTask->TabletId)) {
                    for (size_t i = 0; i < *index; ++i) {
                        auto&& task = (*tasks)[i];
                        task->FutureEffect = LockedGetFutureEffect(guard, task->TabletId);
                    }
                    guard.Release();
                    MakeHeap(tasks->begin(), tasks->begin() + *index, TTask::Comparer);
                    orchidServiceManager->ResetTaskQueue(tasks->begin(), tasks->begin() + *index);
                }
            }

            // Extract the next task.
            ExtractHeap(tasks->begin(), tasks->begin() + *index, TTask::Comparer);
            --(*index);
            auto&& task = tasks->at(*index);
            task->Prepare(this, std::move(semaphoreGuard));
            ++scheduled;
            orchidServiceManager->MarkTaskAsFinished();

            // TODO(sandello): Better ownership management.
            auto invoker = task->Invoker;
            invoker->Invoke(BIND(action, MakeStrong(this), Owned(task.release())));
        }

        if (scheduled > 0) {
            counter.Increment(scheduled);
        }
    }

    void ScheduleMorePartitionings()
    {
        ScheduleMoreTasks(
            &PartitioningTasks_,
            &PartitioningTaskIndex_,
            PartitioningOrchidServiceManager_,
            PartitioningSemaphore_,
            ScheduledPartitioningsCounter_,
            &TStoreCompactor::PartitionEden);
    }

    void ScheduleMoreCompactions()
    {
        ScheduleMoreTasks(
            &CompactionTasks_,
            &CompactionTaskIndex_,
            CompactionOrchidServiceManager_,
            CompactionSemaphore_,
            ScheduledCompactionsCounter_,
            &TStoreCompactor::CompactPartition);
    }

    int LockedGetFutureEffect(NConcurrency::TSpinlockReaderGuard<TReaderWriterSpinLock>&, TTabletId tabletId)
    {
        auto it = FutureEffect_.find(tabletId);
        return it != FutureEffect_.end() ? it->second : 0;
    }

    int GetFutureEffect(TTabletId tabletId)
    {
        auto guard = ReaderGuard(FutureEffectLock_);
        return LockedGetFutureEffect(guard, tabletId);
    }

    void ChangeFutureEffect(TTabletId tabletId, int delta)
    {
        if (delta == 0) {
            return;
        }
        auto guard = WriterGuard(FutureEffectLock_);
        auto pair = FutureEffect_.emplace(tabletId, delta);
        if (!pair.second) {
            pair.first->second += delta;
        }
        const auto& Logger = TabletNodeLogger;
        YT_LOG_DEBUG("Accounting for the future effect of the compaction/partitioning (TabletId: %v, FutureEffect: %v -> %v)",
            tabletId,
            pair.first->second - delta,
            pair.first->second);
        if (pair.first->second == 0) {
            FutureEffect_.erase(pair.first);
        }
    }

    NNative::ITransactionPtr StartMasterTransaction(const TTabletSnapshotPtr& tabletSnapshot, const TString& title)
    {
        auto transactionAttributes = CreateEphemeralAttributes();
        transactionAttributes->Set("title", title);
        auto asyncTransaction = Bootstrap_->GetMasterClient()->StartNativeTransaction(
            NTransactionClient::ETransactionType::Master,
            TTransactionStartOptions{
                .AutoAbort = false,
                .Attributes = std::move(transactionAttributes),
                .CoordinatorMasterCellTag = CellTagFromId(tabletSnapshot->TabletId),
                .ReplicateToMasterCellTags = TCellTagList()
            });
        return WaitFor(asyncTransaction)
            .ValueOrThrow();
    }

    NNative::ITransactionPtr StartCompactionTransaction(
        const TTabletSnapshotPtr& tabletSnapshot,
        const NLogging::TLogger& Logger)
    {
        YT_LOG_INFO("Creating partition compaction transaction");

        auto transaction = StartMasterTransaction(tabletSnapshot, Format("Partition compaction: table %v, tablet %v",
            tabletSnapshot->TablePath,
            tabletSnapshot->TabletId));

        YT_LOG_INFO("Partition compaction transaction created (TransactionId: %v)",
            transaction->GetId());

        return transaction;
    }

    NNative::ITransactionPtr StartPartitioningTransaction(
        const TTabletSnapshotPtr& tabletSnapshot,
        const NLogging::TLogger& Logger)
    {
        YT_LOG_INFO("Creating Eden partitioning transaction");

        auto transaction = StartMasterTransaction(tabletSnapshot, Format("Eden partitioning: table %v, tablet %v",
            tabletSnapshot->TablePath,
            tabletSnapshot->TabletId));

        YT_LOG_INFO("Eden partitioning transaction created (TransactionId: %v)",
            transaction->GetId());

        return transaction;
    }

    void FinishTabletStoresUpdateTransaction(
        TTablet* tablet,
        const TTabletSlotPtr& slot,
        NTabletServer::NProto::TReqUpdateTabletStores actionRequest,
        NNative::ITransactionPtr transaction,
        const NLogging::TLogger& Logger)
    {
        tablet->ThrottleTabletStoresUpdate(slot, Logger);

        ToProto(actionRequest.mutable_tablet_id(), tablet->GetId());
        actionRequest.set_mount_revision(tablet->GetMountRevision());

        auto actionData = MakeTransactionActionData(actionRequest);
        auto masterCellId = Bootstrap_->GetCellId(CellTagFromId(tablet->GetId()));

        transaction->AddAction(masterCellId, actionData);
        transaction->AddAction(slot->GetCellId(), actionData);

        const auto& tabletManager = slot->GetTabletManager();
        WaitFor(tabletManager->CommitTabletStoresUpdateTransaction(tablet, transaction))
            .ThrowOnError();
    }

    void PartitionEden(TTask* task)
    {
        TClientBlockReadOptions blockReadOptions;
        blockReadOptions.WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::SystemTabletPartitioning),
        blockReadOptions.ChunkReaderStatistics = New<TChunkReaderStatistics>();
        blockReadOptions.ReadSessionId = TReadSessionId::Create();

        NLogging::TLogger Logger(TabletNodeLogger);
        Logger.AddTag("%v, ReadSessionId: %v",
            task->TabletLoggingTag,
            blockReadOptions.ReadSessionId);

        auto doneGuard = Finally([&] {
            ScheduleMorePartitionings();
        });

        const auto& slot = task->Slot;
        const auto& tabletManager = slot->GetTabletManager();
        auto* tablet = tabletManager->FindTablet(task->TabletId);
        if (!tablet) {
            YT_LOG_DEBUG("Tablet is missing, aborting partitioning");
            return;
        }

        const auto& snapshotStore = Bootstrap_->GetTabletSnapshotStore();
        auto tabletSnapshot = snapshotStore->FindTabletSnapshot(task->TabletId, task->MountRevision);
        if (!tabletSnapshot) {
            YT_LOG_DEBUG("Tablet snapshot is missing, aborting partitioning");
            return;
        }

        const auto& storeManager = tablet->GetStoreManager();
        const auto& structuredLogger = tablet->GetStructuredLogger();

        auto logFailure = [&] (TStringBuf message) {
            return structuredLogger->LogEvent("abort_partitioning")
                .Item("reason").Value(message)
                .Item("partition_id").Value(task->PartitionId);
        };


        auto* eden = tablet->GetEden();
        if (eden->GetId() != task->PartitionId) {
            YT_LOG_DEBUG("Eden is missing, aborting partitioning");
            logFailure("eden_missing");
            return;
        }

        if (eden->GetState() != EPartitionState::Normal) {
            YT_LOG_DEBUG("Eden is in improper state, aborting partitioning (EdenState: %v)", eden->GetState());
            logFailure("improper_state")
                .Item("state").Value(eden->GetState());
            return;
        }

        std::vector<TSortedChunkStorePtr> stores;
        stores.reserve(task->StoreIds.size());
        for (const auto& storeId : task->StoreIds) {
            auto store = tablet->FindStore(storeId);
            if (!store || !eden->Stores().contains(store->AsSorted())) {
                YT_LOG_DEBUG("Eden store is missing, aborting partitioning (StoreId: %v)", storeId);
                logFailure("store_missing")
                    .Item("store_id").Value(storeId);
                return;
            }
            auto typedStore = store->AsSortedChunk();
            YT_VERIFY(typedStore);
            if (typedStore->GetCompactionState() != EStoreCompactionState::None) {
                YT_LOG_DEBUG("Eden store is in improper state, aborting partitioning (StoreId: %v, CompactionState: %v)",
                    storeId,
                    typedStore->GetCompactionState());
                logFailure("improper_store_state")
                    .Item("store_id").Value(storeId)
                    .Item("store_compaction_state").Value(typedStore->GetCompactionState());
                return;
            }
            stores.push_back(std::move(typedStore));
        }

        std::vector<TLegacyOwningKey> pivotKeys;
        for (const auto& partition : tablet->PartitionList()) {
            if (!partition->GetTablet()->GetConfig()->EnablePartitionSplitWhileEdenPartitioning &&
                partition->GetState() == EPartitionState::Splitting)
            {
                YT_LOG_DEBUG("Other partition is splitting, aborting eden partitioning (PartitionId: %v)",
                     partition->GetId());
                logFailure("other_partition_splitting")
                    .Item("other_partition_id").Value(partition->GetId());
                return;
            }
            pivotKeys.push_back(partition->GetPivotKey());
        }

        YT_VERIFY(tablet->GetPivotKey() == pivotKeys[0]);

        eden->CheckedSetState(EPartitionState::Normal, EPartitionState::Partitioning);

        std::vector<IVersionedMultiChunkWriterPtr> writers;
        IVersionedReaderPtr reader;

        auto readerProfiler = New<TReaderProfiler>();
        auto writerProfiler = New<TWriterProfiler>();

        bool failed = false;

        try {
            i64 dataSize = 0;
            for (const auto& store : stores) {
                dataSize += store->GetCompressedDataSize();
                storeManager->BeginStoreCompaction(store);
            }

            auto timestampProvider = Bootstrap_
                ->GetMasterClient()
                ->GetNativeConnection()
                ->GetTimestampProvider();
            auto currentTimestamp = WaitFor(timestampProvider->GenerateTimestamps())
                .ValueOrThrow();

            auto beginInstant = TInstant::Now();
            eden->SetCompactionTime(beginInstant);

            structuredLogger->LogEvent("start_partitioning")
                .Item("partition_id").Value(eden->GetId())
                .Item("store_ids").Value(task->StoreIds)
                .Item("current_timestamp").Value(currentTimestamp);

            auto bandwidthThrottler = Bootstrap_->GetTabletNodeInThrottler(EWorkloadCategory::SystemTabletPartitioning);

            YT_LOG_INFO("Eden partitioning started (Slack: %v, FutureEffect: %v, Effect: %v, "
                "PartitionCount: %v, CompressedDataSize: %v, "
                "ChunkCount: %v, CurrentTimestamp: %llx, RetentionConfig: %v)",
                task->Slack,
                task->FutureEffect,
                task->Effect,
                pivotKeys.size(),
                dataSize,
                stores.size(),
                currentTimestamp,
                ConvertTo<TRetentionConfigPtr>(tabletSnapshot->Config));

            reader = CreateVersionedTabletReader(
                tabletSnapshot,
                std::vector<ISortedStorePtr>(stores.begin(), stores.end()),
                tablet->GetPivotKey(),
                tablet->GetNextPivotKey(),
                currentTimestamp,
                MinTimestamp, // NB: No major compaction during Eden partitioning.
                blockReadOptions,
                stores.size(),
                ETabletDistributedThrottlerKind::CompactionRead,
                std::move(bandwidthThrottler));

            auto transaction = StartPartitioningTransaction(tabletSnapshot, Logger);
            Logger.AddTag("TransactionId: %v", transaction->GetId());

            auto asyncResult =
                BIND(
                    &TStoreCompactor::DoPartitionEden,
                    MakeStrong(this),
                    reader,
                    tabletSnapshot,
                    transaction,
                    pivotKeys,
                    tablet->GetNextPivotKey(),
                    Logger)
                .AsyncVia(ThreadPool_->GetInvoker())
                .Run();

            auto partitioningResult = WaitFor(asyncResult)
                .ValueOrThrow();
            const auto& partitionWritersWithIndex = partitioningResult.Writers;

            auto endInstant = TInstant::Now();

            // We can release semaphore, because we are no longer actively using resources.
            task->SemaphoreGuard.Release();
            doneGuard.Release();

            NTabletServer::NProto::TReqUpdateTabletStores actionRequest;
            actionRequest.set_update_reason(ToProto<int>(ETabletStoresUpdateReason::Partitioning));

            TStoreIdList storeIdsToRemove;
            for (const auto& store : stores) {
                auto* descriptor = actionRequest.add_stores_to_remove();
                auto storeId = store->GetId();
                ToProto(descriptor->mutable_store_id(), storeId);
                storeIdsToRemove.push_back(storeId);
            }

            std::vector<std::pair<TChunkId, int>> loggableChunkInfos;

            // TODO(sandello): Move specs?
            TStoreIdList storeIdsToAdd;
            for (const auto& [writer, partitionIndex] : partitionWritersWithIndex) {
                for (const auto& chunkSpec : writer->GetWrittenChunksMasterMeta()) {
                    auto* descriptor = actionRequest.add_stores_to_add();
                    descriptor->set_store_type(static_cast<int>(EStoreType::SortedChunk));
                    descriptor->mutable_store_id()->CopyFrom(chunkSpec.chunk_id());
                    descriptor->mutable_chunk_meta()->CopyFrom(chunkSpec.chunk_meta());
                    storeIdsToAdd.push_back(FromProto<TStoreId>(chunkSpec.chunk_id()));

                    loggableChunkInfos.emplace_back(
                        FromProto<TChunkId>(chunkSpec.chunk_id()),
                        partitionIndex
                    );
                }

                tabletSnapshot->PerformanceCounters->PartitioningDataWeightCount += writer->GetDataStatistics().data_weight();
            }

            YT_LOG_INFO("Eden partitioning completed (RowCount: %v, StoreIdsToAdd: %v, StoreIdsToRemove: %v, WallTime: %v)",
                partitioningResult.RowCount,
                storeIdsToAdd,
                storeIdsToRemove,
                endInstant - beginInstant);

            structuredLogger->LogEvent("end_partitioning")
                .Item("partition_id").Value(eden->GetId())
                .Item("stores_to_add").DoListFor(
                    loggableChunkInfos,
                    [&] (TFluentList fluent, const auto& chunkInfo) {
                        fluent
                            .Item().BeginMap()
                                .Item("chunk_id").Value(chunkInfo.first)
                                .Item("partition_index").Value(chunkInfo.second)
                            .EndMap();
                    })
                .Item("output_row_count").Value(partitioningResult.RowCount);

            FinishTabletStoresUpdateTransaction(tablet, slot, std::move(actionRequest), std::move(transaction), Logger);

            for (const auto& store : stores) {
                storeManager->EndStoreCompaction(store);
            }

            tabletSnapshot->TabletRuntimeData->Errors[ETabletBackgroundActivity::Partitioning].Store(TError());
        } catch (const std::exception& ex) {
            auto error = TError(ex)
                << TErrorAttribute("tablet_id", tabletSnapshot->TabletId)
                << TErrorAttribute("background_activity", ETabletBackgroundActivity::Partitioning);

            tabletSnapshot->TabletRuntimeData->Errors[ETabletBackgroundActivity::Partitioning].Store(error);
            YT_LOG_ERROR(error, "Error partitioning Eden, backing off");

            structuredLogger->LogEvent("backoff_partitioning")
                .Item("partition_id").Value(eden->GetId());

            for (const auto& store : stores) {
                storeManager->BackoffStoreCompaction(store);
            }
            failed = true;
        }

        for (const auto& writer : writers) {
            writerProfiler->Update(writer);
        }

        readerProfiler->Update(reader, blockReadOptions.ChunkReaderStatistics);
        writerProfiler->Profile(tabletSnapshot, EChunkWriteProfilingMethod::Partitioning, failed);
        readerProfiler->Profile(tabletSnapshot, EChunkReadProfilingMethod::Partitioning, failed);

        eden->CheckedSetState(EPartitionState::Partitioning, EPartitionState::Normal);
    }

    struct TEdenPartitioningResult
    {
        struct TPartitionWriter
        {
            IVersionedMultiChunkWriterPtr Writer;
            int PartitionIndex;
        };

        std::vector<TPartitionWriter> Writers;
        i64 RowCount;
    };

    TEdenPartitioningResult DoPartitionEden(
        const IVersionedReaderPtr& reader,
        const TTabletSnapshotPtr& tabletSnapshot,
        const ITransactionPtr& transaction,
        const std::vector<TLegacyOwningKey>& pivotKeys,
        const TLegacyOwningKey& nextTabletPivotKey,
        NLogging::TLogger Logger)
    {
        auto writerConfig = CloneYsonSerializable(tabletSnapshot->WriterConfig);
        writerConfig->MinUploadReplicationFactor = writerConfig->UploadReplicationFactor;
        writerConfig->WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::SystemTabletPartitioning);
        auto writerOptions = CloneYsonSerializable(tabletSnapshot->WriterOptions);
        writerOptions->ValidateResourceUsageIncrease = false;

        std::vector<TVersionedRow> writeRows;
        writeRows.reserve(MaxRowsPerWrite);

        int currentPartitionIndex = 0;
        TLegacyOwningKey currentPivotKey;
        TLegacyOwningKey nextPivotKey;

        int currentPartitionRowCount = 0;
        int readRowCount = 0;
        int writeRowCount = 0;
        IVersionedMultiChunkWriterPtr currentWriter;

        auto asyncBlockCache = CreateRemoteInMemoryBlockCache(
            Bootstrap_->GetMasterClient(),
            Bootstrap_->GetClusterNodeMasterConnector()->GetLocalDescriptor(),
            Bootstrap_->GetRpcServer(),
            Bootstrap_
                ->GetMasterClient()
                ->GetNativeConnection()
                ->GetCellDirectory()
                ->GetDescriptorOrThrow(tabletSnapshot->CellId),
            tabletSnapshot->Config->InMemoryMode,
            Bootstrap_->GetInMemoryManager()->GetConfig());

        auto blockCache = WaitFor(asyncBlockCache)
            .ValueOrThrow();

        TMemoryZoneGuard memoryZoneGuard(tabletSnapshot->Config->InMemoryMode == EInMemoryMode::None
            ? EMemoryZone::Normal
            : EMemoryZone::Undumpable);

        auto throttler = CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
            Bootstrap_->GetTabletNodeOutThrottler(EWorkloadCategory::SystemTabletPartitioning),
            tabletSnapshot->PartitioningThrottler});

        auto ensurePartitionStarted = [&] () {
            if (currentWriter)
                return;

            YT_LOG_INFO("Started writing partition (PartitionIndex: %v, Keys: %v .. %v)",
                currentPartitionIndex,
                currentPivotKey,
                nextPivotKey);

            currentWriter = CreateVersionedMultiChunkWriter(
                writerConfig,
                writerOptions,
                tabletSnapshot->PhysicalSchema,
                Bootstrap_->GetMasterClient(),
                CellTagFromId(tabletSnapshot->TabletId),
                transaction->GetId(),
                NullChunkListId,
                throttler,
                blockCache);
        };

        auto flushOutputRows = [&] () {
            if (writeRows.empty())
                return;

            writeRowCount += writeRows.size();

            ensurePartitionStarted();
            if (!currentWriter->Write(writeRows)) {
                WaitFor(currentWriter->GetReadyEvent())
                    .ThrowOnError();
            }

            writeRows.clear();
        };

        auto writeOutputRow = [&] (TVersionedRow row) {
            if (writeRows.size() == writeRows.capacity()) {
                flushOutputRows();
            }
            writeRows.push_back(row);
            ++currentPartitionRowCount;
        };

        std::vector<TFuture<void>> asyncCloseResults;
        std::vector<TEdenPartitioningResult::TPartitionWriter> releasedWriters;

        auto flushPartition = [&] (int partitionIndex) {
            flushOutputRows();

            if (currentWriter) {
                YT_LOG_INFO("Finished writing partition (PartitionIndex: %v, RowCount: %v)",
                    currentPartitionIndex,
                    currentPartitionRowCount);

                asyncCloseResults.push_back(currentWriter->Close());
                releasedWriters.push_back({std::move(currentWriter), partitionIndex});
                currentWriter.Reset();
            }

            currentPartitionRowCount = 0;
            ++currentPartitionIndex;
        };

        IVersionedRowBatchPtr rowBatch;
        TRowBatchReadOptions options{
            .MaxRowsPerRead = MaxRowsPerRead
        };
        int currentRowIndex = 0;

        auto peekInputRow = [&] () -> TVersionedRow {
            if (!rowBatch || currentRowIndex == rowBatch->GetRowCount()) {
                // readRows will be invalidated, must flush writeRows.
                flushOutputRows();
                currentRowIndex = 0;
                while (true) {
                    rowBatch = reader->Read(options);
                    if (!rowBatch) {
                        return TVersionedRow();
                    }
                    readRowCount += rowBatch->GetRowCount();
                    if (!rowBatch->IsEmpty()) {
                        break;
                    }
                    WaitFor(reader->GetReadyEvent())
                        .ThrowOnError();
                }
            }
            return rowBatch->MaterializeRows()[currentRowIndex];
        };

        auto skipInputRow = [&] () {
            ++currentRowIndex;
        };

        WaitFor(reader->Open())
            .ThrowOnError();

        for (auto it = pivotKeys.begin(); it != pivotKeys.end(); ++it) {
            currentPivotKey = *it;
            nextPivotKey = it == pivotKeys.end() - 1 ? nextTabletPivotKey : *(it + 1);

            while (true) {
                auto row = peekInputRow();
                if (!row) {
                    break;
                }

                // NB: pivot keys can be of arbitrary schema and length.
                YT_VERIFY(CompareRows(currentPivotKey.Begin(), currentPivotKey.End(), row.BeginKeys(), row.EndKeys()) <= 0);

                if (CompareRows(nextPivotKey.Begin(), nextPivotKey.End(), row.BeginKeys(), row.EndKeys()) <= 0) {
                    break;
                }

                skipInputRow();
                writeOutputRow(row);
            }

            flushPartition(it - pivotKeys.begin());
        }

        YT_VERIFY(readRowCount == writeRowCount);

        WaitFor(AllSucceeded(asyncCloseResults))
            .ThrowOnError();

        std::vector<TChunkInfo> chunkInfos;
        for (const auto& partitionWriterInfo : releasedWriters) {
            const auto& writer = partitionWriterInfo.Writer;
            for (const auto& chunkSpec : writer->GetWrittenChunksFullMeta()) {
                chunkInfos.emplace_back(
                    FromProto<TChunkId>(chunkSpec.chunk_id()),
                    chunkSpec.chunk_meta(),
                    tabletSnapshot->TabletId,
                    tabletSnapshot->MountRevision);
            }
        }

        WaitFor(blockCache->Finish(chunkInfos))
            .ThrowOnError();

        return {releasedWriters, readRowCount};
    }

    void DiscardPartitionStores(
        TPartition* partition,
        const TTabletSnapshotPtr& tabletSnapshot,
        const TTabletSlotPtr& slot,
        const std::vector<TSortedChunkStorePtr>& stores,
        NLogging::TLogger Logger)
    {
        YT_LOG_DEBUG("Discarding expired partition stores (PartitionId: %v)",
            partition->GetId());

        auto* tablet = partition->GetTablet();
        const auto& storeManager = tablet->GetStoreManager();
        const auto& structuredLogger = tablet->GetStructuredLogger();

        partition->CheckedSetState(EPartitionState::Normal, EPartitionState::Compacting);

        try {
            for (const auto& store : stores) {
                storeManager->BeginStoreCompaction(store);
            }

            auto transaction = StartCompactionTransaction(tabletSnapshot, Logger);
            Logger.AddTag("TransactionId: %v", transaction->GetId());

            auto retainedTimestamp = NullTimestamp;
            for (const auto& store : stores) {
                retainedTimestamp = std::max(retainedTimestamp, store->GetMaxTimestamp());
            }
            ++retainedTimestamp;

            NTabletServer::NProto::TReqUpdateTabletStores actionRequest;
            actionRequest.set_retained_timestamp(retainedTimestamp);
            actionRequest.set_update_reason(ToProto<int>(ETabletStoresUpdateReason::Compaction));

            TStoreIdList storeIdsToRemove;
            for (const auto& store : stores) {
                auto* descriptor = actionRequest.add_stores_to_remove();
                auto storeId = store->GetId();
                ToProto(descriptor->mutable_store_id(), storeId);
                storeIdsToRemove.push_back(storeId);
            }

            YT_LOG_INFO("Partition stores discarded by TTL (UnmergedRowCount: %v, CompressedDataSize: %v, StoreIdsToRemove: %v)",
                partition->GetUnmergedRowCount(),
                partition->GetCompressedDataSize(),
                storeIdsToRemove);

            structuredLogger->LogEvent("discard_stores")
                .Item("partition_id").Value(partition->GetId());

            FinishTabletStoresUpdateTransaction(tablet, slot, std::move(actionRequest), std::move(transaction), Logger);

            for (const auto& store : stores) {
                storeManager->EndStoreCompaction(store);
            }

            tabletSnapshot->TabletRuntimeData->Errors[ETabletBackgroundActivity::Compaction].Store(TError());
        } catch (const std::exception& ex) {
            auto error = TError(ex)
                << TErrorAttribute("tablet_id", tabletSnapshot->TabletId)
                << TErrorAttribute("background_activity", ETabletBackgroundActivity::Compaction);

            tabletSnapshot->TabletRuntimeData->Errors[ETabletBackgroundActivity::Compaction].Store(error);
            YT_LOG_ERROR(error, "Error discarding expired partition stores, backing off");

            structuredLogger->LogEvent("backoff_discard_stores")
                .Item("partition_id").Value(partition->GetId());

            for (const auto& store : stores) {
                storeManager->BackoffStoreCompaction(store);
            }
        }

        partition->CheckedSetState(EPartitionState::Compacting, EPartitionState::Normal);
    }

    void CompactPartition(TTask* task)
    {
        TClientBlockReadOptions blockReadOptions;
        blockReadOptions.WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::SystemTabletCompaction),
        blockReadOptions.ChunkReaderStatistics = New<TChunkReaderStatistics>();
        blockReadOptions.ReadSessionId = TReadSessionId::Create();

        NLogging::TLogger Logger(TabletNodeLogger);
        Logger.AddTag("%v, ReadSessionId: %v",
            task->TabletLoggingTag,
            blockReadOptions.ReadSessionId);

        auto doneGuard = Finally([&] {
            ScheduleMoreCompactions();
        });

        const auto& slot = task->Slot;
        const auto& tabletManager = slot->GetTabletManager();
        auto* tablet = tabletManager->FindTablet(task->TabletId);
        if (!tablet) {
            YT_LOG_DEBUG("Tablet is missing, aborting compaction");
            return;
        }

        const auto& snapshotStore = Bootstrap_->GetTabletSnapshotStore();
        auto tabletSnapshot = snapshotStore->FindTabletSnapshot(task->TabletId, task->MountRevision);
        if (!tabletSnapshot) {
            YT_LOG_DEBUG("Tablet snapshot is missing, aborting compaction");
            return;
        }

        const auto& storeManager = tablet->GetStoreManager();
        const auto& structuredLogger = tablet->GetStructuredLogger();

        auto logFailure = [&] (TStringBuf message) {
            return structuredLogger->LogEvent("abort_compaction")
                .Item("reason").Value(message)
                .Item("partition_id").Value(task->PartitionId);
        };

        auto* partition = tablet->GetEden()->GetId() == task->PartitionId
            ? tablet->GetEden()
            : tablet->FindPartition(task->PartitionId);
        if (!partition) {
            YT_LOG_DEBUG("Partition is missing, aborting compaction");
            logFailure("partition_missing");
            return;
        }

        if (partition->GetState() != EPartitionState::Normal) {
            YT_LOG_DEBUG("Partition is in improper state, aborting compaction (PartitionState: %v)", partition->GetState());
            logFailure("improper_state")
                .Item("state").Value(partition->GetState());
            return;
        }

        std::vector<TSortedChunkStorePtr> stores;
        stores.reserve(task->StoreIds.size());
        for (const auto& storeId : task->StoreIds) {
            auto store = tablet->FindStore(storeId);
            if (!store || !partition->Stores().contains(store->AsSorted())) {
                YT_LOG_DEBUG("Partition store is missing, aborting compaction (StoreId: %v)", storeId);
                logFailure("store_missing")
                    .Item("store_id").Value(storeId);

                return;
            }
            auto typedStore = store->AsSortedChunk();
            YT_VERIFY(typedStore);
            if (typedStore->GetCompactionState() != EStoreCompactionState::None) {
                YT_LOG_DEBUG("Partition store is in improper state, aborting compaction (StoreId: %v, CompactionState: %v)",
                    storeId,
                    typedStore->GetCompactionState());
                logFailure("improper_store_state")
                    .Item("store_id").Value(storeId)
                    .Item("store_compaction_state").Value(typedStore->GetCompactionState());
                return;
            }
            stores.push_back(std::move(typedStore));
        }

        if (task->DiscardStores) {
            DiscardPartitionStores(partition, tabletSnapshot, slot, stores, Logger);
            return;
        }

        Logger.AddTag("Eden: %v, PartitionRange: %v .. %v, PartitionId: %v",
            partition->IsEden(),
            partition->GetPivotKey(),
            partition->GetNextPivotKey(),
            partition->GetId());

        partition->CheckedSetState(EPartitionState::Normal, EPartitionState::Compacting);

        IVersionedReaderPtr reader;
        IVersionedMultiChunkWriterPtr writer;

        auto writerProfiler = New<TWriterProfiler>();
        auto readerProfiler = New<TReaderProfiler>();

        bool failed = false;

        try {
            i64 inputDataSize = 0;
            i64 inputRowCount = 0;
            for (const auto& store : stores) {
                inputDataSize += store->GetCompressedDataSize();
                inputRowCount += store->GetRowCount();
                storeManager->BeginStoreCompaction(store);
            }

            auto timestampProvider = Bootstrap_
                ->GetMasterClient()
                ->GetNativeConnection()
                ->GetTimestampProvider();
            auto currentTimestamp = WaitFor(timestampProvider->GenerateTimestamps())
                .ValueOrThrow();

            auto beginInstant = TInstant::Now();
            partition->SetCompactionTime(beginInstant);

            auto retainedTimestamp = std::min(
                InstantToTimestamp(TimestampToInstant(currentTimestamp).second - tablet->GetConfig()->MinDataTtl).second,
                currentTimestamp
            );

            auto majorTimestamp = std::min(ComputeMajorTimestamp(partition, stores), retainedTimestamp);

            structuredLogger->LogEvent("start_compaction")
                .Item("partition_id").Value(partition->GetId())
                .Item("store_ids").Value(task->StoreIds)
                .Item("current_timestamp").Value(currentTimestamp)
                // NB: deducible.
                .Item("major_timestamp").Value(majorTimestamp)
                .Item("retained_timestamp").Value(retainedTimestamp);

            auto bandwidthThrottler = Bootstrap_->GetTabletNodeInThrottler(EWorkloadCategory::SystemTabletCompaction);

            YT_LOG_INFO("Partition compaction started (Slack: %v, FutureEffect: %v, Effect: %v, "
                "RowCount: %v, CompressedDataSize: %v, ChunkCount: %v, "
                "CurrentTimestamp: %llx, MajorTimestamp: %llx, RetainedTimestamp: %llx, RetentionConfig: %v)",
                task->Slack,
                task->FutureEffect,
                task->Effect,
                inputRowCount,
                inputDataSize,
                stores.size(),
                currentTimestamp,
                majorTimestamp,
                retainedTimestamp,
                ConvertTo<TRetentionConfigPtr>(tabletSnapshot->Config));

            reader = CreateVersionedTabletReader(
                tabletSnapshot,
                std::vector<ISortedStorePtr>(stores.begin(), stores.end()),
                tablet->GetPivotKey(),
                tablet->GetNextPivotKey(),
                currentTimestamp,
                majorTimestamp,
                blockReadOptions,
                stores.size(),
                ETabletDistributedThrottlerKind::CompactionRead,
                std::move(bandwidthThrottler));

            auto transaction = StartCompactionTransaction(tabletSnapshot, Logger);
            Logger.AddTag("TransactionId: %v", transaction->GetId());

            auto asyncResult =
                BIND(
                    &TStoreCompactor::DoCompactPartition,
                    MakeStrong(this),
                    reader,
                    tabletSnapshot,
                    transaction,
                    partition->IsEden(),
                    Logger)
                .AsyncVia(ThreadPool_->GetInvoker())
                .Run();

            i64 outputRowCount;
            i64 outputDataSize;
            std::tie(writer, outputRowCount, outputDataSize) = WaitFor(asyncResult)
                .ValueOrThrow();

            auto endInstant = TInstant::Now();

            // We can release semaphore, because we are no longer actively using resources.
            task->SemaphoreGuard.Release();
            doneGuard.Release();

            NTabletServer::NProto::TReqUpdateTabletStores actionRequest;
            actionRequest.set_retained_timestamp(retainedTimestamp);
            actionRequest.set_update_reason(ToProto<int>(ETabletStoresUpdateReason::Compaction));

            TStoreIdList storeIdsToRemove;
            for (const auto& store : stores) {
                auto* descriptor = actionRequest.add_stores_to_remove();
                auto storeId = store->GetId();
                ToProto(descriptor->mutable_store_id(), storeId);
                storeIdsToRemove.push_back(storeId);
            }

            // TODO(sandello): Move specs?
            TStoreIdList storeIdsToAdd;
            for (const auto& chunkSpec : writer->GetWrittenChunksMasterMeta()) {
                auto* descriptor = actionRequest.add_stores_to_add();
                descriptor->set_store_type(static_cast<int>(EStoreType::SortedChunk));
                descriptor->mutable_store_id()->CopyFrom(chunkSpec.chunk_id());
                descriptor->mutable_chunk_meta()->CopyFrom(chunkSpec.chunk_meta());
                storeIdsToAdd.push_back(FromProto<TStoreId>(chunkSpec.chunk_id()));
            }

            tabletSnapshot->PerformanceCounters->CompactionDataWeightCount += writer->GetDataStatistics().data_weight();

            YT_LOG_INFO("Partition compaction completed (RowCount: %v, CompressedDataSize: %v, StoreIdsToAdd: %v, StoreIdsToRemove: %v, WallTime: %v)",
                outputRowCount,
                outputDataSize,
                storeIdsToAdd,
                storeIdsToRemove,
                endInstant - beginInstant);

            structuredLogger->LogEvent("end_compaction")
                .Item("partition_id").Value(partition->GetId())
                .Item("store_ids_to_add").List(storeIdsToAdd)
                .Item("output_row_count").Value(outputRowCount)
                .Item("output_data_size").Value(outputDataSize);

            FinishTabletStoresUpdateTransaction(tablet, slot, std::move(actionRequest), std::move(transaction), Logger);

            for (const auto& store : stores) {
                storeManager->EndStoreCompaction(store);
            }

            tabletSnapshot->TabletRuntimeData->Errors[ETabletBackgroundActivity::Compaction].Store(TError());
        } catch (const std::exception& ex) {
            auto error = TError(ex)
                << TErrorAttribute("tablet_id", tabletSnapshot->TabletId)
                << TErrorAttribute("background_activity", ETabletBackgroundActivity::Compaction);

            tabletSnapshot->TabletRuntimeData->Errors[ETabletBackgroundActivity::Compaction].Store(error);
            YT_LOG_ERROR(error, "Error compacting partition, backing off");

            structuredLogger->LogEvent("backoff_compaction")
                .Item("partition_id").Value(partition->GetId());

            for (const auto& store : stores) {
                storeManager->BackoffStoreCompaction(store);
            }
            failed = true;
        }

        writerProfiler->Update(writer);
        readerProfiler->Update(reader, blockReadOptions.ChunkReaderStatistics);

        writerProfiler->Profile(tabletSnapshot, EChunkWriteProfilingMethod::Compaction, failed);
        readerProfiler->Profile(tabletSnapshot, EChunkReadProfilingMethod::Compaction, failed);

        partition->CheckedSetState(EPartitionState::Compacting, EPartitionState::Normal);
    }

    std::tuple<IVersionedMultiChunkWriterPtr, i64, i64> DoCompactPartition(
        const IVersionedReaderPtr& reader,
        const TTabletSnapshotPtr& tabletSnapshot,
        const ITransactionPtr& transaction,
        bool isEden,
        NLogging::TLogger Logger)
    {
        auto writerConfig = CloneYsonSerializable(tabletSnapshot->WriterConfig);
        writerConfig->MinUploadReplicationFactor = writerConfig->UploadReplicationFactor;
        writerConfig->WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::SystemTabletCompaction);
        auto writerOptions = CloneYsonSerializable(tabletSnapshot->WriterOptions);
        writerOptions->ChunksEden = isEden;
        writerOptions->ValidateResourceUsageIncrease = false;

        auto asyncBlockCache = CreateRemoteInMemoryBlockCache(
            Bootstrap_->GetMasterClient(),
            Bootstrap_->GetClusterNodeMasterConnector()->GetLocalDescriptor(),
            Bootstrap_->GetRpcServer(),
            Bootstrap_
                ->GetMasterClient()
                ->GetNativeConnection()
                ->GetCellDirectory()
                ->GetDescriptorOrThrow(tabletSnapshot->CellId),
            tabletSnapshot->Config->InMemoryMode,
            Bootstrap_->GetInMemoryManager()->GetConfig());

        auto blockCache = WaitFor(asyncBlockCache)
            .ValueOrThrow();

        auto throttler = CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
            Bootstrap_->GetTabletNodeOutThrottler(EWorkloadCategory::SystemTabletCompaction),
            tabletSnapshot->CompactionThrottler});

        TMemoryZoneGuard memoryZoneGuard(tabletSnapshot->Config->InMemoryMode == EInMemoryMode::None
            ? NYTAlloc::EMemoryZone::Normal
            : NYTAlloc::EMemoryZone::Undumpable);

        auto writer = CreateVersionedMultiChunkWriter(
            writerConfig,
            writerOptions,
            tabletSnapshot->PhysicalSchema,
            Bootstrap_->GetMasterClient(),
            CellTagFromId(tabletSnapshot->TabletId),
            transaction->GetId(),
            NullChunkListId,
            throttler,
            blockCache);

        WaitFor(reader->Open())
            .ThrowOnError();

        TRowBatchReadOptions options{
            .MaxRowsPerRead = MaxRowsPerRead
        };

        i64 readRowCount = 0;
        i64 writeRowCount = 0;

        while (auto batch = reader->Read(options)) {
            readRowCount += batch->GetRowCount();

            if (batch->IsEmpty()) {
                WaitFor(reader->GetReadyEvent())
                    .ThrowOnError();
                continue;
            }

            writeRowCount += batch->GetRowCount();
            if (!writer->Write(batch->MaterializeRows())) {
                WaitFor(writer->GetReadyEvent())
                    .ThrowOnError();
            }
        }

        WaitFor(writer->Close())
            .ThrowOnError();

        std::vector<TChunkInfo> chunkInfos;
        for (const auto& chunkSpec : writer->GetWrittenChunksFullMeta()) {
            chunkInfos.emplace_back(
                FromProto<TChunkId>(chunkSpec.chunk_id()),
                chunkSpec.chunk_meta(),
                tabletSnapshot->TabletId,
                tabletSnapshot->MountRevision);
        }

        WaitFor(blockCache->Finish(chunkInfos))
            .ThrowOnError();

        YT_VERIFY(readRowCount == writeRowCount);

        i64 dataSize = writer->GetDataStatistics().compressed_data_size();
        return std::make_tuple(writer, readRowCount, dataSize);
    }

    static bool IsCompactionForced(const TSortedChunkStorePtr& store)
    {
        std::optional<NHydra::TRevision> forcedCompactionRevision;

        const auto& config = store->GetTablet()->GetConfig();
        if (config->ForcedCompactionRevision) {
            forcedCompactionRevision = config->ForcedCompactionRevision;
        }
        if (config->ForcedChunkViewCompactionRevision &&
            TypeFromId(store->GetId()) == EObjectType::ChunkView)
        {
            // NB: std::nullopt is less than any nonempty optional.
            forcedCompactionRevision = std::max(
                config->ForcedChunkViewCompactionRevision,
                forcedCompactionRevision);
        }

        if (!forcedCompactionRevision) {
            return false;
        }

        auto revision = CounterFromId(store->GetId());
        if (revision > *forcedCompactionRevision) {
            return false;
        }

        return true;
    }

    static bool IsPeriodicCompactionNeeded(const TSortedChunkStorePtr& store)
    {
        const auto& config = store->GetTablet()->GetConfig();
        if (!config->AutoCompactionPeriod) {
            return false;
        }

        auto splayRatio = config->AutoCompactionPeriodSplayRatio *
            store->GetId().Parts32[0] / std::numeric_limits<ui32>::max();
        auto effectivePeriod = TDuration::FromValue(
            static_cast<TDuration::TValue>(config->AutoCompactionPeriod->GetValue() * (1 + splayRatio)));

        if (TInstant::Now() < store->GetCreationTime() + effectivePeriod) {
            return false;
        }

        return true;
    }

    static bool IsStoreOutOfTabletRange(const TSortedChunkStorePtr& store, const TTablet* tablet)
    {
        if (store->GetMinKey() < tablet->GetPivotKey()) {
            return true;
        }

        if (store->GetUpperBoundKey() > tablet->GetNextPivotKey()) {
            return true;
        }

        return false;
    }

    static int GetOverlappingStoreLimit(const TTableMountConfigPtr& config)
    {
        return std::min(
            config->MaxOverlappingStoreCount,
            config->CriticalOverlappingStoreCount.value_or(config->MaxOverlappingStoreCount));
    }
};

DEFINE_REFCOUNTED_TYPE(TStoreCompactor)

IStoreCompactorPtr CreateStoreCompactor(NClusterNode::TBootstrap* bootstrap)
{
    return New<TStoreCompactor>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
