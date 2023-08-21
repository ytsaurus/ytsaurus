#include "partition_balancer.h"
#include "bootstrap.h"
#include "private.h"
#include "sorted_chunk_store.h"
#include "partition.h"
#include "slot_manager.h"
#include "store.h"
#include "tablet.h"
#include "tablet_manager.h"
#include "tablet_slot.h"
#include "structured_logger.h"
#include "yt/yt/library/profiling/sensor.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/lib/hydra/distributed_hydra_manager.h>
#include <yt/yt/server/lib/hydra_common/mutation.h>

#include <yt/yt/server/lib/tablet_node/proto/tablet_manager.pb.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/server/lib/lsm/tablet.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/chunk_replica_cache.h>
#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/fetcher.h>
#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/throttler_manager.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/ytlib/table_client/samples_fetcher.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/ytlib/tablet_client/config.h>
#include <yt/yt/client/table_client/wire_protocol.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/actions/cancelable_context.h>

#include <yt/yt/core/concurrency/async_semaphore.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/tracing/trace_context.h>

namespace NYT::NTabletNode {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NHydra;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTabletNode::NProto;
using namespace NProfiling;
using namespace NTracing;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TPartitionBalancer
    : public IPartitionBalancer
{
public:
    explicit TPartitionBalancer(IBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
        , Config_(Bootstrap_->GetConfig()->TabletNode->PartitionBalancer)
        , Semaphore_(New<TAsyncSemaphore>(Config_->MaxConcurrentSamplings))
        , ThrottlerManager_(New<TThrottlerManager>(
            Config_->ChunkLocationThrottler,
            Logger))
    { }

private:
    IBootstrap* const Bootstrap_;
    const TPartitionBalancerConfigPtr Config_;

    const TAsyncSemaphorePtr Semaphore_;
    const TThrottlerManagerPtr ThrottlerManager_;

    const TProfiler Profiler_ = TabletNodeProfiler.WithPrefix("/partition_balancer");
    TEventTimer ScanTime_ = Profiler_.Timer("/scan_time");

    void ProcessLsmActionBatch(
        const ITabletSlotPtr& slot,
        const NLsm::TLsmActionBatch& batch) override
    {
        YT_LOG_DEBUG("Partition balancer started processing action batch (CellId: %v)",
            slot->GetCellId());

        TEventTimerGuard guard(ScanTime_);

        const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
        auto dynamicConfig = dynamicConfigManager->GetConfig()->TabletNode->PartitionBalancer;
        if (!dynamicConfig->Enable) {
            return;
        }

        if (slot->GetAutomatonState() != EPeerState::Leading) {
            return;
        }

        for (const auto& request : batch.Samplings) {
            ProcessSampleRequest(slot, request);
        }

        for (const auto& request : batch.Splits) {
            ProcessSplitRequest(slot, request);
        }

        for (const auto& request : batch.Merges) {
            ProcessMergeRequest(slot, request);
        }

        YT_LOG_DEBUG("Partition balancer finished processing action batch (CellId: %v)",
            slot->GetCellId());
    }

    static bool CheckPartitionIdMatch(
        const TTablet* tablet,
        TPartitionId partitionId,
        int partitionIndex)
    {
        const auto& partitions = tablet->PartitionList();
        return partitionIndex < ssize(partitions) &&
            partitions[partitionIndex]->GetId() == partitionId;
    }

    void ProcessSampleRequest(const ITabletSlotPtr& slot, const NLsm::TSamplePartitionRequest& request)
    {
        const auto& tabletManager = slot->GetTabletManager();
        auto* tablet = tabletManager->FindTablet(request.Tablet->GetId());
        if (!tablet) {
            return;
        }

        if (!CheckPartitionIdMatch(tablet, request.PartitionId, request.PartitionIndex)) {
            return;
        }

        RunSample(slot, tablet->PartitionList()[request.PartitionIndex].get());
    }

    void ProcessSplitRequest(const ITabletSlotPtr& slot, const NLsm::TSplitPartitionRequest& request)
    {
        const auto& tabletManager = slot->GetTabletManager();
        auto* tablet = tabletManager->FindTablet(request.Tablet->GetId());
        if (!tablet) {
            return;
        }

        if (!CheckPartitionIdMatch(tablet, request.PartitionId, request.PartitionIndex)) {
            return;
        }

        auto* partition = tablet->PartitionList()[request.PartitionIndex].get();
        if (partition->GetState() != EPartitionState::Normal) {
            return;
        }

        for (const auto& store : partition->Stores()) {
            if (store->GetStoreState() != EStoreState::Persistent) {
                return;
            }
        }

        auto Logger = BuildLogger(slot, partition);

        if (request.Immediate) {
            if (!ValidateImmediateSplit(slot, partition)) {
                partition->PivotKeysForImmediateSplit().clear();
                return;
            }

            tablet->GetTableProfiler()->GetLsmCounters()->ProfilePartitionSplit();
            partition->CheckedSetState(EPartitionState::Normal, EPartitionState::Splitting);
            DoRunImmediateSplit(slot, partition, Logger);
            return;
        }

        tablet->GetStructuredLogger()->LogEvent("schedule_partition_split")
            .Item("partition_id").Value(partition->GetId())
            // NB: deducible.
            .Item("split_factor").Value(request.SplitFactor);

        tablet->GetTableProfiler()->GetLsmCounters()->ProfilePartitionSplit();
        partition->CheckedSetState(EPartitionState::Normal, EPartitionState::Splitting);
        auto future = BIND(
            &TPartitionBalancer::DoRunSplit,
            MakeStrong(this),
            slot,
            partition,
            request.SplitFactor,
            partition->GetTablet(),
            Logger)
            .AsyncVia(tablet->GetEpochAutomatonInvoker())
            .Run();
        if (const auto& context = partition->GetTablet()->GetCancelableContext()) {
            context->PropagateTo(future);
        }
    }

    void ProcessMergeRequest(const ITabletSlotPtr& slot, const NLsm::TMergePartitionsRequest& request)
    {
        const auto& tabletManager = slot->GetTabletManager();
        auto* tablet = tabletManager->FindTablet(request.Tablet->GetId());
        if (!tablet) {
            return;
        }

        int partitionIndex = request.FirstPartitionIndex;
        for (auto partitionId : request.PartitionIds) {
            if (!CheckPartitionIdMatch(tablet, partitionId, partitionIndex)) {
                return;
            }
            ++partitionIndex;
        }

        RunMerge(
            slot,
            tablet->PartitionList()[request.FirstPartitionIndex].get(),
            request.FirstPartitionIndex,
            request.FirstPartitionIndex + request.PartitionIds.size() - 1);
    }

    bool ValidateImmediateSplit(ITabletSlotPtr slot, TPartition* partition) const
    {
        auto Logger = BuildLogger(slot, partition);

        const auto* tablet = partition->GetTablet();
        const auto& mountConfig = tablet->GetSettings().MountConfig;

        const auto& pivotKeys = partition->PivotKeysForImmediateSplit();
        if (pivotKeys.empty()) {
            return false;
        }

        if (pivotKeys[0] != partition->GetPivotKey()) {
            YT_LOG_DEBUG_IF(mountConfig->EnableLsmVerboseLogging,
                "Will not perform immediate partition split: first proposed pivot key "
                "does not match partition pivot key (PartitionPivotKey: %v, ProposedPivotKey: %v)",
                partition->GetPivotKey(),
                pivotKeys[0]);

            partition->PivotKeysForImmediateSplit().clear();
            return false;
        }

        for (int index = 1; index < ssize(pivotKeys); ++index) {
            if (pivotKeys[index] <= pivotKeys[index - 1]) {
                YT_LOG_DEBUG_IF(mountConfig->EnableLsmVerboseLogging,
                    "Will not perform immediate partition split: proposed pivots are not sorted");

                partition->PivotKeysForImmediateSplit().clear();
                return false;
            }
        }

        if (pivotKeys.back() >= partition->GetNextPivotKey()) {
            YT_LOG_DEBUG_IF(mountConfig->EnableLsmVerboseLogging,
                "Will not perform immediate partition split: last proposed pivot key "
                "is not less than partition next pivot key (NextPivotKey: %v, ProposedPivotKey: %v)",
                partition->GetNextPivotKey(),
                pivotKeys.back());

            partition->PivotKeysForImmediateSplit().clear();
            return false;
        }

        if (pivotKeys.size() <= 1) {
            YT_LOG_DEBUG_IF(mountConfig->EnableLsmVerboseLogging,
                "Will not perform immediate partition split: too few pivot keys");

            partition->PivotKeysForImmediateSplit().clear();
            return false;
        }

        return true;
    }

    void DoRunSplit(
        ITabletSlotPtr slot,
        TPartition* partition,
        int splitFactor,
        TTablet* tablet,
        NLogging::TLogger Logger)
    {
        TTraceContextGuard TTraceContextGuard(TTraceContext::NewRoot("PartitionBalancer"));

        YT_LOG_DEBUG("Splitting partition");

        YT_VERIFY(tablet == partition->GetTablet());
        const auto& hydraManager = slot->GetHydraManager();
        const auto& structuredLogger = tablet->GetStructuredLogger();

        YT_LOG_INFO("Partition is eligible for split (SplitFactor: %v)",
            splitFactor);

        try {
            auto rowBuffer = New<TRowBuffer>();
            auto samples = GetPartitionSamples(rowBuffer, slot, partition, Config_->MaxPartitioningSampleCount);
            int sampleCount = static_cast<int>(samples.size());
            int minSampleCount = std::max(Config_->MinPartitioningSampleCount, splitFactor);
            if (sampleCount < minSampleCount) {
                structuredLogger->LogEvent("abort_partition_split")
                    .Item("partition_id").Value(partition->GetId())
                    .Item("reason").Value("too_few_samples")
                    .Item("min_sample_count").Value(minSampleCount)
                    .Item("sample_count").Value(sampleCount);
                THROW_ERROR_EXCEPTION("Too few samples fetched: need %v, got %v",
                    minSampleCount,
                    sampleCount);
            }

            std::vector<TLegacyKey> pivotKeys;
            // Take the pivot of the partition.
            pivotKeys.push_back(partition->GetPivotKey());
            // And add |splitFactor - 1| more keys from samples.
            for (int i = 0; i < splitFactor - 1; ++i) {
                int j = (i + 1) * sampleCount / splitFactor - 1;
                auto key = samples[j];
                if (key > pivotKeys.back()) {
                    pivotKeys.push_back(key);
                }
            }

            if (pivotKeys.size() < 2) {
                structuredLogger->LogEvent("abort_partition_split")
                    .Item("partition_id").Value(partition->GetId())
                    .Item("reason").Value("no_valid_pivots");
                THROW_ERROR_EXCEPTION("No valid pivot keys can be obtained from samples");
            }

            structuredLogger->LogEvent("request_partition_split")
                .Item("partition_id").Value(partition->GetId())
                .Item("immediate").Value(false)
                .Item("pivot_keys").List(pivotKeys);

            TReqSplitPartition request;
            ToProto(request.mutable_tablet_id(), tablet->GetId());
            request.set_mount_revision(tablet->GetMountRevision());
            ToProto(request.mutable_partition_id(), partition->GetId());
            ToProto(request.mutable_pivot_keys(), pivotKeys);

            CreateMutation(hydraManager, request)
                ->CommitAndLog(Logger);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Partition splitting aborted");
            structuredLogger->LogEvent("backoff_partition_split")
                .Item("partition_id").Value(partition->GetId());
            partition->CheckedSetState(EPartitionState::Splitting, EPartitionState::Normal);
            partition->SetAllowedSplitTime(TInstant::Now() + Config_->SplitRetryDelay);
        }
    }

    void DoRunImmediateSplit(
        ITabletSlotPtr slot,
        TPartition* partition,
        NLogging::TLogger Logger)
    {
        YT_LOG_DEBUG("Splitting partition with provided pivot keys (SplitFactor: %v)",
            partition->PivotKeysForImmediateSplit().size());

        auto* tablet = partition->GetTablet();

        std::vector<TLegacyOwningKey> pivotKeys;
        pivotKeys.swap(partition->PivotKeysForImmediateSplit());

        tablet->GetStructuredLogger()->LogEvent("request_partition_split")
            .Item("partition_id").Value(partition->GetId())
            .Item("immediate").Value(true)
            .Item("pivot_keys").List(pivotKeys);

        const auto& hydraManager = slot->GetHydraManager();
        TReqSplitPartition request;
        ToProto(request.mutable_tablet_id(), tablet->GetId());
        request.set_mount_revision(tablet->GetMountRevision());
        ToProto(request.mutable_partition_id(), partition->GetId());
        ToProto(request.mutable_pivot_keys(), pivotKeys);

        CreateMutation(hydraManager, request)
            ->CommitAndLog(Logger);
    }

    bool RunMerge(
        ITabletSlotPtr slot,
        TPartition* partition,
        int firstPartitionIndex,
        int lastPartitionIndex)
    {
        auto* tablet = partition->GetTablet();

        const auto& mountConfig = tablet->GetSettings().MountConfig;
        for (int index = firstPartitionIndex; index <= lastPartitionIndex; ++index) {
            if (tablet->PartitionList()[index]->GetState() != EPartitionState::Normal) {
                YT_LOG_DEBUG_IF(mountConfig->EnableLsmVerboseLogging,
                    "Will not merge partitions due to improper partition state "
                    "(%v, InitialPartitionId: %v, PartitionId: %v, PartitionIndex: %v, PartitionState: %v)",
                    tablet->GetLoggingTag(),
                    partition->GetId(),
                    tablet->PartitionList()[index]->GetId(),
                    index,
                    tablet->PartitionList()[index]->GetState());
                return false;
            }
        }

        for (int index = firstPartitionIndex; index <= lastPartitionIndex; ++index) {
            tablet->PartitionList()[index]->CheckedSetState(EPartitionState::Normal, EPartitionState::Merging);
        }

        tablet->GetTableProfiler()->GetLsmCounters()->ProfilePartitionMerge();

        auto Logger = TabletNodeLogger;
        Logger.AddTag("%v, CellId: %v, PartitionIds: %v",
            partition->GetTablet()->GetLoggingTag(),
            slot->GetCellId(),
            MakeFormattableView(
                MakeRange(
                    tablet->PartitionList().data() + firstPartitionIndex,
                    tablet->PartitionList().data() + lastPartitionIndex + 1),
                TPartitionIdFormatter()));

        YT_LOG_INFO("Partitions are eligible for merge");

        tablet->GetStructuredLogger()->LogEvent("request_partitions_merge")
            .Item("initial_partition_id").Value(partition->GetId())
            .Item("first_partition_index").Value(firstPartitionIndex)
            .Item("last_partition_index").Value(lastPartitionIndex);

        const auto& hydraManager = slot->GetHydraManager();

        TReqMergePartitions request;
        ToProto(request.mutable_tablet_id(), tablet->GetId());
        request.set_mount_revision(tablet->GetMountRevision());
        ToProto(request.mutable_partition_id(), tablet->PartitionList()[firstPartitionIndex]->GetId());
        request.set_partition_count(lastPartitionIndex - firstPartitionIndex + 1);

        CreateMutation(hydraManager, request)
            ->CommitAndLog(Logger);
        return true;
    }


    bool RunSample(ITabletSlotPtr slot, TPartition* partition)
    {
        if (partition->GetState() != EPartitionState::Normal) {
            return false;
        }

        auto guard = TAsyncSemaphoreGuard::TryAcquire(Semaphore_);
        if (!guard) {
            return false;
        }

        partition->CheckedSetState(EPartitionState::Normal, EPartitionState::Sampling);

        auto Logger = BuildLogger(slot, partition);

        YT_LOG_DEBUG("Partition is scheduled for sampling");

        auto future = BIND(&TPartitionBalancer::DoRunSample, MakeStrong(this), Passed(std::move(guard)))
            .AsyncVia(partition->GetTablet()->GetEpochAutomatonInvoker())
            .Run(
                slot,
                partition,
                partition->GetTablet(),
                Logger);
        if (const auto& context = partition->GetTablet()->GetCancelableContext()) {
            context->PropagateTo(future);
        }
        return true;
    }

    void DoRunSample(
        TAsyncSemaphoreGuard /*guard*/,
        ITabletSlotPtr slot,
        TPartition* partition,
        TTablet* tablet,
        NLogging::TLogger Logger)
    {
        TTraceContextGuard TTraceContextGuard(TTraceContext::NewRoot("PartitionBalancer"));

        YT_LOG_DEBUG("Sampling partition");

        YT_VERIFY(tablet == partition->GetTablet());
        const auto& mountConfig = tablet->GetSettings().MountConfig;

        const auto& hydraManager = slot->GetHydraManager();

        try {
            auto compressedDataSize = partition->GetCompressedDataSize();
            if (compressedDataSize == 0) {
                THROW_ERROR_EXCEPTION("Empty partition");
            }

            auto uncompressedDataSize = partition->GetUncompressedDataSize();
            auto scaledSamples = static_cast<int>(
                mountConfig->SamplesPerPartition * std::max(compressedDataSize, uncompressedDataSize) / compressedDataSize);
            YT_LOG_INFO("Sampling partition (DesiredSampleCount: %v)", scaledSamples);

            auto rowBuffer = New<TRowBuffer>();
            auto samples = GetPartitionSamples(rowBuffer, slot, partition, scaledSamples);
            samples.erase(
                std::unique(samples.begin(), samples.end()),
                samples.end());

            auto writer = CreateWireProtocolWriter();
            writer->WriteUnversionedRowset(samples);

            TReqUpdatePartitionSampleKeys request;
            ToProto(request.mutable_tablet_id(), tablet->GetId());
            request.set_mount_revision(tablet->GetMountRevision());
            ToProto(request.mutable_partition_id(), partition->GetId());
            request.set_sample_keys(MergeRefsToString(writer->Finish()));

            CreateMutation(hydraManager, request)
                ->CommitAndLog(Logger);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Partition sampling aborted");
        }

        partition->CheckedSetState(EPartitionState::Sampling, EPartitionState::Normal);
        // NB: Update the timestamp even in case of failure to prevent
        // repeating unsuccessful samplings too rapidly.
        partition->SetSamplingTime(TInstant::Now());
    }


    std::vector<TLegacyKey> GetPartitionSamples(
        const TRowBufferPtr& rowBuffer,
        ITabletSlotPtr slot,
        TPartition* partition,
        int maxSampleCount)
    {
        YT_VERIFY(!partition->IsEden());

        if (maxSampleCount == 0) {
            return std::vector<TLegacyKey>();
        }

        auto Logger = BuildLogger(slot, partition);

        auto* tablet = partition->GetTablet();

        auto nodeDirectory = Bootstrap_->GetConnection()->GetNodeDirectory();

        auto chunkScraper = CreateFetcherChunkScraper(
            Config_->ChunkScraper,
            Bootstrap_->GetControlInvoker(),
            ThrottlerManager_,
            Bootstrap_->GetClient(),
            nodeDirectory,
            Logger);

        auto samplesFetcher = New<TSamplesFetcher>(
            Config_->SamplesFetcher,
            ESamplingPolicy::Partitioning,
            maxSampleCount,
            tablet->GetPhysicalSchema()->GetKeyColumns(),
            NTableClient::MaxSampleSize,
            nodeDirectory,
            GetCurrentInvoker(),
            rowBuffer,
            chunkScraper,
            Bootstrap_->GetClient(),
            Logger);

        const auto& chunkReplicaCache = Bootstrap_->GetConnection()->GetChunkReplicaCache();

        std::vector<TChunkId> chunkIds;
        std::vector<TFuture<TAllyReplicasInfo>> replicasFutures;
        {
            auto channel = Bootstrap_->GetClient()->GetMasterChannelOrThrow(
                NApi::EMasterChannelKind::Follower,
                CellTagFromId(tablet->GetId()));
            TChunkServiceProxy proxy(channel);

            THashMap<TChunkId, TSortedChunkStorePtr> storeMap;

            auto addStore = [&] (const ISortedStorePtr& store) {
                if (store->GetType() != EStoreType::SortedChunk) {
                    return;
                }

                if (store->GetUpperBoundKey() <= partition->GetPivotKey() ||
                    store->GetMinKey() >= partition->GetNextPivotKey())
                {
                    return;
                }

                auto sortedChunk = store->AsSortedChunk();
                auto chunkId = sortedChunk->GetChunkId();
                YT_VERIFY(chunkId);
                if (storeMap.emplace(chunkId, sortedChunk).second) {
                    chunkIds.push_back(chunkId);
                }
            };

            auto addStores = [&] (const THashSet<ISortedStorePtr>& stores) {
                for (const auto& store : stores) {
                    addStore(store);
                }
            };

            addStores(partition->Stores());
            addStores(tablet->GetEden()->Stores());

            if (chunkIds.empty()) {
                return std::vector<TLegacyKey>();
            }

            YT_LOG_INFO("Locating partition chunks (ChunkCount: %v)",
                chunkIds.size());

            replicasFutures = chunkReplicaCache->GetReplicas(chunkIds);

            auto replicasListOrError = WaitFor(AllSucceeded(replicasFutures));
            THROW_ERROR_EXCEPTION_IF_FAILED(replicasListOrError, "Error locating partition chunks");
            const auto& replicasList = replicasListOrError.Value();
            YT_VERIFY(replicasList.size() == chunkIds.size());

            YT_LOG_INFO("Partition chunks located");

            for (int index = 0; index < std::ssize(chunkIds); ++index) {
                auto chunkId = chunkIds[index];
                const auto& replicas = replicasList[index];

                const auto& store = GetOrCrash(storeMap, chunkId);

                NChunkClient::NProto::TChunkSpec chunkSpec;
                ToProto(chunkSpec.mutable_chunk_id(), chunkId);
                ToProto(chunkSpec.mutable_replicas(), replicas.Replicas);
                ToProto(chunkSpec.mutable_legacy_replicas(), TChunkReplicaWithMedium::ToChunkReplicas(replicas.Replicas));
                *chunkSpec.mutable_chunk_meta() = store->GetChunkMeta();
                ToProto(chunkSpec.mutable_lower_limit(), TLegacyReadLimit(partition->GetPivotKey()));
                ToProto(chunkSpec.mutable_upper_limit(), TLegacyReadLimit(partition->GetNextPivotKey()));
                chunkSpec.set_erasure_codec(ToProto<int>(store->GetErasureCodecId()));

                auto inputChunk = New<TInputChunk>(chunkSpec);
                samplesFetcher->AddChunk(std::move(inputChunk));
            }
        }

        try {
            WaitFor(samplesFetcher->Fetch())
                .ThrowOnError();
        } catch (const std::exception&) {
            for (int index = 0; index < std::ssize(chunkIds); ++index) {
                chunkReplicaCache->DiscardReplicas(
                    chunkIds[index],
                    replicasFutures[index]);
            }
            throw;
        }

        YT_LOG_DEBUG("Samples fetched");

        std::vector<TLegacyKey> samples;
        for (const auto& sample : samplesFetcher->GetSamples()) {
            YT_VERIFY(!sample.Incomplete);
            samples.push_back(sample.Key);
        }

        // NB(psushin): This filtering is typically redundant (except for the first pivot),
        // since fetcher already returns samples within given limits.
        samples.erase(
            std::remove_if(
                samples.begin(),
                samples.end(),
                [&] (TLegacyKey key) {
                    return key <= partition->GetPivotKey() || key >= partition->GetNextPivotKey();
                }),
            samples.end());

        std::sort(samples.begin(), samples.end());
        return samples;
    }


    static NLogging::TLogger BuildLogger(
        const ITabletSlotPtr& slot,
        TPartition* partition)
    {
        return TabletNodeLogger.WithTag("%v, CellId: %v, PartitionId: %v",
            partition->GetTablet()->GetLoggingTag(),
            slot->GetCellId(),
            partition->GetId());
    }
};

////////////////////////////////////////////////////////////////////////////////

IPartitionBalancerPtr CreatePartitionBalancer(IBootstrap* bootstrap)
{
    return New<TPartitionBalancer>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
