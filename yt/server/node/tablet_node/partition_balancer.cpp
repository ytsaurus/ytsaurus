#include "partition_balancer.h"
#include "private.h"
#include "sorted_chunk_store.h"
#include "partition.h"
#include "slot_manager.h"
#include "store.h"
#include "tablet.h"
#include "tablet_manager.h"
#include "tablet_slot.h"

#include <yt/server/node/cluster_node/bootstrap.h>

#include <yt/server/lib/hydra/hydra_manager.h>
#include <yt/server/lib/hydra/mutation.h>

#include <yt/server/lib/tablet_node/proto/tablet_manager.pb.h>

#include <yt/server/lib/tablet_node/config.h>

#include <yt/ytlib/api/native/client.h>

#include <yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/ytlib/chunk_client/fetcher.h>
#include <yt/ytlib/chunk_client/input_chunk.h>
#include <yt/ytlib/chunk_client/throttler_manager.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/ytlib/table_client/samples_fetcher.h>
#include <yt/client/table_client/unversioned_row.h>
#include <yt/client/table_client/row_buffer.h>

#include <yt/ytlib/tablet_client/config.h>
#include <yt/client/table_client/wire_protocol.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/concurrency/async_semaphore.h>
#include <yt/core/concurrency/scheduler.h>

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

namespace NYT::NTabletNode {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NHydra;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTabletNode::NProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TPartitionBalancer
    : public TRefCounted
{
public:
    TPartitionBalancer(
        TPartitionBalancerConfigPtr config,
        NClusterNode::TBootstrap* bootstrap)
        : Config_(config)
        , Bootstrap_(bootstrap)
        , Semaphore_(New<TAsyncSemaphore>(Config_->MaxConcurrentSamplings))
        , ThrottlerManager_(New<TThrottlerManager>(
            Config_->ChunkLocationThrottler,
            Logger))
        , Profiler("/tablet_node/partition_balancer")
        , PartitionSplitCounter_("/scheduled_splits")
        , PartitionMergeCounter_("/scheduled_merges")
    {
        auto slotManager = Bootstrap_->GetTabletSlotManager();
        slotManager->SubscribeScanSlot(BIND(&TPartitionBalancer::OnScanSlot, MakeStrong(this)));
    }

private:
    TPartitionBalancerConfigPtr Config_;
    NClusterNode::TBootstrap* Bootstrap_;
    TAsyncSemaphorePtr Semaphore_;
    TThrottlerManagerPtr ThrottlerManager_;

    const NProfiling::TProfiler Profiler;
    NProfiling::TMonotonicCounter PartitionSplitCounter_;
    NProfiling::TMonotonicCounter PartitionMergeCounter_;


    void OnScanSlot(TTabletSlotPtr slot)
    {
        const auto& tagIdList = slot->GetProfilingTagIds();
        PROFILE_TIMING("/scan_time", tagIdList) {
            OnScanSlotImpl(slot, tagIdList);
        }
    }

    void OnScanSlotImpl(TTabletSlotPtr slot, const NProfiling::TTagIdList& tagIdList)
    {
        if (slot->GetAutomatonState() != EPeerState::Leading) {
            return;
        }
        const auto& tabletManager = slot->GetTabletManager();
        for (const auto& pair : tabletManager->Tablets()) {
            auto* tablet = pair.second;
            ScanTablet(slot, tablet);
        }
    }

    void ScanTablet(TTabletSlotPtr slot, TTablet* tablet)
    {
        if (tablet->GetState() != ETabletState::Mounted) {
            return;
        }

        if (!tablet->IsPhysicallySorted()) {
            return;
        }

        for (const auto& partition : tablet->PartitionList()) {
            ScanPartitionToSample(slot, partition.get());
        }

        if (!tablet->GetConfig()->EnableCompactionAndPartitioning) {
            return;
        }

        int currentMaxOverlappingStoreCount = tablet->GetOverlappingStoreCount();
        int estimatedMaxOverlappingStoreCount = currentMaxOverlappingStoreCount;

        YT_LOG_DEBUG_IF(tablet->GetConfig()->EnableLsmVerboseLogging,
            "Partition balancer started tablet scan for splits (%v, CurrentMosc: %v)",
            tablet->GetLoggingId(),
            currentMaxOverlappingStoreCount);

        for (const auto& partition : tablet->PartitionList()) {
            ScanPartitionToSplit(slot, partition.get(), &estimatedMaxOverlappingStoreCount);
        }

        int maxAllowedOverlappingStoreCount = tablet->GetConfig()->MaxOverlappingStoreCount -
            (estimatedMaxOverlappingStoreCount - currentMaxOverlappingStoreCount);

        YT_LOG_DEBUG_IF(tablet->GetConfig()->EnableLsmVerboseLogging,
            "Partition balancer started tablet scan for merges (%v, "
            "EstimatedMosc: %v, MaxAllowedOsc: %v)",
            tablet->GetLoggingId(),
            estimatedMaxOverlappingStoreCount,
            maxAllowedOverlappingStoreCount);

        for (const auto& partition : tablet->PartitionList()) {
            ScanPartitionToMerge(slot, partition.get(), maxAllowedOverlappingStoreCount);
        }
    }

    void ScanPartitionToSplit(TTabletSlotPtr slot, TPartition* partition, int* estimatedMaxOverlappingStoreCount)
    {
        auto* tablet = partition->GetTablet();
        const auto& config = tablet->GetConfig();
        int partitionCount = tablet->PartitionList().size();
        i64 actualDataSize = partition->GetCompressedDataSize();
        int estimatedStoresDelta = partition->Stores().size();

        auto Logger = BuildLogger(slot, partition);

        if (tablet->GetConfig()->EnableLsmVerboseLogging) {
            YT_LOG_DEBUG(
                "Scanning partition to split (PartitionIndex: %v of %v, "
                "EstimatedMosc: %v, DataSize: %v, StoreCount: %v)",
                partition->GetIndex(),
                partitionCount,
                *estimatedMaxOverlappingStoreCount,
                actualDataSize,
                partition->Stores().size());
        }

        if (partition->GetState() != EPartitionState::Normal) {
            YT_LOG_DEBUG_IF(tablet->GetConfig()->EnableLsmVerboseLogging,
                "Aborting partition split due to improper partition state (PartitionState: %v)",
                partition->GetState());
            return;
        }


        if (partition->IsImmediateSplitRequested()) {
            if (ValidateSplit(slot, partition, true)) {
                partition->CheckedSetState(EPartitionState::Normal, EPartitionState::Splitting);
                Profiler.Increment(PartitionSplitCounter_);
                DoRunImmediateSplit(slot, partition, Logger);
                // This is inexact to say the least: immediate split is called when we expect that
                // most of the stores will stay intact after splitting by the provided pivots.
                *estimatedMaxOverlappingStoreCount += estimatedStoresDelta;
            }
            return;
        }

        if (estimatedStoresDelta + *estimatedMaxOverlappingStoreCount <= config->MaxOverlappingStoreCount &&
            actualDataSize > config->MaxPartitionDataSize)
        {
            int splitFactor = std::min({
                actualDataSize / config->DesiredPartitionDataSize + 1,
                actualDataSize / config->MinPartitionDataSize,
                static_cast<i64>(config->MaxPartitionCount - partitionCount)});

            if (splitFactor > 1 && ValidateSplit(slot, partition, false)) {
                partition->CheckedSetState(EPartitionState::Normal, EPartitionState::Splitting);
                Profiler.Increment(PartitionSplitCounter_);
                YT_LOG_DEBUG("Partition is scheduled for split");
                tablet->GetEpochAutomatonInvoker()->Invoke(BIND(
                    &TPartitionBalancer::DoRunSplit,
                    MakeStrong(this),
                    slot,
                    partition,
                    splitFactor,
                    partition->GetTablet(),
                    partition->GetId(),
                    tablet->GetId(),
                    Logger));
                *estimatedMaxOverlappingStoreCount += estimatedStoresDelta;
            }
        }
    }

    void ScanPartitionToMerge(TTabletSlotPtr slot, TPartition* partition, int maxAllowedOverlappingStoreCount)
    {
        auto* tablet = partition->GetTablet();
        const auto& config = tablet->GetConfig();
        int partitionCount = tablet->PartitionList().size();
        i64 actualDataSize = partition->GetCompressedDataSize();

        // Maximum data size the partition might have if all chunk stores from Eden go here.
        i64 maxPotentialDataSize = actualDataSize;
        for (const auto& store : tablet->GetEden()->Stores()) {
            if (store->GetType() == EStoreType::SortedChunk) {
                maxPotentialDataSize += store->GetCompressedDataSize();
            }
        }

        auto Logger = BuildLogger(slot, partition);

        YT_LOG_DEBUG_IF(tablet->GetConfig()->EnableLsmVerboseLogging,
            "Scanning partition to merge (PartitionIndex: %v of %v, "
            "DataSize: %v, MaxPotentialDataSize: %v)",
            partition->GetIndex(),
            partitionCount,
            actualDataSize,
            maxPotentialDataSize);

        if (maxPotentialDataSize < config->MinPartitionDataSize && partitionCount > 1) {
            int firstPartitionIndex = partition->GetIndex();
            int lastPartitionIndex = firstPartitionIndex + 1;
            if (lastPartitionIndex == partitionCount) {
                --firstPartitionIndex;
                --lastPartitionIndex;
            }
            int estimatedOverlappingStoreCount = tablet->GetEdenOverlappingStoreCount() +
                tablet->PartitionList()[firstPartitionIndex]->Stores().size() +
                tablet->PartitionList()[lastPartitionIndex]->Stores().size();

            YT_LOG_DEBUG_IF(tablet->GetConfig()->EnableLsmVerboseLogging,
                "Found candidate partitions to merge (FirstPartitionIndex: %v, "
                "LastPartitionIndex: %v, EstimatedOsc: %v, WillRunMerge: %v",
                firstPartitionIndex,
                lastPartitionIndex,
                estimatedOverlappingStoreCount,
                estimatedOverlappingStoreCount < maxAllowedOverlappingStoreCount);

            if (estimatedOverlappingStoreCount <= maxAllowedOverlappingStoreCount) {
                RunMerge(slot, partition, firstPartitionIndex, lastPartitionIndex);
            }
        }
    }

    void ScanPartitionToSample(TTabletSlotPtr slot, TPartition* partition)
    {
        if (partition->GetSamplingRequestTime() > partition->GetSamplingTime() &&
            partition->GetSamplingTime() < TInstant::Now() - Config_->ResamplingPeriod) {
            RunSample(slot, partition);
        }
    }

    bool ValidateSplit(TTabletSlotPtr slot, TPartition* partition, bool immediateSplit) const
    {
        const auto* tablet = partition->GetTablet();

        if (!immediateSplit && TInstant::Now() < partition->GetAllowedSplitTime()) {
            return false;
        }

        auto Logger = BuildLogger(slot, partition);

        if (!tablet->GetConfig()->EnablePartitionSplitWhileEdenPartitioning &&
            tablet->GetEden()->GetState() == EPartitionState::Partitioning)
        {
            YT_LOG_DEBUG("Eden is partitioning, aborting partition split (EdenPartitionId: %v)",
                tablet->GetEden()->GetId());
            return false;
        }

        for (const auto& store : partition->Stores()) {
            if (store->GetStoreState() != EStoreState::Persistent) {
                YT_LOG_DEBUG_IF(tablet->GetConfig()->EnableLsmVerboseLogging,
                    "Aborting partition split due to improper store state "
                    "(StoreId: %v, StoreState: %v)",
                    store->GetId(),
                    store->GetStoreState());
                return false;
            }
        }

        if (immediateSplit) {
            const auto& pivotKeys = partition->PivotKeysForImmediateSplit();
            YT_VERIFY(!pivotKeys.empty());
            if (pivotKeys[0] != partition->GetPivotKey()) {
                YT_LOG_DEBUG_IF(tablet->GetConfig()->EnableLsmVerboseLogging,
                    "Aborting immediate partition split: first proposed pivot key "
                    "does not match partition pivot key (PartitionPivotKey: %v, ProposedPivotKey: %v)",
                    partition->GetPivotKey(),
                    pivotKeys[0]);

                partition->PivotKeysForImmediateSplit().clear();
                return false;
            }

            for (int index = 1; index < pivotKeys.size(); ++index) {
                if (pivotKeys[index] <= pivotKeys[index - 1]) {
                    YT_LOG_DEBUG_IF(tablet->GetConfig()->EnableLsmVerboseLogging,
                        "Aborting immediate partition split: proposed pivots are not sorted");

                    partition->PivotKeysForImmediateSplit().clear();
                    return false;
                }
            }

            if (pivotKeys.back() >= partition->GetNextPivotKey()) {
                YT_LOG_DEBUG_IF(tablet->GetConfig()->EnableLsmVerboseLogging,
                    "Aborting immediate partition split: last proposed pivot key "
                    "is not less than partition next pivot key (NextPivotKey: %v, ProposedPivotKey: %v)",
                    partition->GetNextPivotKey(),
                    pivotKeys.back());

                partition->PivotKeysForImmediateSplit().clear();
                return false;
            }
        }

        return true;
    }

    void DoRunSplit(
        TTabletSlotPtr slot,
        TPartition* partition,
        int splitFactor,
        TTablet* tablet,
        TPartitionId partitionId,
        TTabletId tabletId,
        NLogging::TLogger Logger)
    {
        YT_LOG_DEBUG("Splitting partition");

        YT_VERIFY(tablet == partition->GetTablet());
        const auto& hydraManager = slot->GetHydraManager();

        YT_LOG_INFO("Partition is eligible for split (SplitFactor: %v)",
            splitFactor);

        try {
            auto rowBuffer = New<TRowBuffer>();
            auto samples = GetPartitionSamples(rowBuffer, slot, partition, Config_->MaxPartitioningSampleCount);
            int sampleCount = static_cast<int>(samples.size());
            int minSampleCount = std::max(Config_->MinPartitioningSampleCount, splitFactor);
            if (sampleCount < minSampleCount) {
                THROW_ERROR_EXCEPTION("Too few samples fetched: need %v, got %v",
                    minSampleCount,
                    sampleCount);
            }

            std::vector<TKey> pivotKeys;
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
                THROW_ERROR_EXCEPTION("No valid pivot keys can be obtained from samples");
            }

            TReqSplitPartition request;
            ToProto(request.mutable_tablet_id(), tablet->GetId());
            request.set_mount_revision(tablet->GetMountRevision());
            ToProto(request.mutable_partition_id(), partition->GetId());
            ToProto(request.mutable_pivot_keys(), pivotKeys);

            CreateMutation(hydraManager, request)
                ->CommitAndLog(Logger);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Partition splitting aborted");
            partition->CheckedSetState(EPartitionState::Splitting, EPartitionState::Normal);
            partition->SetAllowedSplitTime(TInstant::Now() + Config_->SplitRetryDelay);
        }
    }

    void DoRunImmediateSplit(
        TTabletSlotPtr slot,
        TPartition* partition,
        NLogging::TLogger Logger)
    {
        YT_LOG_DEBUG("Splitting partition with provided pivot keys (SplitFactor: %v)",
            partition->PivotKeysForImmediateSplit().size());

        auto* tablet = partition->GetTablet();

        std::vector<TOwningKey> pivotKeys;
        pivotKeys.swap(partition->PivotKeysForImmediateSplit());

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
        TTabletSlotPtr slot,
        TPartition* partition,
        int firstPartitionIndex,
        int lastPartitionIndex)
    {
        auto* tablet = partition->GetTablet();

        for (int index = firstPartitionIndex; index <= lastPartitionIndex; ++index) {
            if (tablet->PartitionList()[index]->GetState() != EPartitionState::Normal) {
                YT_LOG_DEBUG_IF(tablet->GetConfig()->EnableLsmVerboseLogging,
                    "Aborting partition split due to improper partition state "
                    "(%v, InitialPartitionId: %v, PartitionId: %v, PartitionIndex: %v, PartitionState: %v)",
                    tablet->GetLoggingId(),
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
        Profiler.Increment(PartitionMergeCounter_);

        auto Logger = TabletNodeLogger;
        Logger.AddTag("%v, CellId: %v, PartitionIds: %v",
            partition->GetTablet()->GetLoggingId(),
            slot->GetCellId(),
            MakeFormattableView(
                MakeRange(
                    tablet->PartitionList().data() + firstPartitionIndex,
                    tablet->PartitionList().data() + lastPartitionIndex + 1),
                TPartitionIdFormatter()));

        YT_LOG_INFO("Partitions are eligible for merge");

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


    bool RunSample(TTabletSlotPtr slot, TPartition* partition)
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

        BIND(&TPartitionBalancer::DoRunSample, MakeStrong(this), Passed(std::move(guard)))
            .AsyncVia(partition->GetTablet()->GetEpochAutomatonInvoker())
            .Run(
                slot,
                partition,
                partition->GetTablet(),
                partition->GetId(),
                partition->GetTablet()->GetId(),
                Logger);
        return true;
    }

    void DoRunSample(
        TAsyncSemaphoreGuard /*guard*/,
        TTabletSlotPtr slot,
        TPartition* partition,
        TTablet* tablet,
        TPartitionId partitionId,
        TTabletId tabletId,
        NLogging::TLogger Logger)
    {
        YT_LOG_DEBUG("Sampling partition");

        YT_VERIFY(tablet == partition->GetTablet());
        auto config = tablet->GetConfig();

        const auto& hydraManager = slot->GetHydraManager();

        try {
            auto compressedDataSize = partition->GetCompressedDataSize();
            if (compressedDataSize == 0) {
                THROW_ERROR_EXCEPTION("Empty partition");
            }

            auto uncompressedDataSize = partition->GetUncompressedDataSize();
            auto scaledSamples = static_cast<int>(
                config->SamplesPerPartition * std::max(compressedDataSize, uncompressedDataSize) / compressedDataSize);
            YT_LOG_INFO("Sampling partition (DesiredSampleCount: %v)", scaledSamples);

            auto rowBuffer = New<TRowBuffer>();
            auto samples = GetPartitionSamples(rowBuffer, slot, partition, scaledSamples);
            samples.erase(
                std::unique(samples.begin(), samples.end()),
                samples.end());

            TWireProtocolWriter writer;
            writer.WriteUnversionedRowset(samples);

            TReqUpdatePartitionSampleKeys request;
            ToProto(request.mutable_tablet_id(), tablet->GetId());
            request.set_mount_revision(tablet->GetMountRevision());
            ToProto(request.mutable_partition_id(), partition->GetId());
            request.set_sample_keys(MergeRefsToString(writer.Finish()));

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


    std::vector<TKey> GetPartitionSamples(
        const TRowBufferPtr& rowBuffer,
        TTabletSlotPtr slot,
        TPartition* partition,
        int maxSampleCount)
    {
        YT_VERIFY(!partition->IsEden());

        if (maxSampleCount == 0) {
            return std::vector<TKey>();
        }

        auto Logger = BuildLogger(slot, partition);

        auto* tablet = partition->GetTablet();

        auto nodeDirectory = New<TNodeDirectory>();

        auto chunkScraper = CreateFetcherChunkScraper(
            Config_->ChunkScraper,
            Bootstrap_->GetControlInvoker(),
            ThrottlerManager_,
            Bootstrap_->GetMasterClient(),
            nodeDirectory,
            Logger);

        auto samplesFetcher = New<TSamplesFetcher>(
            Config_->SamplesFetcher,
            ESamplingPolicy::Partitioning,
            maxSampleCount,
            tablet->PhysicalSchema().GetKeyColumns(),
            NTableClient::MaxSampleSize,
            nodeDirectory,
            GetCurrentInvoker(),
            rowBuffer,
            chunkScraper,
            Bootstrap_->GetMasterClient(),
            Logger);

        {
            auto channel = Bootstrap_->GetMasterClient()->GetMasterChannelOrThrow(
                NApi::EMasterChannelKind::Follower,
                CellTagFromId(tablet->GetId()));
            TChunkServiceProxy proxy(channel);

            auto req = proxy.LocateChunks();
            req->SetHeavy(true);

            THashMap<TChunkId, TSortedChunkStorePtr> storeMap;

            auto addStore = [&] (const ISortedStorePtr& store) {
                if (store->GetType() != EStoreType::SortedChunk)
                    return;

                if (store->GetUpperBoundKey() <= partition->GetPivotKey() ||
                    store->GetMinKey() >= partition->GetNextPivotKey())
                    return;

                auto chunkId = store->AsSortedChunk()->GetChunkId();
                YT_VERIFY(chunkId);
                if (storeMap.insert(std::make_pair(chunkId, store->AsSortedChunk())).second) {
                    ToProto(req->add_subrequests(), chunkId);
                }
            };

            auto addStores = [&] (const THashSet<ISortedStorePtr>& stores) {
                for (const auto& store : stores) {
                    addStore(store);
                }
            };

            addStores(partition->Stores());
            addStores(tablet->GetEden()->Stores());

            if (req->subrequests_size() == 0) {
                return std::vector<TKey>();
            }

            YT_LOG_INFO("Locating partition chunks (ChunkCount: %v)",
                req->subrequests_size());

            auto rspOrError = WaitFor(req->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error locating partition chunks");
            const auto& rsp = rspOrError.Value();
            YT_VERIFY(req->subrequests_size() == rsp->subresponses_size());

            YT_LOG_INFO("Partition chunks located");

            nodeDirectory->MergeFrom(rsp->node_directory());

            for (int index = 0; index < rsp->subresponses_size(); ++index) {
                const auto& subrequest = req->subrequests(index);
                const auto& subresponse = rsp->subresponses(index);

                auto chunkId = FromProto<TChunkId>(subrequest);
                const auto& store = GetOrCrash(storeMap, chunkId);

                NChunkClient::NProto::TChunkSpec chunkSpec;
                ToProto(chunkSpec.mutable_chunk_id(), chunkId);
                *chunkSpec.mutable_replicas() = subresponse.replicas();
                *chunkSpec.mutable_chunk_meta() = store->GetChunkMeta();
                ToProto(chunkSpec.mutable_lower_limit(), TReadLimit(partition->GetPivotKey()));
                ToProto(chunkSpec.mutable_upper_limit(), TReadLimit(partition->GetNextPivotKey()));
                chunkSpec.set_erasure_codec(subresponse.erasure_codec());

                auto inputChunk = New<TInputChunk>(chunkSpec);
                samplesFetcher->AddChunk(std::move(inputChunk));
            }
        }

        WaitFor(samplesFetcher->Fetch())
            .ThrowOnError();

        YT_LOG_DEBUG("Samples fetched");

        std::vector<TKey> samples;
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
                [&] (TKey key) {
                    return key <= partition->GetPivotKey() || key >= partition->GetNextPivotKey();
                }),
            samples.end());

        std::sort(samples.begin(), samples.end());
        return samples;
    }


    static NLogging::TLogger BuildLogger(
        const TTabletSlotPtr& slot,
        TPartition* partition)
    {
        auto logger = TabletNodeLogger;
        logger.AddTag("%v, CellId: %v, PartitionId: %v",
            partition->GetTablet()->GetLoggingId(),
            slot->GetCellId(),
            partition->GetId());
        return logger;
    }
};

void StartPartitionBalancer(
    TTabletNodeConfigPtr config,
    NClusterNode::TBootstrap* bootstrap)
{
    if (config->EnablePartitionBalancer) {
        New<TPartitionBalancer>(config->PartitionBalancer, bootstrap);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
