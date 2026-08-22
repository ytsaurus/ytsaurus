#include "partition_balancer.h"

#include "config.h"
#include "partition.h"
#include "store.h"
#include "tablet.h"

#include <yt/yt/server/lib/tablet_node/config.h>
#include <yt/yt/server/lib/tablet_node/private.h>

#include <yt/yt/client/transaction_client/helpers.h>

namespace NYT::NLsm {

using namespace NTransactionClient;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = NTabletNode::TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TPartitionBalancer
    : public ILsmBackend
{
public:
    void StartNewRound(const TLsmBackendState& state) override
    {
        ResamplingPeriod_ = state.TabletNodeConfig->ResamplingPeriod;
        CurrentTime_ = state.CurrentTime;
    }

    TLsmActionBatch BuildLsmActions(
        const std::vector<TTabletPtr>& tablets,
        const std::string& /*bundleName*/) override
    {
        YT_TLOG_DEBUG("Started building partition balancer action batch");

        TLsmActionBatch batch;
        for (const auto& tablet : tablets) {
            batch.MergeWith(ScanTablet(tablet.Get()));
        }

        YT_TLOG_DEBUG("Finished building partition balancer action batch");

        return batch;
    }

    TLsmActionBatch BuildOverallLsmActions() override
    {
        return {};
    }

private:
    TDuration ResamplingPeriod_;
    // System time. Used for imprecise activities like periodic compaction.
    TInstant CurrentTime_;

    TLsmActionBatch ScanTablet(TTablet* tablet)
    {
        TLsmActionBatch batch;

        if (!tablet->GetMounted()) {
            return batch;
        }

        if (!tablet->IsPhysicallySorted()) {
            return batch;
        }

        for (const auto& partition : tablet->Partitions()) {
            if (auto request = ScanPartitionToSample(partition.get())) {
                batch.Samplings.push_back(std::move(*request));
            }
        }

        const auto& mountConfig = tablet->GetMountConfig();
        if (!mountConfig->EnableCompactionAndPartitioning) {
            return batch;
        }

        int currentMaxOverlappingStoreCount = tablet->GetOverlappingStoreCount();
        int estimatedMaxOverlappingStoreCount = currentMaxOverlappingStoreCount;

        i64 currentEdenDataSize = tablet->Eden()->GetCompressedDataSize();
        i64 estimatedEdenDataSize = currentEdenDataSize;
        bool atLeastOneSplitScheduled = false;

        for (const auto& partition : tablet->Partitions()) {
            if (partition->GetState() == EPartitionState::Splitting) {
                estimatedEdenDataSize += partition->GetCompressedDataSize();
                atLeastOneSplitScheduled = true;
            }
        }

        YT_TLOG_DEBUG_IF(mountConfig->EnableLsmVerboseLogging, "Partition balancer started tablet scan for splits")
            .With(tablet->LoggingTags())
            .With("CurrentMosc", currentMaxOverlappingStoreCount);

        int largestPartitionStoreCount = 0;
        int secondLargestPartitionStoreCount = 0;
        for (const auto& partition : tablet->Partitions()) {
            int storeCount = partition->Stores().size();
            if (storeCount > largestPartitionStoreCount) {
                secondLargestPartitionStoreCount = largestPartitionStoreCount;
                largestPartitionStoreCount = storeCount;
            } else if (storeCount > secondLargestPartitionStoreCount) {
                secondLargestPartitionStoreCount = storeCount;
            }
        }

        for (const auto& partition : tablet->Partitions()) {
            auto request = ScanPartitionToSplit(
                partition.get(),
                &estimatedMaxOverlappingStoreCount,
                &estimatedEdenDataSize,
                &atLeastOneSplitScheduled,
                secondLargestPartitionStoreCount);
            if (request) {
                batch.Splits.push_back(std::move(*request));
            }
        }

        int maxAllowedOverlappingStoreCount = mountConfig->MaxOverlappingStoreCount -
            (estimatedMaxOverlappingStoreCount - currentMaxOverlappingStoreCount);

        YT_TLOG_DEBUG_IF(mountConfig->EnableLsmVerboseLogging, "Partition balancer started tablet scan for merges")
            .With(tablet->LoggingTags())
            .With("EstimatedMosc", estimatedMaxOverlappingStoreCount)
            .With("MaxAllowedOsc", maxAllowedOverlappingStoreCount);

        for (const auto& partition : tablet->Partitions()) {
            auto request = ScanPartitionToMerge(partition.get(), maxAllowedOverlappingStoreCount);
            if (request) {
                batch.Merges.push_back(std::move(*request));
            }
        }

        return batch;
    }

    std::optional<TSplitPartitionRequest> ScanPartitionToSplit(
        TPartition* partition,
        int* estimatedMaxOverlappingStoreCount,
        i64* estimatedEdenDataSize,
        bool* atLeastOneSplitScheduled,
        int secondLargestPartitionStoreCount)
    {
        auto* tablet = partition->GetTablet();
        const auto& mountConfig = tablet->GetMountConfig();
        int partitionCount = tablet->Partitions().size();
        i64 actualDataSize = partition->GetCompressedDataSize();
        int estimatedStoresDelta = partition->Stores().size();

        auto Logger = mountConfig->EnableLsmVerboseLogging
            ? BuildLogger(partition)
            : NLogging::TLogger();

        YT_TLOG_DEBUG("Scanning partition to split")
            .WithFormat("PartitionIndex", "%v of %v", partition->GetIndex(), partitionCount)
            .With("EstimatedMosc", *estimatedMaxOverlappingStoreCount)
            .With("EstimatedEdenDataSize", *estimatedEdenDataSize)
            .With("AtLeastOneSplitScheduled", *atLeastOneSplitScheduled)
            .With("DataSize", actualDataSize)
            .With("StoreCount", partition->Stores().size())
            .With("SecondLargestPartitionStoreCount", secondLargestPartitionStoreCount);

        if (partition->GetState() != EPartitionState::Normal) {
            YT_TLOG_DEBUG("Will not split partition due to improper partition state")
                .With("PartitionState", partition->GetState());
            return {};
        }

        // TODO(ifsmirnov): validate that all stores are persistent.

        if (partition->GetIsImmediateSplitRequested()) {
            if (ValidateSplit(partition, *estimatedEdenDataSize, *atLeastOneSplitScheduled, true, Logger)) {
                // This is inexact to say the least: immediate split is called when we expect that
                // most of the stores will stay intact after splitting by the provided pivots.
                *estimatedMaxOverlappingStoreCount += estimatedStoresDelta;
                *estimatedEdenDataSize += partition->GetCompressedDataSize();
                *atLeastOneSplitScheduled = true;

                return TSplitPartitionRequest{
                    .Tablet = MakeStrong(tablet),
                    .PartitionId = partition->GetId(),
                    .PartitionIndex = partition->GetIndex(),
                    .Immediate = true,
                };
            }
            return {};
        }

        int maxOverlappingStoreCountAfterSplit = estimatedStoresDelta + *estimatedMaxOverlappingStoreCount;
        // If the partition is the largest one, the estimate is incorrect since its stores will move to eden
        // and the partition will no longer contribute to the first summand in (max_partition_size + eden_size).
        // Instead, the second largest partition will.
        if (ssize(partition->Stores()) > secondLargestPartitionStoreCount) {
            maxOverlappingStoreCountAfterSplit -= ssize(partition->Stores()) - secondLargestPartitionStoreCount;
        }

        if (maxOverlappingStoreCountAfterSplit <= mountConfig->MaxOverlappingStoreCount &&
            actualDataSize > mountConfig->MaxPartitionDataSize)
        {
            int splitFactor = std::min({
                actualDataSize / mountConfig->DesiredPartitionDataSize + 1,
                actualDataSize / mountConfig->MinPartitionDataSize,
                static_cast<i64>(mountConfig->MaxPartitionCount - partitionCount)});

            if (splitFactor > 1 && ValidateSplit(partition, *estimatedEdenDataSize, *atLeastOneSplitScheduled, false, Logger)) {
                if (!Logger) {
                    Logger = BuildLogger(partition);
                }
                YT_TLOG_DEBUG("Partition is scheduled for split");
                *estimatedMaxOverlappingStoreCount = maxOverlappingStoreCountAfterSplit;
                *estimatedEdenDataSize += partition->GetCompressedDataSize();
                *atLeastOneSplitScheduled = true;

                return TSplitPartitionRequest{
                    .Tablet = MakeStrong(tablet),
                    .PartitionId = partition->GetId(),
                    .PartitionIndex = partition->GetIndex(),
                    .SplitFactor = splitFactor,
                };
            }
        }

        return {};
    }

    bool ValidateSplit(
        TPartition* partition,
        i64 estimatedEdenDataSize,
        bool atLeastOneSplitScheduled,
        bool immediateSplit,
        const NLogging::TLogger& Logger) const
    {
        const auto* tablet = partition->GetTablet();

        const auto& mountConfig = tablet->GetMountConfig();
        if (!immediateSplit && CurrentTime_ < partition->GetAllowedSplitTime()) {
            YT_TLOG_DEBUG("Will not split partition: too early")
                .With("CurrentTime", CurrentTime_)
                .With("AllowedSplitTime", partition->GetAllowedSplitTime());
            return false;
        }

        if (!mountConfig->EnablePartitionSplitWhileEdenPartitioning &&
            tablet->Eden()->GetState() == EPartitionState::Partitioning)
        {
            YT_TLOG_DEBUG("Eden is partitioning, will not split partition")
                .With("EdenPartitionId", tablet->Eden()->GetId());
            return false;
        }

        for (const auto& store : partition->Stores()) {
            if (store->GetStoreState() != EStoreState::Persistent) {
                YT_TLOG_DEBUG("Will not split partition due to improper store state")
                    .With("StoreId", store->GetId())
                    .With("StoreState", store->GetStoreState());
                return false;
            }
        }

        i64 currentEdenDataSize = tablet->Eden()->GetCompressedDataSize();
        estimatedEdenDataSize += partition->GetCompressedDataSize();

        return currentEdenDataSize < mountConfig->MaxEdenDataSizeForSplitting &&
            (!atLeastOneSplitScheduled || estimatedEdenDataSize <= mountConfig->MaxEdenDataSizeForSplitting);
    }

    std::optional<TMergePartitionsRequest> ScanPartitionToMerge(
        TPartition* partition,
        int maxAllowedOverlappingStoreCount)
    {
        auto* tablet = partition->GetTablet();
        const auto& mountConfig = tablet->GetMountConfig();
        int partitionCount = tablet->Partitions().size();
        i64 actualDataSize = partition->GetCompressedDataSize();

        // Maximum data size the partition might have if all chunk stores from Eden go here.
        i64 maxPotentialDataSize = actualDataSize;
        for (const auto& store : tablet->Eden()->Stores()) {
            if (store->GetType() == EStoreType::SortedChunk) {
                maxPotentialDataSize += store->GetCompressedDataSize();
            }
        }

        NLogging::TLogger Logger;
        if (mountConfig->EnableLsmVerboseLogging) {
            Logger = BuildLogger(partition);
        }

        YT_TLOG_DEBUG("Scanning partition to merge")
            .WithFormat("PartitionIndex", "%v of %v", partition->GetIndex(), partitionCount)
            .With("DataSize", actualDataSize)
            .With("MaxPotentialDataSize", maxPotentialDataSize);

        if (maxPotentialDataSize < mountConfig->MinPartitionDataSize && partitionCount > 1) {
            int firstPartitionIndex = partition->GetIndex();
            int lastPartitionIndex = firstPartitionIndex + 1;
            if (lastPartitionIndex == partitionCount) {
                --firstPartitionIndex;
                --lastPartitionIndex;
            }
            int estimatedOverlappingStoreCount = tablet->GetEdenOverlappingStoreCount() +
                tablet->Partitions()[firstPartitionIndex]->Stores().size() +
                tablet->Partitions()[lastPartitionIndex]->Stores().size();

            YT_TLOG_DEBUG("Found candidate partitions to merge")
                .With("FirstPartitionIndex", firstPartitionIndex)
                .With("LastPartitionIndex", lastPartitionIndex)
                .With("EstimatedOsc", estimatedOverlappingStoreCount)
                .With("WillRunMerge", estimatedOverlappingStoreCount < maxAllowedOverlappingStoreCount);

            std::vector<TPartitionId> partitionIds;
            for (int index = firstPartitionIndex; index <= lastPartitionIndex; ++index) {
                partitionIds.push_back(tablet->Partitions()[index]->GetId());
                if (!ValidateMerge(tablet->Partitions()[index].get(), Logger)) {
                    return {};
                }
            }

            if (estimatedOverlappingStoreCount < maxAllowedOverlappingStoreCount) {
                return TMergePartitionsRequest{
                    .Tablet = MakeStrong(tablet),
                    .FirstPartitionIndex = firstPartitionIndex,
                    .PartitionIds = std::move(partitionIds),
                };
            }
        }

        return {};
    }

    bool ValidateMerge(TPartition* partition, const NLogging::TLogger& Logger) const
    {
        const auto& mountConfig = partition->GetTablet()->GetMountConfig();
        if (CurrentTime_ < partition->GetAllowedMergeTime()) {
            YT_TLOG_DEBUG("Will not merge partition: too early")
                .With("CurrentTime", CurrentTime_)
                .With("AllowedMergeTime", partition->GetAllowedMergeTime());
            return false;
        }
        return true;
    }

    std::optional<TSamplePartitionRequest> ScanPartitionToSample(TPartition* partition) const
    {
        if (partition->GetSamplingRequestTime() > partition->GetSamplingTime() &&
            partition->GetSamplingTime() < CurrentTime_ - ResamplingPeriod_)
        {
            auto* tablet = partition->GetTablet();
            return TSamplePartitionRequest{
                .Tablet = MakeStrong(tablet),
                .PartitionId = partition->GetId(),
                .PartitionIndex = partition->GetIndex(),
            };
        }

        return {};
    }

    static NLogging::TLogger BuildLogger(TPartition* partition)
    {
        auto* tablet = partition->GetTablet();
        return Logger()
            .WithTags(tablet->LoggingTags())
            .WithTag("CellId", tablet->GetCellId())
            .WithTag("PartitionId", partition->GetId());
    }
};

////////////////////////////////////////////////////////////////////////////////

ILsmBackendPtr CreatePartitionBalancer()
{
    return New<TPartitionBalancer>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm
