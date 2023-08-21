#include "partition_balancer.h"

#include "tablet.h"
#include "store.h"
#include "partition.h"

#include <yt/yt/server/lib/tablet_node/config.h>
#include <yt/yt/server/lib/tablet_node/private.h>

#include <yt/yt/client/transaction_client/helpers.h>

namespace NYT::NLsm {

using namespace NTransactionClient;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

const static auto& Logger = NTabletNode::TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TPartitionBalancer
    : public ILsmBackend
{
public:
    void StartNewRound(const TLsmBackendState& state) override
    {
        ResamplingPeriod_ = state.TabletNodeConfig->PartitionBalancer->ResamplingPeriod;
        CurrentTime_ = state.CurrentTime;
    }

    TLsmActionBatch BuildLsmActions(
        const std::vector<TTabletPtr>& tablets,
        const TString& /*bundleName*/) override
    {
        YT_LOG_DEBUG("Started building partition balancer action batch");

        TLsmActionBatch batch;
        for (const auto& tablet : tablets) {
            batch.MergeWith(ScanTablet(tablet.Get()));
        }

        YT_LOG_DEBUG("Finished building partition balancer action batch");

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

        YT_LOG_DEBUG_IF(mountConfig->EnableLsmVerboseLogging,
            "Partition balancer started tablet scan for splits (%v, CurrentMosc: %v)",
            tablet->GetLoggingTag(),
            currentMaxOverlappingStoreCount);

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
                secondLargestPartitionStoreCount);
            if (request) {
                batch.Splits.push_back(std::move(*request));
            }
        }

        int maxAllowedOverlappingStoreCount = mountConfig->MaxOverlappingStoreCount -
            (estimatedMaxOverlappingStoreCount - currentMaxOverlappingStoreCount);

        YT_LOG_DEBUG_IF(mountConfig->EnableLsmVerboseLogging,
            "Partition balancer started tablet scan for merges (%v, "
            "EstimatedMosc: %v, MaxAllowedOsc: %v)",
            tablet->GetLoggingTag(),
            estimatedMaxOverlappingStoreCount,
            maxAllowedOverlappingStoreCount);

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
        int secondLargestPartitionStoreCount)
    {
        auto* tablet = partition->GetTablet();
        const auto& mountConfig = tablet->GetMountConfig();
        int partitionCount = tablet->Partitions().size();
        i64 actualDataSize = partition->GetCompressedDataSize();
        int estimatedStoresDelta = partition->Stores().size();

        auto Logger = BuildLogger(partition);

        YT_LOG_DEBUG_IF(mountConfig->EnableLsmVerboseLogging,
            "Scanning partition to split (PartitionIndex: %v of %v, "
            "EstimatedMosc: %v, DataSize: %v, StoreCount: %v, SecondLargestPartitionStoreCount: %v)",
            partition->GetIndex(),
            partitionCount,
            *estimatedMaxOverlappingStoreCount,
            actualDataSize,
            partition->Stores().size(),
            secondLargestPartitionStoreCount);

        if (partition->GetState() != EPartitionState::Normal) {
            YT_LOG_DEBUG_IF(mountConfig->EnableLsmVerboseLogging,
                "Will not split partition due to improper partition state (PartitionState: %v)",
                partition->GetState());
            return {};
        }

        // TODO(ifsmirnov): validate that all stores are persistent.

        if (partition->GetIsImmediateSplitRequested()) {
            if (ValidateSplit(partition, true)) {
                // This is inexact to say the least: immediate split is called when we expect that
                // most of the stores will stay intact after splitting by the provided pivots.
                *estimatedMaxOverlappingStoreCount += estimatedStoresDelta;
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

            if (splitFactor > 1 && ValidateSplit(partition, false)) {
                YT_LOG_DEBUG("Partition is scheduled for split");
                *estimatedMaxOverlappingStoreCount = maxOverlappingStoreCountAfterSplit;

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

    bool ValidateSplit(TPartition* partition, bool immediateSplit) const
    {
        const auto* tablet = partition->GetTablet();

        auto Logger = BuildLogger(partition);

        const auto& mountConfig = tablet->GetMountConfig();
        if (!immediateSplit && CurrentTime_ < partition->GetAllowedSplitTime()) {
            YT_LOG_DEBUG_IF(mountConfig->EnableLsmVerboseLogging,
                "Will not split partition: too early "
                "(CurrentTime: %v, AllowedSplitTime: %v)",
                CurrentTime_,
                partition->GetAllowedSplitTime());
            return false;
        }

        if (!mountConfig->EnablePartitionSplitWhileEdenPartitioning &&
            tablet->Eden()->GetState() == EPartitionState::Partitioning)
        {
            YT_LOG_DEBUG("Eden is partitioning, will not split partition (EdenPartitionId: %v)",
                tablet->Eden()->GetId());
            return false;
        }

        for (const auto& store : partition->Stores()) {
            if (store->GetStoreState() != EStoreState::Persistent) {
                YT_LOG_DEBUG_IF(mountConfig->EnableLsmVerboseLogging,
                    "Will not split partition due to improper store state "
                    "(StoreId: %v, StoreState: %v)",
                    store->GetId(),
                    store->GetStoreState());
                return false;
            }
        }

        return true;
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

        auto Logger = BuildLogger(partition);

        YT_LOG_DEBUG_IF(mountConfig->EnableLsmVerboseLogging,
            "Scanning partition to merge (PartitionIndex: %v of %v, "
            "DataSize: %v, MaxPotentialDataSize: %v)",
            partition->GetIndex(),
            partitionCount,
            actualDataSize,
            maxPotentialDataSize);

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

            YT_LOG_DEBUG_IF(mountConfig->EnableLsmVerboseLogging,
                "Found candidate partitions to merge (FirstPartitionIndex: %v, "
                "LastPartitionIndex: %v, EstimatedOsc: %v, WillRunMerge: %v",
                firstPartitionIndex,
                lastPartitionIndex,
                estimatedOverlappingStoreCount,
                estimatedOverlappingStoreCount < maxAllowedOverlappingStoreCount);

            std::vector<TPartitionId> partitionIds;
            for (int index = firstPartitionIndex; index <= lastPartitionIndex; ++index) {
                partitionIds.push_back(tablet->Partitions()[index]->GetId());
                if (!ValidateMerge(tablet->Partitions()[index].get())) {
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

    bool ValidateMerge(TPartition* partition) const
    {
        auto Logger = BuildLogger(partition);

        const auto& mountConfig = partition->GetTablet()->GetMountConfig();
        if (CurrentTime_ < partition->GetAllowedMergeTime()) {
            YT_LOG_DEBUG_IF(mountConfig->EnableLsmVerboseLogging,
                "Will not merge partition: too early "
                "(CurrentTime: %v, AllowedMergeTime: %v)",
                CurrentTime_, partition->GetAllowedSplitTime());
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
        return Logger.WithTag("%v, CellId: %v, PartitionId: %v",
            tablet->GetLoggingTag(),
            tablet->GetCellId(),
            partition->GetId());
    }
};

////////////////////////////////////////////////////////////////////////////////

ILsmBackendPtr CreatePartitionBalancer()
{
    return New<TPartitionBalancer>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm
