#include "lsm_backend.h"

#include "partition_balancer.h"
#include "store_compactor.h"
#include "store_rotator.h"
#include "tablet.h"

#include <yt/yt/server/lib/tablet_node/private.h>

namespace NYT::NLsm {

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = NTabletNode::TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

bool TCompactionRequest::operator<(const TCompactionRequest& other) const
{
    auto getOrderingTuple = [] (const TCompactionRequest& request) {
        return std::tuple(
            !request.DiscardStores,
            request.Slack,
            -request.Effect,
            -ssize(request.Stores),
            request.Reason);
    };

    auto getMinStoreCreationTime = [] (const TCompactionRequest& request) {
        TInstant minCreationTime = TInstant::Max();
        for (auto id : request.Stores) {
            minCreationTime = std::min(minCreationTime, request.Tablet->GetStore(id)->GetCreationTime());
        }

        return minCreationTime;
    };

    auto lhsOrderingTuple = getOrderingTuple(*this);
    auto rhsOrderingTuple = getOrderingTuple(other);

    if (lhsOrderingTuple != rhsOrderingTuple) {
        return lhsOrderingTuple < rhsOrderingTuple;
    }

    if (Reason == EStoreCompactionReason::Periodic) {
        return getMinStoreCreationTime(*this) < getMinStoreCreationTime(other);
    }

    return false;
}

////////////////////////////////////////////////////////////////////////////////

void TLsmActionBatch::MergeWith(TLsmActionBatch&& other)
{
    DoMerge(other, &TLsmActionBatch::Compactions);
    DoMerge(other, &TLsmActionBatch::Partitionings);

    DoMerge(other, &TLsmActionBatch::Samplings);
    DoMerge(other, &TLsmActionBatch::Splits);
    DoMerge(other, &TLsmActionBatch::Merges);

    DoMerge(other, &TLsmActionBatch::Rotations);

    DoMerge(other, &TLsmActionBatch::CompactionHintUpdates);
}

TString TLsmActionBatch::GetStatsLoggingString() const
{
    return Format("Compactions: %v, Partitionings: %v, Samplings: %v, "
        "Splits: %v, Merges: %v, Rotations: %v, CompactionHintUpdates: %v",
        Compactions.size(),
        Partitionings.size(),
        Samplings.size(),
        Splits.size(),
        Merges.size(),
        Rotations.size(),
        CompactionHintUpdates.size());
}

////////////////////////////////////////////////////////////////////////////////

class TLsmBackend
    : public ILsmBackend
{
public:
    TLsmBackend()
        : Backends_({
            CreateStoreCompactor(),
            CreatePartitionBalancer(),
            CreateStoreRotator(),
        })
    {
        YT_LOG_DEBUG("Created LSM backend (BackendCount: %v)",
            Backends_.size());
    }

    void StartNewRound(const TLsmBackendState& state) override
    {
        for (const auto& backend : Backends_) {
            backend->StartNewRound(state);
        }
    }

    TLsmActionBatch BuildLsmActions(
        const std::vector<TTabletPtr>& tablets,
        const std::string& bundleName) override
    {
        YT_LOG_DEBUG("Started building LSM action batch");

        TLsmActionBatch batch;
        for (const auto& backend : Backends_) {
            batch.MergeWith(backend->BuildLsmActions(tablets, bundleName));
        }

        YT_LOG_DEBUG("Finished building LSM action batch (%v)",
            batch.GetStatsLoggingString());

        return batch;
    }

    TLsmActionBatch BuildOverallLsmActions() override
    {
        YT_LOG_DEBUG("Started building overall LSM action batch");

        TLsmActionBatch batch;
        for (const auto& backend : Backends_) {
            batch.MergeWith(backend->BuildOverallLsmActions());
        }

        YT_LOG_DEBUG("Finished building overall LSM action batch (%v)",
            batch.GetStatsLoggingString());

        return batch;
    }

private:
    std::vector<ILsmBackendPtr> Backends_;
};

////////////////////////////////////////////////////////////////////////////////

ILsmBackendPtr CreateLsmBackend()
{
    return New<TLsmBackend>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm
