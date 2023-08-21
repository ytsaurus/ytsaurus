#include "lsm_backend.h"

#include "store_compactor.h"
#include "partition_balancer.h"
#include "store_rotator.h"

#include <yt/yt/server/lib/tablet_node/private.h>

namespace NYT::NLsm {

////////////////////////////////////////////////////////////////////////////////

const static auto& Logger = NTabletNode::TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

void TLsmActionBatch::MergeWith(TLsmActionBatch&& other)
{
    DoMerge(other, &TLsmActionBatch::Compactions);
    DoMerge(other, &TLsmActionBatch::Partitionings);

    DoMerge(other, &TLsmActionBatch::Samplings);
    DoMerge(other, &TLsmActionBatch::Splits);
    DoMerge(other, &TLsmActionBatch::Merges);

    DoMerge(other, &TLsmActionBatch::Rotations);
}

TString TLsmActionBatch::GetStatsLoggingString() const
{
    return Format("Compactions: %v, Partitionings: %v, Samplings: %v, "
        "Splits: %v, Merges: %v, Rotations: %v",
        Compactions.size(),
        Partitionings.size(),
        Samplings.size(),
        Splits.size(),
        Merges.size(),
        Rotations.size());
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
        const TString& bundleName) override
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
