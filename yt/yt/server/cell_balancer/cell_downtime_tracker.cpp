#include "cell_downtime_tracker.h"

namespace NYT::NCellBalancer {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

// static constexpr auto& Logger = BundleControllerLogger;
static const std::string CellStateLeading = "leading";

////////////////////////////////////////////////////////////////////////////////

class TCellDowntimeTracker
    : public ICellDowntimeTracker
{
public:
    void HandleState(
        const TSchedulerInputState& inputState,
        TBundleSensorProvider sensorsProvider) override
    {
        for (const auto& [bundleName, _] : inputState.Bundles) {
            CheckBundleDowntime(bundleName, inputState, sensorsProvider);
        }
    }


private:
    THashMap<std::string, TInstant> LastLeaderSeenTime_;


    void CheckBundleDowntime(
        const std::string& bundleName,
        const TSchedulerInputState& inputState,
        TBundleSensorProvider sensorsProvider)
    {
        const auto& bundleInfo = GetOrCrash(inputState.Bundles, bundleName);

        TDuration bundleDowntime;
        auto now = TInstant::Now();

        for (const auto& cellId : bundleInfo->TabletCellIds) {
            auto& lastLeaderSeenTime = LastLeaderSeenTime_[cellId];
            auto leaderSeenTime = GetCellLeaderSeenTime(cellId, inputState);
            if (leaderSeenTime) {
                lastLeaderSeenTime = *leaderSeenTime;
            }

            if (lastLeaderSeenTime != TInstant::Zero()) {
                bundleDowntime = std::max(now - lastLeaderSeenTime, bundleDowntime);
            }
        }

        auto bundleSensors = sensorsProvider(bundleName);
        bundleSensors->BundleCellsDowntime.Update(bundleDowntime);
    }

    std::optional<TInstant> GetCellLeaderSeenTime(
        const std::string& cellId,
        const TSchedulerInputState& inputState) const
    {
        auto it = inputState.TabletCells.find(cellId);
        if (it == inputState.TabletCells.end()) {
            return {};
        }

        for (const auto& peer : it->second->Peers) {
            if (peer->LastSeenState == CellStateLeading && peer->State == CellStateLeading) {
                return peer->LastSeenTime;
            }
        }

        return {};
    }
};

////////////////////////////////////////////////////////////////////////////////

ICellDowntimeTrackerPtr CreateCellDowntimeTracker()
{
    return New<TCellDowntimeTracker>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
