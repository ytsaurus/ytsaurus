#include "cell_tracker.h"

namespace NYT::NHiveClient {

using namespace NElection;

////////////////////////////////////////////////////////////////////////////////

std::vector<TCellId> TCellTracker::Select(const std::vector<TCellId>& candidates)
{
    auto guard = Guard(SpinLock_);

    std::vector<TCellId> result;
    result.reserve(candidates.size());

    for (const auto& id : candidates) {
        if (CellIds_.find(id) != CellIds_.end()) {
            result.push_back(id);
        }
    }

    return result;
}

void TCellTracker::Update(const std::vector<TCellId>& toRemove, const std::vector<TCellId>& toAdd)
{
    auto guard = Guard(SpinLock_);

    for (const auto& id : toRemove) {
        CellIds_.erase(id);
    }
    for (const auto& id : toAdd) {
        CellIds_.insert(id);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveClient

