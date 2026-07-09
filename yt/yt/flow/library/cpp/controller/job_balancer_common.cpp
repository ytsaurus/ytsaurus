#include "private.h"

#include "job_balancer_common.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>

namespace NYT::NFlow::NBalancer {

////////////////////////////////////////////////////////////////////////////////

bool ComputationBelongsToGroup(const TComputationSpecPtr& computationSpec, const TWorkerGroupId& workerGroup)
{
    return computationSpec->WorkerGroup == workerGroup;
}

bool WorkerBelongsToGroup(const TWorkerPtr& worker, const TWorkerGroupId& workerGroup)
{
    if (worker->Groups.empty()) {
        return workerGroup.Underlying().empty();
    }
    return FindPtr(worker->Groups, workerGroup);
}

////////////////////////////////////////////////////////////////////////////////

void CompactRebalanceActions(std::vector<TRebalanceResultAction>& actions)
{
    // O(N) algorithm:
    //
    // Walk actions left-to-right. For each Add, append it to `result` and
    // record its index in `lastAddIndex`. For each Del that matches a previous
    // Add (same partition, same worker), mark that Add slot as cancelled
    // (tombstone) instead of erasing it — avoiding any index shifting.
    // Unmatched Dels are appended normally.
    // A final erase-remove pass removes all tombstones in O(N).

    // Sentinel: a cancelled (tombstoned) slot is represented by an action
    // whose Type is neither Add nor Del. We repurpose the unused value by
    // checking a separate bitset to avoid touching the enum.
    // Simpler: use a parallel bool vector.

    std::vector<TRebalanceResultAction> result;
    result.reserve(actions.size());

    std::vector<bool> cancelled; // parallel to `result`; true = tombstoned
    cancelled.reserve(actions.size());

    // lastAddIndex[partitionId] = index in `result` of the most recent Add
    // for that partition (may point to a tombstoned slot).
    THashMap<TPartitionId, size_t> lastAddIndex;

    for (auto& action : actions) {
        if (action.Type == ERebalanceActionType::Add) {
            lastAddIndex[action.PartitionId] = result.size();
            result.push_back(std::move(action));
            cancelled.push_back(false);
        } else { // Del
            auto it = lastAddIndex.find(action.PartitionId);
            if (it != lastAddIndex.end() &&
                !cancelled[it->second] &&
                result[it->second].WorkerAddress == action.WorkerAddress)
            {
                // Cancel the matching Add — mark as tombstone, skip this Del.
                cancelled[it->second] = true;
                lastAddIndex.erase(it);
            } else {
                result.push_back(std::move(action));
                cancelled.push_back(false);
            }
        }
    }

    // Remove tombstoned slots in a single O(N) pass.
    size_t write = 0;
    for (size_t read = 0; read < result.size(); ++read) {
        if (!cancelled[read]) {
            if (write != read) {
                result[write] = std::move(result[read]);
            }
            ++write;
        }
    }
    result.resize(write);

    actions = std::move(result);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NBalancer
