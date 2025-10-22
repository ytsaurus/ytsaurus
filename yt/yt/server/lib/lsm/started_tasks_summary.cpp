#include "started_tasks_summary.h"

namespace NYT::NLsm {

////////////////////////////////////////////////////////////////////////////////

void TBackgroundTaskHistory::RegisterTasks(const std::vector<TStartedCompactionTask>& tasks, TInstant now)
{
    if (!Counter_.empty()) {
        std::vector<TKey> obsoleteKeys;

        double factor = GetNormalizationFactor(now);
        for (auto& [key, value] : Counter_) {
            value *= factor;
            if (value < ObsoleteKeyThreshold_) {
                obsoleteKeys.push_back(key);
            }
        }

        for (const auto& key : obsoleteKeys) {
            Counter_.erase(key);
        }
    }

    LastUpdateTimestamp_ = now;
    for (const auto& task : tasks) {
        Counter_[TKey{task.TablePath, task.Reason}] += 1;
    }
}

void TBackgroundTaskHistory::UpdateWindow(TDuration newWindow)
{
    Window_ = newWindow;
}

double TBackgroundTaskHistory::GetWeight(const TKey& key) const
{
    return GetOrDefault(Counter_, key, 0);
}

double TBackgroundTaskHistory::GetNormalizationFactor(TInstant now) const
{
    return std::exp2((LastUpdateTimestamp_ - now) / Window_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm
