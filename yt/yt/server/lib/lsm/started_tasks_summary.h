#pragma once

#include "lsm_backend.h"
#include "public.h"

namespace NYT::NLsm {

////////////////////////////////////////////////////////////////////////////////

class TBackgroundTaskHistory
{
public:
    // Table path, compaction reason.
    using TKey = std::pair<NYPath::TYPath, EStoreCompactionReason>;

    void RegisterTasks(const std::vector<TStartedCompactionTask>& tasks, TInstant now);

    void UpdateWindow(TDuration newWindow);

    double GetWeight(const TKey& key) const;

private:
    static constexpr double ObsoleteKeyThreshold_ = 1e-9;
    TDuration Window_;
    TInstant LastUpdateTimestamp_;

    THashMap<TKey, double> Counter_;

    double GetNormalizationFactor(TInstant now) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TStartedTasksSummary
{
public:
    TBackgroundTaskHistory CompactionHistory;
    TBackgroundTaskHistory PartitioningHistory;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm
