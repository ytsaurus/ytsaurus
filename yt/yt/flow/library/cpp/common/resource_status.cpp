#include "resource_status.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>

#include <library/cpp/yt/memory/new.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TResourceStatus::Update(i64 morePushedToQueue, i64 moreFetchedFromQueue, TInstant now)
{
    QueuePushedTotal_ += morePushedToQueue;
    QueueFetchedTotal_ += moreFetchedFromQueue;
    QueuePushCount_.Update(QueuePushedTotal_, now);
    QueueFetchCount_.Update(QueueFetchedTotal_, now);
    QueueSize_.Set(QueuePushedTotal_ - QueueFetchedTotal_, now);
    LastUpdateTime_ = now;
}

TWorkerResourceStatusPtr TResourceStatus::Collect(TInstant now)
{
    // The queue size is a gauge that holds its last value until the next update; feed it again at
    // collection time, or the averages freeze at the last update and a drained queue keeps
    // reporting the backlog it had just before the drain.
    if (auto last = QueueSize_.Last(); last && now > LastUpdateTime_) {
        QueueSize_.Set(*last, now);
    }

    auto status = New<TWorkerResourceStatus>();
    status->QueueSize30s = QueueSize_.Average()[0];
    status->QueueSize10m = QueueSize_.Average()[1];
    status->QueueGrowthRate30s = QueueSize_.GrowthRate()[0];
    status->QueueGrowthRate10m = QueueSize_.GrowthRate()[1];
    status->QueuePushRate30s = QueuePushCount_.GetRate(0, now);
    status->QueuePushRate10m = QueuePushCount_.GetRate(1, now);
    status->QueueFetchRate30s = QueueFetchCount_.GetRate(0, now);
    status->QueueFetchRate10m = QueueFetchCount_.GetRate(1, now);
    return status;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
