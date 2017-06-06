#include "operation_tracker.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void TOperationTracker::Start(const TOperationId& operationId) {
    with_lock(Lock_) {
        StartTimes_[operationId] = TInstant::Now();
    }
}

TDuration TOperationTracker::Finish(const TOperationId& operationId) {
    TDuration duration;
    with_lock(Lock_) {
        auto i = StartTimes_.find(operationId);
        if (i == StartTimes_.end()) {
            ythrow yexception() <<
                "Operation " << GetGuidAsString(operationId) << " did not start";
        }
        duration = TInstant::Now() - i->second;
        StartTimes_.erase(i);
    }
    return duration;
}

TOperationTracker* TOperationTracker::Get() {
    return Singleton<TOperationTracker>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
