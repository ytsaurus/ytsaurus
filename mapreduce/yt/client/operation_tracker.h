#pragma once

#include <mapreduce/yt/interface/operation.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/system/mutex.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TOperationExecutionTimeTracker {
public:
    void Start(const TOperationId& operationId);
    TDuration Finish(const TOperationId& operationId);
    static TOperationExecutionTimeTracker* Get();

private:
    THashMap<TOperationId, TInstant> StartTimes_;
    TMutex Lock_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
