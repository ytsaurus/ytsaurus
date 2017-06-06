#pragma once

#include <mapreduce/yt/interface/operation.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/system/mutex.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TOperationTracker {
public:
    void Start(const TOperationId& operationId);
    TDuration Finish(const TOperationId& operationId);
    static TOperationTracker* Get();

private:
    yhash<TOperationId, TInstant> StartTimes_;
    TMutex Lock_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
