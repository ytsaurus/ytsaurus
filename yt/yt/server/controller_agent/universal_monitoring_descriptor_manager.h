#pragma once

#include "public.h"

#include <library/cpp/yt/system/spin_lock.h>

#include <util/generic/hash_set.h>

#include <set>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

// Thread affinity: any.
class TUniversalMonitoringDescriptorManager
{
public:
    explicit TUniversalMonitoringDescriptorManager(int maxSize);

    int GetSize() const;
    int GetMaxSize() const;
    int GetResidualCapacity() const;
    int GetNotAcquiredMonitoringDescriptorsCount() const;
    void SetMaxSize(int maxSize);

    bool TryAcqireMonitoringDescriptor(TOperationId operationId);
    bool TryReleaseMonitoringDescriptor(TOperationId operationId);
    bool TryRemoveOperation(TOperationId operationId);
    void RemoveAllOperations();

private:
    YT_DECLARE_SPIN_LOCK(TSpinLock, SpinLock_);
    int Size_ = 0;
    int MaxSize_ = -1;
    int NotAcquiredMonitoringDescriptorsCount_ = 0;
    THashMap<TOperationId, int> OperationIdToCountOfAcquiredMonitor_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
