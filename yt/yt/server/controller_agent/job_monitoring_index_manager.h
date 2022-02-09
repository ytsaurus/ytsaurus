#pragma once

#include "public.h"

#include <library/cpp/yt/threading/spin_lock.h>

#include <util/generic/hash_set.h>

#include <set>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

constexpr int InitialIndexCount = 1024;

////////////////////////////////////////////////////////////////////////////////

class TJobMonitoringIndexManager
{
public:
    explicit TJobMonitoringIndexManager(int maxSize);

    int GetSize();
    int GetMaxSize();
    int GetResidualCapacity();
    void SetMaxSize(int maxSize);

    std::optional<int> TryAddJob(TOperationId operationId, TJobId jobId);
    bool TryRemoveJob(TOperationId operationId, TJobId jobId);
    bool TryRemoveOperationJobs(TOperationId operationId);

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    int Size_ = 0;
    int MaxSize_ = -1;
    int IndexCount_ = InitialIndexCount;
    std::set<int> FreeIndices_;
    using TJobIdToIndex = THashMap<TJobId, int>;
    THashMap<TOperationId, TJobIdToIndex> OperationIdToJobIdToIndex_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent

