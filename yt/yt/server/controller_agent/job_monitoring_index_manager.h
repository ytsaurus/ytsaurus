#pragma once

#include "public.h"

#include <util/generic/hash_set.h>

#include <set>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

constexpr int InitialIndexCount = 1024;

////////////////////////////////////////////////////////////////////////////////

class TJobMonitoringIndexManager
{
public:
    TJobMonitoringIndexManager();

    int GetSize() const;
    int AddJob(TOperationId operationId, TJobId jobId);
    bool TryRemoveJob(TOperationId operationId, TJobId jobId);
    bool TryRemoveOperationJobs(TOperationId operationId);

private:
    int Size_ = 0;
    int IndexCount = InitialIndexCount;
    std::set<int> FreeIndices_;
    using TJobIdToIndex = THashMap<TJobId, int>;
    THashMap<TOperationId, TJobIdToIndex> OperationIdToJobIdToIndex_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent

