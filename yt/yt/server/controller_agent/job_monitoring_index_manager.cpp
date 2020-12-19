#include "job_monitoring_index_manager.h"

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

TJobMonitoringIndexManager::TJobMonitoringIndexManager()
{
    for (int i = 0; i < IndexCount; ++i) {
        FreeIndices_.insert(i);
    }
}

int TJobMonitoringIndexManager::GetSize() const
{
    return Size_;
}

int TJobMonitoringIndexManager::AddJob(TOperationId operationId, TJobId jobId)
{
    if (FreeIndices_.empty()) {
        auto newIndexCount = IndexCount * 2;
        for (int i = IndexCount; i < newIndexCount; ++i) {
            FreeIndices_.insert(i);
        }
        IndexCount = newIndexCount;
    }
    YT_VERIFY(!FreeIndices_.empty());
    auto indexIt = FreeIndices_.begin();
    auto index = *indexIt;
    FreeIndices_.erase(indexIt);
    YT_VERIFY(OperationIdToJobIdToIndex_[operationId].emplace(jobId, index).second);
    ++Size_;
    return index;
}

bool TJobMonitoringIndexManager::TryRemoveJob(TOperationId operationId, TJobId jobId)
{
    auto jobIdToIndexIt = OperationIdToJobIdToIndex_.find(operationId);
    if (jobIdToIndexIt == OperationIdToJobIdToIndex_.end()) {
        return false;
    }
    auto& jobIdToIndex = jobIdToIndexIt->second;
    auto jobIt = jobIdToIndex.find(jobId);
    if (jobIt == jobIdToIndex.end()) {
        return false;
    }

    auto index = jobIt->second;
    YT_VERIFY(FreeIndices_.insert(index).second);
    jobIdToIndex.erase(jobIt);
    --Size_;
    return true;
}

bool TJobMonitoringIndexManager::TryRemoveOperationJobs(TOperationId operationId)
{
    auto jobIdToIndexIt = OperationIdToJobIdToIndex_.find(operationId);
    if (jobIdToIndexIt == OperationIdToJobIdToIndex_.end()) {
        return false;
    }
    const auto& jobIdToIndex = jobIdToIndexIt->second;
    if (jobIdToIndex.empty()) {
        return false;
    }
    for (const auto& [jobId, index] : jobIdToIndex) {
        YT_VERIFY(FreeIndices_.insert(index).second);
    }

    Size_ -= static_cast<int>(jobIdToIndex.size());
    OperationIdToJobIdToIndex_.erase(jobIdToIndexIt);
    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
