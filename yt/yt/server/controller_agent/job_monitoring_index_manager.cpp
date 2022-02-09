#include "job_monitoring_index_manager.h"

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

TJobMonitoringIndexManager::TJobMonitoringIndexManager(int maxSize)
    : MaxSize_(maxSize)
{
    for (int i = 0; i < IndexCount_; ++i) {
        FreeIndices_.insert(i);
    }
}

int TJobMonitoringIndexManager::GetSize()
{
    auto guard = TGuard(SpinLock_);
    return Size_;
}

int TJobMonitoringIndexManager::GetMaxSize()
{
    auto guard = TGuard(SpinLock_);
    return MaxSize_;
}

int TJobMonitoringIndexManager::GetResidualCapacity()
{
    auto guard = TGuard(SpinLock_);
    return MaxSize_ - Size_;
}

void TJobMonitoringIndexManager::SetMaxSize(int maxSize)
{
    auto guard = TGuard(SpinLock_);
    MaxSize_ = maxSize;
}

std::optional<int> TJobMonitoringIndexManager::TryAddJob(TOperationId operationId, TJobId jobId)
{
    auto guard = TGuard(SpinLock_);

    YT_VERIFY(Size_ <= MaxSize_);
    if (Size_ == MaxSize_) {
        return std::nullopt;
    }

    if (FreeIndices_.empty()) {
        auto newIndexCount = IndexCount_ * 2;
        for (int i = IndexCount_; i < newIndexCount; ++i) {
            FreeIndices_.insert(i);
        }
        IndexCount_ = newIndexCount;
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
    auto guard = TGuard(SpinLock_);

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
    auto guard = TGuard(SpinLock_);

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
