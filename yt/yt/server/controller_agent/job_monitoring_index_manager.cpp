#include "job_monitoring_index_manager.h"

#include <yt/yt/core/misc/collection_helpers.h>

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

std::optional<int> TJobMonitoringIndexManager::TryAddIndex(TOperationId operationId)
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
    InsertOrCrash(OperationIdToIndexes_[operationId], index);
    ++Size_;
    return index;
}

bool TJobMonitoringIndexManager::TryRemoveIndex(TOperationId operationId, int index)
{
    auto guard = TGuard(SpinLock_);

    auto indexesIt = OperationIdToIndexes_.find(operationId);
    if (indexesIt == OperationIdToIndexes_.end()) {
        return false;
    }
    auto& indexes = indexesIt->second;

    auto indexIt = indexes.find(index);
    if (indexIt == indexes.end()) {
        return false;
    }

    InsertOrCrash(FreeIndices_, index);
    indexes.erase(indexIt);
    --Size_;
    return true;
}

bool TJobMonitoringIndexManager::TryRemoveOperation(TOperationId operationId)
{
    auto guard = TGuard(SpinLock_);

    auto indexesIt = OperationIdToIndexes_.find(operationId);
    if (indexesIt == OperationIdToIndexes_.end()) {
        return false;
    }
    auto& indexes = indexesIt->second;
    if (indexes.empty()) {
        OperationIdToIndexes_.erase(indexesIt);
        return false;
    }
    for (auto index : indexes) {
        InsertOrCrash(FreeIndices_, index);
    }

    Size_ -= static_cast<int>(indexes.size());
    OperationIdToIndexes_.erase(indexesIt);
    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
