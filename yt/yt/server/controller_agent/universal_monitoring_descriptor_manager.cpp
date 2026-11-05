#include "universal_monitoring_descriptor_manager.h"

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

TUniversalMonitoringDescriptorManager::TUniversalMonitoringDescriptorManager(int maxSize)
    : MaxSize_(maxSize)
{ }

int TUniversalMonitoringDescriptorManager::GetSize() const
{
    auto guard = TGuard(SpinLock_);
    return Size_;
}

int TUniversalMonitoringDescriptorManager::GetMaxSize() const
{
    auto guard = TGuard(SpinLock_);
    return MaxSize_;
}

int TUniversalMonitoringDescriptorManager::GetResidualCapacity() const
{
    auto guard = TGuard(SpinLock_);
    return MaxSize_ - Size_;
}

int TUniversalMonitoringDescriptorManager::GetNotAcquiredMonitoringDescriptorsCount() const
{
    auto guard = TGuard(SpinLock_);
    return NotAcquiredMonitoringDescriptorsCount_;
}

void TUniversalMonitoringDescriptorManager::SetMaxSize(int maxSize)
{
    auto guard = TGuard(SpinLock_);
    MaxSize_ = maxSize;
}

bool TUniversalMonitoringDescriptorManager::TryAcqireMonitoringDescriptor(TOperationId operationId)
{
    auto guard = TGuard(SpinLock_);
    if (Size_ >= MaxSize_) {
        ++NotAcquiredMonitoringDescriptorsCount_;
        return false;
    }
    OperationIdToCountOfAcquiredMonitor_[operationId]++;
    ++Size_;
    return true;
}

bool TUniversalMonitoringDescriptorManager::TryReleaseMonitoringDescriptor(TOperationId operationId)
{
    auto guard = TGuard(SpinLock_);

    auto it = OperationIdToCountOfAcquiredMonitor_.find(operationId);
    if (it == OperationIdToCountOfAcquiredMonitor_.end() || it->second == 0) {
        return false;
    }

    YT_VERIFY(Size_ > 0);
    --Size_;
    --it->second;

    return true;
}

bool TUniversalMonitoringDescriptorManager::TryRemoveOperation(TOperationId operationId)
{
    auto guard = TGuard(SpinLock_);
    auto it = OperationIdToCountOfAcquiredMonitor_.find(operationId);
    if (it == OperationIdToCountOfAcquiredMonitor_.end()) {
        return false;
    }

    auto countOfUnreleasedMonitor = it->second;
    Size_ -= countOfUnreleasedMonitor;
    OperationIdToCountOfAcquiredMonitor_.erase(it);
    return true;
}

void TUniversalMonitoringDescriptorManager::RemoveAllOperations()
{
    auto guard = TGuard(SpinLock_);

    for (const auto& [_, countOfAcquiredMonitor] : OperationIdToCountOfAcquiredMonitor_) {
        Size_ -= countOfAcquiredMonitor;
    }

    YT_VERIFY(Size_ == 0);

    OperationIdToCountOfAcquiredMonitor_.clear();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
