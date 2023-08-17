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

    std::optional<int> TryAddIndex(TOperationId operationId);
    bool TryRemoveIndex(TOperationId operationId, int index);
    bool TryRemoveOperation(TOperationId operationId);

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    int Size_ = 0;
    int MaxSize_ = -1;
    int IndexCount_ = InitialIndexCount;
    std::set<int> FreeIndices_;
    THashMap<TOperationId, THashSet<int>> OperationIdToIndexes_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent

