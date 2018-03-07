#pragma once

#include "public.h"

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

class TMemoryTagQueue
{
public:
    TMemoryTagQueue();

    void Reset();

    TMemoryTag AssignTagToOperation(const TOperationId& operationId);
    void ReclaimOperationTag(const TOperationId& operationId);

private:
    std::queue<TMemoryTag> AvailableTags_;
    THashMap<TOperationId, TMemoryTag> OperationIdToTag_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT

