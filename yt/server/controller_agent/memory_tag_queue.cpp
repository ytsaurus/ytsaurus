#include "memory_tag_queue.h"
#include "private.h"

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ControllerLogger;

////////////////////////////////////////////////////////////////////////////////

TMemoryTagQueue::TMemoryTagQueue()
{
    Reset();
}

void TMemoryTagQueue::Reset()
{
    for (TMemoryTag tag = 1; tag < MaxMemoryTag; ++tag) {
        AvailableTags_.push(tag);
    }
}

TMemoryTag TMemoryTagQueue::AssignTagToOperation(const TOperationId& operationId)
{
    YCHECK(!AvailableTags_.empty());
    auto tag = AvailableTags_.front();
    AvailableTags_.pop();
    OperationIdToTag_[operationId] = tag;
    LOG_DEBUG("Assigning memory tag to operation (OperationId: %v, MemoryTag: %v, AvailableTagCount: %v)",
        operationId,
        tag,
        AvailableTags_.size());
    return tag;
}

void TMemoryTagQueue::ReclaimOperationTag(const TOperationId& operationId)
{
    auto it = OperationIdToTag_.find(operationId);
    YCHECK(it != OperationIdToTag_.end());
    auto tag = it->second;
    OperationIdToTag_.erase(it);
    AvailableTags_.push(tag);
    LOG_DEBUG("Reclaiming memory tag of operation (OperationId: %v, MemoryTag: %v, AvailableTagCount: %v)",
        operationId,
        tag,
        AvailableTags_.size());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
