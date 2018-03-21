#include "schedule_queue.h"

#include <yt/core/misc/heap.h>

namespace NYP {
namespace NServer {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

void TScheduleQueue::Enqueue(TObjectId id, TInstant deadline)
{
    if (!Ids_.insert(id).second) {
        return;
    }
    Heap_.push_back({std::move(id), deadline});
    AdjustHeapBack(Heap_.begin(), Heap_.end());
}

TObjectId TScheduleQueue::Dequeue(TInstant deadline)
{
    if (Heap_.empty() || Heap_.front().Deadline > deadline) {
        return TObjectId();
    }
    auto id = std::move(Heap_.front().Id);
    ExtractHeap(Heap_.begin(), Heap_.end());
    Heap_.pop_back();
    YCHECK(Ids_.erase(id) == 1);
    return id;
}

bool TScheduleQueue::TEntry::operator<(const TScheduleQueue::TEntry& other) const
{
    return Deadline < other.Deadline;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NServer
} // namespace NYP

