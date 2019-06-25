#pragma once

#include "private.h"

namespace NYP::NServer::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TScheduleQueue
    : public TRefCounted
{
public:
    void Enqueue(TObjectId id, TInstant deadline);
    TObjectId Dequeue(TInstant deadline);

private:
    struct TEntry
    {
        TObjectId Id;
        TInstant Deadline;

        bool operator<(const TEntry& other) const;
    };
    std::vector<TEntry> Heap_;
    THashSet<TObjectId> Ids_;
};

DEFINE_REFCOUNTED_TYPE(TScheduleQueue)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
