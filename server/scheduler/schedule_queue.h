#pragma once

#include "private.h"

namespace NYP::NServer::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TScheduleQueue
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
