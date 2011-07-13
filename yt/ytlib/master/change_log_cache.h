#pragma once

#include "common.h"
#include "change_log.h"
#include "async_change_log.h"

#include "../misc/cache.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TCachedChangeLog
    : public TCacheValueBase<i32, TCachedChangeLog>
    , public TAsyncChangeLog
{
public:
    TCachedChangeLog(TChangeLog::TPtr changeLog)
        : TCacheValueBase<i32, TCachedChangeLog>(changeLog->GetId())
        , TAsyncChangeLog(changeLog)
    { }
};

////////////////////////////////////////////////////////////////////////////////

class TChangeLogCache
    : public TCapacityLimitedCache<i32, TCachedChangeLog>
{
public:
    typedef TIntrusivePtr<TChangeLogCache> TPtr;

    TChangeLogCache(Stroka location);

    TCachedChangeLog::TPtr Get(i32 segmentId);
    TCachedChangeLog::TPtr Create(i32 segmentId, i32 prevRecordCount);

protected:
    virtual void OnTrim(TValuePtr value);

private:
    Stroka GetChangeLogFileName(i32 segmentId);

    Stroka Location;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
