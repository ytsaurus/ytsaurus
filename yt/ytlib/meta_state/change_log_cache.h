#pragma once

#include "common.h"
#include "change_log.h"
#include "async_change_log.h"

#include "../misc/cache.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TCachedAsyncChangeLog
    : public TCacheValueBase<i32, TCachedAsyncChangeLog>
    , public TAsyncChangeLog
{
public:
    TCachedAsyncChangeLog(TChangeLog::TPtr changeLog)
        : TCacheValueBase<i32, TCachedAsyncChangeLog>(changeLog->GetId())
        , TAsyncChangeLog(changeLog)
    { }
};

////////////////////////////////////////////////////////////////////////////////

class TChangeLogCache
    : public TCapacityLimitedCache<i32, TCachedAsyncChangeLog>
{
public:
    typedef TIntrusivePtr<TChangeLogCache> TPtr;

    TChangeLogCache(Stroka location);

    TCachedAsyncChangeLog::TPtr Get(i32 segmentId);
    TCachedAsyncChangeLog::TPtr Create(i32 segmentId, i32 prevRecordCount);

protected:
    virtual void OnTrim(TValuePtr value);

private:
    Stroka GetChangeLogFileName(i32 segmentId);

    Stroka Location;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
