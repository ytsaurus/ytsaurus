#pragma once

#include "common.h"
#include "change_log.h"
#include "change_log_writer.h"

#include "../misc/cache.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TCachedChangeLog
    : public TCacheValueBase<i32, TCachedChangeLog>
{
public:
    TCachedChangeLog(TChangeLog* changeLog);

    TChangeLog::TPtr GetChangeLog() const;
    TChangeLogWriter& GetWriter();

private:
    TChangeLog::TPtr ChangeLog;
    TChangeLogWriter Writer;
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
