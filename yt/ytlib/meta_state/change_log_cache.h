#pragma once

#include "public.h"
#include "async_change_log.h"
#include "meta_state_manager_proxy.h"

#include <ytlib/misc/cache.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

class TCachedAsyncChangeLog
    : public TCacheValueBase<i32, TCachedAsyncChangeLog>
    , public TAsyncChangeLog
{
public:
    TCachedAsyncChangeLog(TChangeLog* changeLog);

};

////////////////////////////////////////////////////////////////////////////////

class TChangeLogCache
    : public TSizeLimitedCache<i32, TCachedAsyncChangeLog>
{
public:
    typedef TMetaStateManagerProxy::EErrorCode EErrorCode;

    TChangeLogCache(const Stroka& path);

    void Start();

    typedef TValueOrError<TCachedAsyncChangeLogPtr> TGetResult;
    TGetResult Get(i32 changeLogId);

    TCachedAsyncChangeLogPtr Create(i32 changeLogId, i32 prevRecordCount);

private:
    Stroka Path;

    Stroka GetChangeLogFileName(i32 changeLogId);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
