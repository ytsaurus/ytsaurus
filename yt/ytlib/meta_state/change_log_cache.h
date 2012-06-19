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
    explicit TCachedAsyncChangeLog(TChangeLogPtr changeLog);

};

////////////////////////////////////////////////////////////////////////////////

class TChangeLogCache
    : public TSizeLimitedCache<i32, TCachedAsyncChangeLog>
{
public:
    typedef TMetaStateManagerProxy::EErrorCode EErrorCode;

    explicit TChangeLogCache(TChangeLogCacheConfigPtr config);

    void Start();

    typedef TValueOrError<TCachedAsyncChangeLogPtr> TGetResult;
    TGetResult Get(i32 id);

    TCachedAsyncChangeLogPtr Create(i32 id, i32 prevRecordCount, const TEpoch& epoch);

private:
    TChangeLogCacheConfigPtr Config;

    Stroka GetChangeLogFileName(i32 id);
    TChangeLogPtr CreateChangeLog(i32 id);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
