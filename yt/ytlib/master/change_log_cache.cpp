#include "change_log_cache.h"
#include "master_state_manager.h"

#include "../logging/log.h"

#include <util/folder/dirut.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("ChangeLogCache");
static const char* const LogExtension = "log";

////////////////////////////////////////////////////////////////////////////////

TCachedChangeLog::TCachedChangeLog(TChangeLog* changeLog)
    : TCacheValueBase<i32, TCachedChangeLog>(changeLog->GetId())
    , ChangeLog(changeLog)
    , Writer(changeLog)
{ }

TChangeLog::TPtr TCachedChangeLog::GetChangeLog() const
{
    return ChangeLog;
}

TAsyncChangeLog& TCachedChangeLog::GetWriter()
{
    return Writer;
}

////////////////////////////////////////////////////////////////////////////////

TChangeLogCache::TChangeLogCache(Stroka location)
    : TCapacityLimitedCache<i32, TCachedChangeLog>(4) // TODO: introduce config
    , Location(location)
{ }

Stroka TChangeLogCache::GetChangeLogFileName(i32 segmentId)
{
    return Location + "/" +
           Sprintf("%09d", segmentId) + "." +
           LogExtension;
}

TCachedChangeLog::TPtr TChangeLogCache::Get(i32 segmentId)
{
    TInsertCookie cookie(segmentId);
    if (BeginInsert(&cookie)) {
        Stroka fileName = GetChangeLogFileName(segmentId);
        if (!isexist(~fileName)) {
            return NULL;
        }
        THolder<TChangeLog> changeLog(new TChangeLog(fileName, segmentId));
        try {
            changeLog->Open();
        } catch (const yexception& ex) {
            LOG_ERROR("Could not open changelog %d: %s",
                segmentId, ex.what());
            return NULL;
        }
        EndInsert(new TCachedChangeLog(changeLog.Release()), &cookie);
    }
    return cookie.GetAsyncResult()->Get();
}

TCachedChangeLog::TPtr TChangeLogCache::Create(
    i32 segmentId, i32 prevRecordCount)
{
    TInsertCookie cookie(segmentId);
    if (!BeginInsert(&cookie)) {
        LOG_FATAL("Trying to create an already existing changelog %d",
            segmentId);
    }
    Stroka fileName = GetChangeLogFileName(segmentId);
    THolder<TChangeLog> changeLog(new TChangeLog(fileName, segmentId));
    try {
        changeLog->Create(prevRecordCount);
    } catch (const yexception& ex) {
        LOG_FATAL("Could not create changelog %d: %s",
            segmentId, ex.what());
    }
    EndInsert(new TCachedChangeLog(changeLog.Release()), &cookie);
    return cookie.GetAsyncResult()->Get();
}

void TChangeLogCache::OnTrim(TValuePtr value)
{
    if (!value->GetChangeLog()->IsFinalized()) {
        LOG_WARNING("Trimming a non-finalized changelog %d",
                      value->GetChangeLog()->GetId());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
