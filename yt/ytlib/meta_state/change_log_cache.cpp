#include "change_log_cache.h"
#include "meta_state_manager.h"

#include <util/folder/dirut.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;
static const char LogExtension[] = "log";

////////////////////////////////////////////////////////////////////////////////

TChangeLogCache::TChangeLogCache(Stroka location)
    : TCapacityLimitedCache<i32, TCachedAsyncChangeLog>(4) // TODO: introduce config
    , Location(location)
{ }

Stroka TChangeLogCache::GetChangeLogFileName(i32 segmentId)
{
    return Location + "/" + Sprintf("%09d", segmentId) + "." + LogExtension;
}

TCachedAsyncChangeLog::TPtr TChangeLogCache::Get(i32 segmentId)
{
    TInsertCookie cookie(segmentId);
    if (BeginInsert(&cookie)) {
        Stroka fileName = GetChangeLogFileName(segmentId);
        if (!isexist(~fileName)) {
            return NULL;
        }

        TChangeLog::TPtr changeLog = New<TChangeLog>(fileName, segmentId);

        try {
            changeLog->Open();
        } catch (const yexception& ex) {
            LOG_ERROR("Could not open changelog %d: %s",
                segmentId,
                ex.what());
            return NULL;
        }

        EndInsert(New<TCachedAsyncChangeLog>(changeLog), &cookie);
    }

    return cookie.GetAsyncResult()->Get();
}

TCachedAsyncChangeLog::TPtr TChangeLogCache::Create(
    i32 segmentId, i32 prevRecordCount)
{
    TInsertCookie cookie(segmentId);
    if (!BeginInsert(&cookie)) {
        LOG_FATAL("Trying to create an already existing changelog %d",
            segmentId);
    }

    Stroka fileName = GetChangeLogFileName(segmentId);
    TChangeLog::TPtr changeLog = New<TChangeLog>(fileName, segmentId);

    try {
        changeLog->Create(prevRecordCount);
    } catch (const yexception& ex) {
        LOG_FATAL("Could not create changelog %d: %s",
            segmentId,
            ex.what());
    }

    EndInsert(New<TCachedAsyncChangeLog>(changeLog), &cookie);

    return cookie.GetAsyncResult()->Get();
}

void TChangeLogCache::OnTrim(TValuePtr value)
{
    if (!value->IsFinalized()) {
        LOG_WARNING("Trimming a non-finalized changelog %d", value->GetId());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
