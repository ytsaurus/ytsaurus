#include "change_log_cache.h"
#include "meta_state_manager.h"

#include <util/folder/dirut.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;
static const char LogExtension[] = "log";

////////////////////////////////////////////////////////////////////////////////

TChangeLogCache::TChangeLogCache(Stroka location)
    // TODO: introduce config
    : TCapacityLimitedCache<i32, TCachedAsyncChangeLog>(4)
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
        auto fileName = GetChangeLogFileName(segmentId);
        if (!isexist(~fileName)) {
            return NULL;
        }

        auto changeLog = New<TChangeLog>(fileName, segmentId);

        try {
            changeLog->Open();
        } catch (...) {
            LOG_ERROR("Error opening changelog (SegmentId: %d, What: %s)",
                segmentId,
                ~CurrentExceptionMessage());
            return NULL;
        }

        EndInsert(New<TCachedAsyncChangeLog>(changeLog), &cookie);
    }
    return cookie.GetAsyncResult()->Get();
}

TCachedAsyncChangeLog::TPtr TChangeLogCache::Create(
    i32 segmentId,
    i32 prevRecordCount)
{
    TInsertCookie cookie(segmentId);
    if (!BeginInsert(&cookie)) {
        LOG_FATAL("Trying to create an already existing changelog (SegmentId: %d)",
            segmentId);
    }

    auto fileName = GetChangeLogFileName(segmentId);
    auto changeLog = New<TChangeLog>(fileName, segmentId);

    try {
        changeLog->Create(prevRecordCount);
    } catch (...) {
        LOG_ERROR("Error creating changelog (SegmentId: %d, What: %s)",
            segmentId,
            ~CurrentExceptionMessage());
    }

    EndInsert(New<TCachedAsyncChangeLog>(changeLog), &cookie);

    return cookie.GetAsyncResult()->Get();
}

void TChangeLogCache::OnTrim(TValuePtr value)
{
    if (!value->IsFinalized()) {
        LOG_WARNING("Trimming a non-finalized changelog (SegmentId: %d)",
            value->GetId());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
