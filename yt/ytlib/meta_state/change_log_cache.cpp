#include "stdafx.h"
#include "change_log_cache.h"
#include "meta_state_manager.h"

#include <util/folder/dirut.h>

namespace NYT {
namespace NMetaState {

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

        try {
            auto changeLog = New<TChangeLog>(fileName, segmentId);
            changeLog->Open();
            cookie.EndInsert(New<TCachedAsyncChangeLog>(changeLog));
        } catch (...) {
            LOG_ERROR("Error opening changelog (SegmentId: %d, What: %s)",
                segmentId,
                ~CurrentExceptionMessage());
            return NULL;
        }
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

    try {
        auto changeLog = New<TChangeLog>(fileName, segmentId);
        changeLog->Create(prevRecordCount);
        cookie.EndInsert(New<TCachedAsyncChangeLog>(changeLog));
    } catch (...) {
        LOG_ERROR("Error creating changelog (SegmentId: %d, What: %s)",
            segmentId,
            ~CurrentExceptionMessage());
    }

    return cookie.GetAsyncResult()->Get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
