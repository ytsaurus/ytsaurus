#include "stdafx.h"
#include "change_log_cache.h"
#include "meta_state_manager.h"

#include <ytlib/misc/fs.h>

#include <util/folder/dirut.h>

namespace NYT {
namespace NMetaState {

using namespace NFS;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;
static const char* LogExtension = "log";

////////////////////////////////////////////////////////////////////////////////

TChangeLogCache::TChangeLogCache(const Stroka& path)
    // TODO: introduce config
    : TSizeLimitedCache<i32, TCachedAsyncChangeLog>(4)
    , Path(path)
{ }

Stroka TChangeLogCache::GetChangeLogFileName(i32 changeLogId)
{
    return CombinePaths(Path, Sprintf("%09d.%s", changeLogId, LogExtension));
}

TChangeLogCache::TGetResult TChangeLogCache::Get(i32 changeLogId)
{
    TInsertCookie cookie(changeLogId);
    if (BeginInsert(&cookie)) {
        auto fileName = GetChangeLogFileName(changeLogId);
        if (!isexist(~fileName)) {
            cookie.Cancel(TError(
                EErrorCode::NoSuchChangeLog,
                Sprintf("No such changelog (ChangeLogId: %d)", changeLogId)));
        } else {
            try {
                auto changeLog = New<TChangeLog>(fileName, changeLogId);
                changeLog->Open();
                cookie.EndInsert(New<TCachedAsyncChangeLog>(~changeLog));
            } catch (...) {
                LOG_FATAL("Error opening changelog (ChangeLogId: %d)\n%s",
                    changeLogId,
                    ~CurrentExceptionMessage());
            }
        }
    }
    return cookie.GetAsyncResult()->Get();
}

TCachedAsyncChangeLog::TPtr TChangeLogCache::Create(
    i32 changeLogId,
    i32 prevRecordCount)
{
    TInsertCookie cookie(changeLogId);
    if (!BeginInsert(&cookie)) {
        LOG_FATAL("Trying to create an already existing changelog (ChangeLogId: %d)",
            changeLogId);
    }

    auto fileName = GetChangeLogFileName(changeLogId);

    try {
        auto changeLog = New<TChangeLog>(fileName, changeLogId);
        changeLog->Create(prevRecordCount);
        cookie.EndInsert(New<TCachedAsyncChangeLog>(~changeLog));
    } catch (...) {
        LOG_FATAL("Error creating changelog (ChangeLogId: %d)\n%s",
            changeLogId,
            ~CurrentExceptionMessage());
    }

    return cookie.GetAsyncResult()->Get().Value();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
