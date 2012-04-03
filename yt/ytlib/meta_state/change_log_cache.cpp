#include "stdafx.h"
#include "change_log_cache.h"
#include "common.h"
#include "meta_state_manager.h"
#include "change_log.h"
#include "config.h"

#include <ytlib/misc/fs.h>

#include <util/folder/dirut.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;
static const char* LogExtension = "log";

////////////////////////////////////////////////////////////////////////////////

TCachedAsyncChangeLog::TCachedAsyncChangeLog(TChangeLog* changeLog)
    : TCacheValueBase<i32, TCachedAsyncChangeLog>(changeLog->GetId())
    , TAsyncChangeLog(changeLog)
{ }

////////////////////////////////////////////////////////////////////////////////

TChangeLogCache::TChangeLogCache(
    const Stroka& path,
    TChangeLogCacheConfigPtr config)
    : TSizeLimitedCache<i32, TCachedAsyncChangeLog>(config->MaxSize)
    , Config(config)
    , Path(path)
{ }

void TChangeLogCache::Start()
{
    LOG_DEBUG("Preparing changelog directory %s", ~Path.Quote());

    NFS::ForcePath(Path);
    NFS::CleanTempFiles(Path);
}

Stroka TChangeLogCache::GetChangeLogFileName(i32 id)
{
    return NFS::CombinePaths(Path, Sprintf("%09d.%s", id, LogExtension));
}

TChangeLogPtr TChangeLogCache::CreateChangeLog(i32 id)
{
    return New<TChangeLog>(
        GetChangeLogFileName(id),
        id,
        Config->DisableFlush);
}

TChangeLogCache::TGetResult TChangeLogCache::Get(i32 id)
{
    TInsertCookie cookie(id);
    if (BeginInsert(&cookie)) {
        auto fileName = GetChangeLogFileName(id);
        if (!isexist(~fileName)) {
            cookie.Cancel(TError(
                EErrorCode::NoSuchChangeLog,
                Sprintf("No such changelog (ChangeLogId: %d)", id)));
        } else {
            try {
                auto changeLog = CreateChangeLog(id);
                changeLog->Open();
                cookie.EndInsert(New<TCachedAsyncChangeLog>(~changeLog));
            } catch (const std::exception& ex) {
                LOG_FATAL("Error opening changelog (ChangeLogId: %d)\n%s",
                    id,
                    ex.what());
            }
        }
    }
    return cookie.GetAsyncResult()->Get();
}

TCachedAsyncChangeLogPtr TChangeLogCache::Create(
    i32 id,
    i32 prevRecordCount)
{
    TInsertCookie cookie(id);
    if (!BeginInsert(&cookie)) {
        LOG_FATAL("Trying to create an already existing changelog (ChangeLogId: %d)",
            id);
    }

    auto fileName = GetChangeLogFileName(id);

    try {
        auto changeLog = New<TChangeLog>(fileName, id, Config->DisableFlush);
        changeLog->Create(prevRecordCount);
        cookie.EndInsert(New<TCachedAsyncChangeLog>(~changeLog));
    } catch (const std::exception& ex) {
        LOG_FATAL("Error creating changelog (ChangeLogId: %d)\n%s",
            id,
            ex.what());
    }

    return cookie.GetAsyncResult()->Get().Value();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
