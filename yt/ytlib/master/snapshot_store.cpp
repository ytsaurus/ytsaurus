#include "common.h"
#include "snapshot_store.h"

#include "../misc/fs.h"
#include "../logging/log.h"

#include <util/folder/dirut.h>
#include <util/folder/filelist.h>
#include <util/string/cast.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("SnapshotStore");

////////////////////////////////////////////////////////////////////////////////

static const char* const SnapshotExtension = "snapshot";

TSnapshotStore::TSnapshotStore(Stroka location)
    : Location(location)
{ }

Stroka TSnapshotStore::GetSnapshotFileName(i32 segmentId)
{
    return Location + "/" +
           Sprintf("%09d", segmentId) + "." +
           SnapshotExtension;
}

TAutoPtr<TSnapshotReader> TSnapshotStore::GetReader(i32 segmentId)
{
    YASSERT(segmentId > 0);
    Stroka fileName = GetSnapshotFileName(segmentId);
    if (!isexist(~fileName))
        return NULL;
    return new TSnapshotReader(fileName, segmentId);
}

TAutoPtr<TSnapshotWriter> TSnapshotStore::GetWriter(i32 segmentId)
{
    YASSERT(segmentId > 0);
    Stroka fileName = GetSnapshotFileName(segmentId);
    return new TSnapshotWriter(fileName, segmentId);
}

i32 TSnapshotStore::GetMaxSnapshotId()
{
    LOG_DEBUG("Looking for snapshots in %s", ~Location);

    TFileList fileList;
    fileList.Fill(Location);

    i32 maxSnapshotId = Min<i32>();
    Stroka fileName;
    while ((fileName = fileList.Next()) != NULL) {
        Stroka extension = NFS::GetFileExtension(fileName);
        if (extension == SnapshotExtension) {
            Stroka name = NFS::GetFileNameWithoutExtension(fileName);
            try {
                i32 segmentId = FromString<i32>(name);
                LOG_INFO("Found snapshot %d", segmentId);
                maxSnapshotId = Max(maxSnapshotId, segmentId);
            } catch (const yexception&) {
                LOG_WARNING("Found unrecognized file %s", ~fileName);
            }
        }
    }

    if (maxSnapshotId < 0) {
        LOG_DEBUG("No snapshots found");
    } else {
        LOG_DEBUG("Maximum snapshot id is %d", maxSnapshotId);
    }

    return maxSnapshotId;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
