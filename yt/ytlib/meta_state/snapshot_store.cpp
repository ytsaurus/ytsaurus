#include "stdafx.h"
#include "common.h"
#include "snapshot_store.h"

#include "../misc/fs.h"

#include <util/folder/dirut.h>
#include <util/folder/filelist.h>
#include <util/string/cast.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;

////////////////////////////////////////////////////////////////////////////////

static const char* const SnapshotExtension = "snapshot";

TSnapshotStore::TSnapshotStore(Stroka location)
    : Location(location)
    , CachedMaxSnapshotId(NonexistingSnapshotId)
{ }

Stroka TSnapshotStore::GetSnapshotFileName(i32 snapshotId) const
{
    return
        Location + "/" +
        Sprintf("%09d", snapshotId) + "." +
        SnapshotExtension;
}

TSnapshotReader::TPtr TSnapshotStore::GetReader(i32 snapshotId) const
{
    YASSERT(snapshotId > 0);
    Stroka fileName = GetSnapshotFileName(snapshotId);
    if (!isexist(~fileName))
        return NULL;
    return New<TSnapshotReader>(fileName, snapshotId);
}

TSnapshotWriter::TPtr TSnapshotStore::GetWriter(i32 snapshotId) const
{
    YASSERT(snapshotId > 0);
    Stroka fileName = GetSnapshotFileName(snapshotId);
    return New<TSnapshotWriter>(fileName, snapshotId);
}

TAutoPtr<TFile> TSnapshotStore::GetRawReader(int snapshotId) const
{
    YASSERT(snapshotId > 0);
    Stroka fileName = GetSnapshotFileName(snapshotId);
    if (!isexist(~fileName))
        return NULL;
    return new TFile(fileName, OpenExisting | RdOnly);
}

TAutoPtr<TFile> TSnapshotStore::GetRawWriter(int snapshotId) const
{
    YASSERT(snapshotId > 0);
    Stroka fileName = GetSnapshotFileName(snapshotId);
    return new TFile(fileName, CreateAlways | WrOnly | Seq);
}

i32 TSnapshotStore::GetMaxSnapshotId() const
{
    // Check for a cached value first.
    if (CachedMaxSnapshotId != NonexistingSnapshotId &&
        isexist(~GetSnapshotFileName(CachedMaxSnapshotId)))
    {
        LOG_DEBUG("Cached maximum snapshot id is %d", CachedMaxSnapshotId);
        return CachedMaxSnapshotId;
    }

    // Look for snapshots.
    CachedMaxSnapshotId = NonexistingSnapshotId;
    LOG_DEBUG("Looking for snapshots in %s", ~Location.Quote());

    TFileList fileList;
    fileList.Fill(Location);

    i32 maxSnapshotId = NonexistingSnapshotId;
    Stroka fileName;
    while ((fileName = fileList.Next()) != NULL) {
        Stroka extension = NFS::GetFileExtension(fileName);
        if (extension == SnapshotExtension) {
            Stroka name = NFS::GetFileNameWithoutExtension(fileName);
            try {
                i32 segmentId = FromString<i32>(name);
                LOG_DEBUG("Found snapshot %d", segmentId);
                maxSnapshotId = Max(maxSnapshotId, segmentId);
            } catch (const yexception&) {
                LOG_WARNING("Found unrecognized file %s", ~fileName.Quote());
            }
        }
    }

    if (maxSnapshotId < 0) {
        LOG_DEBUG("No snapshots found");
    } else {
        LOG_DEBUG("Maximum snapshot id is %d", maxSnapshotId);
    }

    CachedMaxSnapshotId = maxSnapshotId;
    return maxSnapshotId;
}

void TSnapshotStore::UpdateMaxSnapshotId(int snapshotId)
{
    CachedMaxSnapshotId = Max(CachedMaxSnapshotId, snapshotId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
