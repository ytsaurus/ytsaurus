#include "stdafx.h"
#include "common.h"
#include "snapshot_store.h"

#include <ytlib/misc/fs.h>

namespace NYT {
namespace NMetaState {

using namespace NFS;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;
static const char* const SnapshotExtension = "snapshot";

////////////////////////////////////////////////////////////////////////////////

TSnapshotStore::TSnapshotStore(const Stroka& path)
    : Path(path)
    , CachedMaxSnapshotId(NonexistingSnapshotId)
{ }

Stroka TSnapshotStore::GetSnapshotFileName(i32 snapshotId) const
{
    return CombinePaths(Path, Sprintf("%09d.%s", snapshotId, SnapshotExtension));
}

TSnapshotStore::TGetReaderResult TSnapshotStore::GetReader(i32 snapshotId) const
{
    YASSERT(snapshotId > 0);
    Stroka fileName = GetSnapshotFileName(snapshotId);
    if (!isexist(~fileName)) {
        return TError(
            EErrorCode::NoSuchSnapshot,
            Sprintf("No such snapshot (SnapshotId: %d)", snapshotId));
    }
    return New<TSnapshotReader>(fileName, snapshotId);
}

TSnapshotWriter::TPtr TSnapshotStore::GetWriter(i32 snapshotId) const
{
    YASSERT(snapshotId > 0);
    Stroka fileName = GetSnapshotFileName(snapshotId);
    return New<TSnapshotWriter>(fileName, snapshotId);
}

TSnapshotStore::TGetRawReaderResult TSnapshotStore::GetRawReader(int snapshotId) const
{
    YASSERT(snapshotId > 0);
    Stroka fileName = GetSnapshotFileName(snapshotId);
    if (!isexist(~fileName)) {
        return TError(
            EErrorCode::NoSuchSnapshot,
            Sprintf("No such snapshot (SnapshotId: %d)", snapshotId));
    }
    return TSharedPtr<TFile>(new TFile(fileName, OpenExisting | RdOnly));
}

TSharedPtr<TFile> TSnapshotStore::GetRawWriter(int snapshotId) const
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
    LOG_DEBUG("Looking for snapshots in %s", ~Path.Quote());

    TFileList fileList;
    fileList.Fill(Path);

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
