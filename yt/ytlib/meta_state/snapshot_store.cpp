#include "stdafx.h"
#include "common.h"
#include "snapshot_store.h"
#include "snapshot.h"

#include <ytlib/misc/fs.h>
#include <ytlib/misc/thread_affinity.h>

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

void TSnapshotStore::Start()
{
    LOG_DEBUG("Preparing snapshot directory %s", ~Path.Quote());

    NFS::ForcePath(Path);
    NFS::CleanTempFiles(Path);

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

    if (maxSnapshotId == NonexistingSnapshotId) {
        LOG_DEBUG("No snapshots found");
    } else {
        LOG_DEBUG("Maximum snapshot id is %d", maxSnapshotId);
    }

    CachedMaxSnapshotId = maxSnapshotId;
}

Stroka TSnapshotStore::GetSnapshotFileName(i32 snapshotId) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return CombinePaths(Path, Sprintf("%09d.%s", snapshotId, SnapshotExtension));
}

TSnapshotStore::TGetReaderResult TSnapshotStore::GetReader(i32 snapshotId) const
{
    VERIFY_THREAD_AFFINITY_ANY();
    YASSERT(snapshotId > 0);

    Stroka fileName = GetSnapshotFileName(snapshotId);
    if (!isexist(~fileName)) {
        return TError(
            EErrorCode::NoSuchSnapshot,
            Sprintf("No such snapshot (SnapshotId: %d)", snapshotId));
    }
    return New<TSnapshotReader>(fileName, snapshotId);
}

TSnapshotWriterPtr TSnapshotStore::GetWriter(i32 snapshotId) const
{
    VERIFY_THREAD_AFFINITY_ANY();
    YASSERT(snapshotId > 0);

    Stroka fileName = GetSnapshotFileName(snapshotId);
    return New<TSnapshotWriter>(fileName, snapshotId);
}

TSnapshotStore::TGetRawReaderResult TSnapshotStore::GetRawReader(int snapshotId) const
{
    VERIFY_THREAD_AFFINITY_ANY();
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
    VERIFY_THREAD_AFFINITY_ANY();
    YASSERT(snapshotId > 0);

    Stroka fileName = GetSnapshotFileName(snapshotId);
    return new TFile(fileName, CreateAlways | WrOnly | Seq);
}

i32 TSnapshotStore::GetMaxSnapshotId() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(SpinLock);
    return CachedMaxSnapshotId;
}

void TSnapshotStore::UpdateMaxSnapshotId(int snapshotId)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(SpinLock);
    CachedMaxSnapshotId = Max(CachedMaxSnapshotId, snapshotId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
