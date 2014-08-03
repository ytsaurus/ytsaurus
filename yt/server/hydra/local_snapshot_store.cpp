#include "stdafx.h"
#include "local_snapshot_store.h"
#include "snapshot.h"
#include "file_snapshot_store.h"
#include "snapshot_discovery.h"
#include "snapshot_download.h"
#include "config.h"
#include "private.h"

#include <core/concurrency/scheduler.h>

#include <ytlib/election/cell_manager.h>

namespace NYT {
namespace NHydra {

using namespace NElection;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TLocalSnapshotStore
    : public ISnapshotStore
{
public:
    TLocalSnapshotStore(
        TDistributedHydraManagerConfigPtr config,
        TCellManagerPtr cellManager,
        TFileSnapshotStorePtr fileStore)
        : Config_(config)
        , CellManager_(cellManager)
        , FileStore_(fileStore)
    { }

    virtual TFuture<TErrorOr<ISnapshotReaderPtr>> CreateReader(int snapshotId) override
    {
        return BIND(&TLocalSnapshotStore::DoCreateReader, MakeStrong(this))
            .Guarded()
            .AsyncVia(GetHydraIOInvoker())
            .Run(snapshotId);
    }

    virtual ISnapshotWriterPtr CreateWriter(int snapshotId, const TSharedRef& meta) override
    {
        return FileStore_->CreateWriter(snapshotId, meta);
    }

    virtual TFuture<TErrorOr<int>> GetLatestSnapshotId(int maxSnapshotId) override
    {
        return BIND(&TLocalSnapshotStore::DoGetLatestSnapshotId, MakeStrong(this))
            .Guarded()
            .AsyncVia(GetHydraIOInvoker())
            .Run(maxSnapshotId);
    }

    virtual TFuture<TErrorOr<TSnapshotParams>> ConfirmSnapshot(int snapshotId) override
    {
        return BIND(&TLocalSnapshotStore::DoConfirmSnapshot, MakeStrong(this))
            .Guarded()
            .AsyncVia(GetHydraIOInvoker())
            .Run(snapshotId);
    }

private:
    TDistributedHydraManagerConfigPtr Config_;
    TCellManagerPtr CellManager_;
    TFileSnapshotStorePtr FileStore_;


    ISnapshotReaderPtr DoCreateReader(int snapshotId)
    {
        if (!FileStore_->CheckSnapshotExists(snapshotId)) {
            auto downloadResult = WaitFor(DownloadSnapshot(
                Config_,
                CellManager_,
                FileStore_,
                snapshotId));
            THROW_ERROR_EXCEPTION_IF_FAILED(downloadResult);
        }
        return FileStore_->CreateReader(snapshotId);
    }

    int DoGetLatestSnapshotId(int maxSnapshotId)
    {
        auto remoteSnapshotInfo = WaitFor(DiscoverLatestSnapshot(Config_, CellManager_, maxSnapshotId));
        int localSnnapshotId = FileStore_->GetLatestSnapshotId(maxSnapshotId);
        return std::max(localSnnapshotId, remoteSnapshotInfo.SnapshotId);
    }

    TSnapshotParams DoConfirmSnapshot(int snapshotId)
    {
        FileStore_->ConfirmSnapshot(snapshotId);
        
        auto reader = FileStore_->CreateReader(snapshotId);
        return reader->GetParams();
    }

};

ISnapshotStorePtr CreateLocalSnapshotStore(
    TDistributedHydraManagerConfigPtr config,
    TCellManagerPtr cellManager,
    TFileSnapshotStorePtr fileStore)
{
    return New<TLocalSnapshotStore>(
        config,
        cellManager,
        fileStore);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
