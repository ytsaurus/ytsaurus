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
using namespace NHydra::NProto;

////////////////////////////////////////////////////////////////////////////////

class TLocalSnapshotReader
    : public ISnapshotReader
{
public:
    TLocalSnapshotReader(
        TDistributedHydraManagerConfigPtr config,
        TCellManagerPtr cellManager,
        TFileSnapshotStorePtr fileStore,
        int snapshotId)
        : Config_(config)
        , CellManager_(cellManager)
        , FileStore_(fileStore)
        , SnapshotId_(snapshotId)
    { }

    virtual TFuture<void> Open() override
    {
        return BIND(&TLocalSnapshotReader::DoOpen, MakeStrong(this))
            .AsyncVia(GetHydraIOInvoker())
            .Run();
    }

    virtual TFuture<size_t> Read(void* buf, size_t len) override
    {
        return UnderlyingReader_->Read(buf, len);
    }

    virtual TSnapshotParams GetParams() const override
    {
        return UnderlyingReader_->GetParams();
    }

private:
    const TDistributedHydraManagerConfigPtr Config_;
    const TCellManagerPtr CellManager_;
    const TFileSnapshotStorePtr FileStore_;
    const int SnapshotId_;

    ISnapshotReaderPtr UnderlyingReader_;


    void DoOpen()
    {
        if (!FileStore_->CheckSnapshotExists(SnapshotId_)) {
            auto asyncResult = DownloadSnapshot(
                Config_,
                CellManager_,
                FileStore_,
                SnapshotId_);
            WaitFor(asyncResult)
                .ThrowOnError();
        }

        UnderlyingReader_ = FileStore_->CreateReader(SnapshotId_);

        WaitFor(UnderlyingReader_->Open())
            .ThrowOnError();
    }

};

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

    virtual ISnapshotReaderPtr CreateReader(int snapshotId) override
    {
        return New<TLocalSnapshotReader>(
            Config_,
            CellManager_,
            FileStore_,
            snapshotId);
    }

    virtual ISnapshotWriterPtr CreateWriter(int snapshotId, const TSnapshotMeta& meta) override
    {
        return FileStore_->CreateWriter(snapshotId, meta);
    }

    virtual TFuture<int> GetLatestSnapshotId(int maxSnapshotId) override
    {
        return BIND(&TLocalSnapshotStore::DoGetLatestSnapshotId, MakeStrong(this))
            .AsyncVia(GetHydraIOInvoker())
            .Run(maxSnapshotId);
    }

private:
    const TDistributedHydraManagerConfigPtr Config_;
    const TCellManagerPtr CellManager_;
    const TFileSnapshotStorePtr FileStore_;


    int DoGetLatestSnapshotId(int maxSnapshotId)
    {
        auto remoteSnapshotInfo = WaitFor(DiscoverLatestSnapshot(
            Config_,
            CellManager_,
            maxSnapshotId)).ValueOrThrow();
        int localSnapshotId = FileStore_->GetLatestSnapshotId(maxSnapshotId);
        return std::max(localSnapshotId, remoteSnapshotInfo.SnapshotId);
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
