#include "stdafx.h"
#include "local_snapshot_store.h"
#include "snapshot.h"
#include "file_snapshot_store.h"
#include "snapshot_discovery.h"
#include "snapshot_download.h"
#include "config.h"
#include "private.h"

#include <core/concurrency/fiber.h>

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

    virtual const TCellGuid& GetCellGuid() const override
    {
        return FileStore_->GetCellGuid();
    }

    virtual TFuture<TErrorOr<ISnapshotReaderPtr>> CreateReader(int snapshotId) override
    {
        return BIND(&TLocalSnapshotStore::DoCreateReader, MakeStrong(this))
            .AsyncVia(GetHydraIOInvoker())
            .Run(snapshotId);
    }

    virtual ISnapshotWriterPtr CreateWriter(
        int snapshotId,
        const TSnapshotCreateParams& params) override
    {
        return FileStore_->CreateWriter(snapshotId, params);
    }

    virtual TFuture<TErrorOr<int>> GetLatestSnapshotId(int maxSnapshotId) override
    {
        return BIND(&TLocalSnapshotStore::DoGetLatestSnapshotId, MakeStrong(this))
            .AsyncVia(GetHydraIOInvoker())
            .Run(maxSnapshotId);
    }

    virtual TFuture<TErrorOr<TSnapshotParams>> ConfirmSnapshot(int snapshotId) override
    {
        return BIND(&TLocalSnapshotStore::DoConfirmSnapshot, MakeStrong(this))
            .AsyncVia(GetHydraIOInvoker())
            .Run(snapshotId);
    }

    virtual TFuture<TErrorOr<TSnapshotParams>> GetSnapshotParams(int snapshotId) override
    {
        return BIND(&TLocalSnapshotStore::DoGetSnapshotParams, MakeStrong(this))
            .AsyncVia(GetHydraIOInvoker())
            .Run(snapshotId);
    }

private:
    TDistributedHydraManagerConfigPtr Config_;
    TCellManagerPtr CellManager_;
    TFileSnapshotStorePtr FileStore_;


    TErrorOr<ISnapshotReaderPtr> DoCreateReader(int snapshotId)
    {
        try {
            auto maybeParams = FileStore_->FindSnapshotParams(snapshotId);
            if (!maybeParams) {
                auto downloadResult = WaitFor(DownloadSnapshot(
                    Config_,
                    CellManager_,
                    FileStore_,
                    snapshotId));
                THROW_ERROR_EXCEPTION_IF_FAILED(downloadResult);
            }
            return FileStore_->CreateReader(snapshotId);
        } catch (const std::exception& ex) {
            return ex;
        }
    }

    TErrorOr<int> DoGetLatestSnapshotId(int maxSnapshotId)
    {
        auto snapshotInfo = WaitFor(DiscoverLatestSnapshot(Config_, CellManager_, maxSnapshotId));
        return snapshotInfo.SnapshotId;
    }

    TErrorOr<TSnapshotParams> DoConfirmSnapshot(int snapshotId)
    {
        try {
            return FileStore_->ConfirmSnapshot(snapshotId);
        } catch (const std::exception& ex) {
            return ex;
        }
    }

    TErrorOr<TSnapshotParams> DoGetSnapshotParams(int snapshotId)
    {
        try {
            auto maybeParams = FileStore_->FindSnapshotParams(snapshotId);
            if (!maybeParams) {
                THROW_ERROR_EXCEPTION("No such snapshot %d", snapshotId);
            }
            return *maybeParams;
        } catch (const std::exception& ex) {
            return ex;
        }
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
