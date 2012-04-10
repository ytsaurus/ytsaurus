#pragma once

#include "public.h"
#include "meta_state_manager_proxy.h"

#include <ytlib/rpc/client.h>
#include <ytlib/actions/parallel_awaiter.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

class TSnapshotDownloader
    : private TNonCopyable
{
public:
    DECLARE_ENUM(EResult,
        (OK)
        (SnapshotNotFound)
        (SnapshotUnavailable)
        (RemoteError)
    );

    TSnapshotDownloader(
        TSnapshotDownloaderConfigPtr config,
        NElection::TCellManagerPtr cellManager);

    EResult DownloadSnapshot(i32 snapshotId, const Stroka& fileName);

private:
    struct TSnapshotInfo
    {
        TPeerId SourceId;
        i64 Length;
        
        TSnapshotInfo()
        { }

        TSnapshotInfo(TPeerId owner, i64 length)
            : SourceId(owner)
            , Length(length)
        { }
    };

    typedef TMetaStateManagerProxy TProxy;
    typedef TProxy::EErrorCode EErrorCode;

    TSnapshotDownloaderConfigPtr Config;
    NElection::TCellManagerPtr CellManager;

    TSnapshotInfo GetSnapshotInfo(i32 snapshotId); // also finds snapshot source
    static void OnSnapshotInfoResponse(
        TParallelAwaiter::TPtr awaiter,
        TPromise<TSnapshotInfo> promise,
        TPeerId peerId,
        TProxy::TRspGetSnapshotInfo::TPtr response);
    static void OnSnapshotInfoComplete(
        i32 snapshotId,
        TPromise<TSnapshotInfo> promise);

    EResult DownloadSnapshot(
        const Stroka& fileName,
        i32 snapshotId,
        const TSnapshotInfo& snapshotInfo);
    EResult WriteSnapshot(
        i32 snapshotId,
        i64 snapshotLength,
        i32 sourceId,
        TOutputStream &output);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
