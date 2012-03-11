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
        TSnapshotDownloaderConfig* config,
        TCellManagerPtr cellManager);

    EResult GetSnapshot(i32 segmentId, TFile* snapshotFile);

private:
    struct TSnapshotInfo
    {
        TPeerId SourceId;
        i64 Length;
        
        TSnapshotInfo() {}

        TSnapshotInfo(TPeerId owner, i64 length)
            : SourceId(owner)
            , Length(length)
        {}
    };

    typedef TMetaStateManagerProxy TProxy;
    typedef TProxy::EErrorCode EErrorCode;

    TSnapshotDownloaderConfigPtr Config;
    TCellManagerPtr CellManager;

    TSnapshotInfo GetSnapshotInfo(i32 segmentId); // also finds snapshot source
    static void OnResponse(
        TProxy::TRspGetSnapshotInfo::TPtr response,
        TParallelAwaiter::TPtr awaiter,
        TFuture<TSnapshotInfo>::TPtr asyncResult,
        TPeerId peerId);
    static void OnComplete(
        i32 segmentId,
        TFuture<TSnapshotInfo>::TPtr asyncResult);
    EResult DownloadSnapshot(
        i32 segmentId,
        TSnapshotInfo snapshotInfo,
        TFile* snapshotFile);
    EResult WriteSnapshot(
        i32 segmentId,
        i64 snapshotLength,
        i32 sourceId,
        TOutputStream &output);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
