#pragma once

#include "common.h"
#include "meta_state_manager_proxy.h"
#include "snapshot.h"
#include "cell_manager.h"

#include "../rpc/client.h"
#include "../actions/parallel_awaiter.h"

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

class TSnapshotDownloader
    : private TNonCopyable
{
public:
    struct TConfig
        : public TConfigurable
    {
        typedef TIntrusivePtr<TConfig> TPtr;

        TDuration LookupTimeout;
        TDuration ReadTimeout;
        i32 BlockSize;

        TConfig()
            : LookupTimeout(TDuration::Seconds(2))
            , ReadTimeout(TDuration::Seconds(5))
            , BlockSize(32 * 1024 * 1024)
        {}
    };

    DECLARE_ENUM(EResult,
        (OK)
        (SnapshotNotFound)
        (SnapshotUnavailable)
        (RemoteError)
    );

    TSnapshotDownloader(
        TConfig* config,
        TCellManager::TPtr cellManager);

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

    TConfig ::TPtr Config;
    TCellManager::TPtr CellManager;

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
