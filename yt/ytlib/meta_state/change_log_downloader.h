#pragma once

#include "private.h"
#include "meta_state_manager_proxy.h"

#include <ytlib/rpc/client.h>
#include <ytlib/actions/parallel_awaiter.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

class TChangeLogDownloader
    : private TNonCopyable
{
public:
    DECLARE_ENUM(EResult,
        (OK)
        (ChangeLogNotFound)
        (ChangeLogUnavailable)
        (RemoteError)
    );

    TChangeLogDownloader(
        TChangeLogDownloaderConfigPtr config,
        NElection::TCellManagerPtr cellManager,
        IInvokerPtr controlInvoker);

    EResult Download(TMetaVersion version, TAsyncChangeLog& changeLog);

private:
    typedef TMetaStateManagerProxy TProxy;
    typedef TProxy::EErrorCode EErrorCode;

    TChangeLogDownloaderConfigPtr Config;
    NElection::TCellManagerPtr CellManager;
    IInvokerPtr ControlInvoker;

    TPeerId GetChangeLogSource(TMetaVersion version);

    EResult DownloadChangeLog(
        TMetaVersion version,
        TPeerId sourceId,
        TAsyncChangeLog& changeLog);

    static void OnResponse(
        TParallelAwaiterPtr awaiter,
        TPromise<TPeerId> promise,
        TPeerId peerId,
        TMetaVersion version,
        TProxy::TRspGetChangeLogInfoPtr response);
    static void OnComplete(
        TPromise<TPeerId> promise);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
