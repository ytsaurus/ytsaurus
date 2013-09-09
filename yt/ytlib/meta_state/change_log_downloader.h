#pragma once

#include "private.h"
#include "meta_state_manager_proxy.h"

#include <core/rpc/client.h>
#include <core/concurrency/parallel_awaiter.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

class TChangeLogDownloader
    : private TNonCopyable
{
public:
    TChangeLogDownloader(
        TChangeLogDownloaderConfigPtr config,
        NElection::TCellManagerPtr cellManager,
        IInvokerPtr controlInvoker);

    TError Download(
        const TMetaVersion& version,
        TAsyncChangeLog* changeLog);

private:
    typedef TMetaStateManagerProxy TProxy;

    TChangeLogDownloaderConfigPtr Config;
    NElection::TCellManagerPtr CellManager;
    IInvokerPtr ControlInvoker;

    TPeerId GetChangeLogSource(const TMetaVersion& version);

    TError DownloadChangeLog(
        const TMetaVersion& version,
        TPeerId sourceId,
        TAsyncChangeLog* changeLog);

    static void OnResponse(
        NConcurrency::TParallelAwaiterPtr awaiter,
        TPromise<TPeerId> promise,
        TPeerId peerId,
        const TMetaVersion& version,
        TProxy::TRspGetChangeLogInfoPtr response);

    static void OnComplete(
        TPromise<TPeerId> promise);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
