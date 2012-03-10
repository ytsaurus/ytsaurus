#pragma once

#include "public.h"
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
    struct TConfig
        : public TConfigurable
    {
        typedef TIntrusivePtr<TConfig> TPtr;

        TDuration LookupTimeout;
        TDuration ReadTimeout;
        i32 RecordsPerRequest;

        TConfig()
        {
            Register("lookup_timeout", LookupTimeout)
                .GreaterThan(TDuration())
                .Default(TDuration::Seconds(5));
            Register("read_timeout", ReadTimeout)
                .GreaterThan(TDuration())
                .Default(TDuration::Seconds(10));
            Register("records_per_request", RecordsPerRequest)
                .GreaterThan(0)
                .Default(1024 * 1024);
        }
    };

    DECLARE_ENUM(EResult,
        (OK)
        (ChangeLogNotFound)
        (ChangeLogUnavailable)
        (RemoteError)
    );

    TChangeLogDownloader(
        TConfig* config,
        TCellManager* cellManager);

    EResult Download(TMetaVersion version, TAsyncChangeLog& changeLog);

private:
    typedef TMetaStateManagerProxy TProxy;
    typedef TProxy::EErrorCode EErrorCode;

    TConfig::TPtr Config;
    TCellManagerPtr CellManager;

    TPeerId GetChangeLogSource(TMetaVersion version);

    EResult DownloadChangeLog(
        TMetaVersion version,
        TPeerId sourceId,
        TAsyncChangeLog& changeLog);

    static void OnResponse(
        TProxy::TRspGetChangeLogInfo::TPtr response,
        TParallelAwaiter::TPtr awaiter,
        TFuture<TPeerId>::TPtr asyncResult,
        TPeerId peerId,
        TMetaVersion version);
    static void OnComplete(
        TFuture<TPeerId>::TPtr asyncResult);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
