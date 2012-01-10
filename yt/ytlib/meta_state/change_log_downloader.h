#pragma once

#include "common.h"
#include "meta_state.h"
#include "decorated_meta_state.h"
#include "meta_state_manager_proxy.h"
#include "cell_manager.h"
#include "async_change_log.h"

#include "../rpc/client.h"
#include "../actions/parallel_awaiter.h"

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
                .Default(TDuration::Seconds(5));
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
        TCellManager::TPtr cellManager);

    EResult Download(TMetaVersion version, TAsyncChangeLog& changeLog);

private:
    typedef TMetaStateManagerProxy TProxy;
    typedef TProxy::EErrorCode EErrorCode;

    TConfig::TPtr Config;
    TCellManager::TPtr CellManager;

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
