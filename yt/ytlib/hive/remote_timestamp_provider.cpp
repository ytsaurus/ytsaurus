#include "stdafx.h"
#include "timestamp_provider.h"
#include "remote_timestamp_provider.h"
#include "timestamp_service_proxy.h"
#include "config.h"

#include <core/concurrency/thread_affinity.h>

#include <ytlib/hydra/peer_channel.h>

namespace NYT {
namespace NHive {

using namespace NRpc;
using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

class TRemoteTimestampProvider
    : public ITimestampProvider
{
public:
    explicit TRemoteTimestampProvider(TRemoteTimestampProviderConfigPtr config)
        : Config(config)
        , Channel(CreatePeerChannel(config, EPeerRole::Leader))
        , Proxy(Channel)
        , LatestTimestamp(NullTimestamp)
    { }

    virtual TFuture<TErrorOr<TTimestamp>> GenerateNewTimestamp() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TGuard<TSpinLock> guard(Spinlock);
        if (!NewTimestamp) {
            NewTimestamp = NewPromise<TErrorOr<TTimestamp>>();
            auto req = Proxy.GetTimestamp();
            req->Invoke().Subscribe(
                BIND(&TRemoteTimestampProvider::OnGetTimestampResponse, MakeStrong(this)));
        }
        return NewTimestamp;
    }

    virtual TTimestamp GetLatestTimestamp() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return LatestTimestamp;
    }


private:
    TRemoteTimestampProviderConfigPtr Config;

    IChannelPtr Channel;
    TTimestampServiceProxy Proxy;

    TSpinLock Spinlock;
    TTimestamp LatestTimestamp;
    TPromise<TErrorOr<TTimestamp>> NewTimestamp;


    void OnGetTimestampResponse(TTimestampServiceProxy::TRspGetTimestampPtr rsp)
    {
        TGuard<TSpinLock> guard(Spinlock);
        if (rsp->IsOK()) {
            NewTimestamp.Set(rsp->timestamp());
        } else {
            NewTimestamp.Set(rsp->GetError());
        }
    }

};

ITimestampProviderPtr CreateRemoteTimestampProvider(TRemoteTimestampProviderConfigPtr config)
{
    return New<TRemoteTimestampProvider>(config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT

