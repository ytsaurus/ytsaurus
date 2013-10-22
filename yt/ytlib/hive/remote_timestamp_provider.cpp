#include "stdafx.h"
#include "timestamp_provider.h"
#include "remote_timestamp_provider.h"
#include "timestamp_service_proxy.h"
#include "config.h"

#include <core/concurrency/thread_affinity.h>

namespace NYT {
namespace NHive {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TRemoteTimestampProvider
    : public ITimestampProvider
{
public:
    TRemoteTimestampProvider(
        TRemoteTimestampProviderConfigPtr config,
        IChannelPtr channel)
        : Config(config)
        , Channel(channel)
        , LatestTimestamp(NullTimestamp)
    { }

    virtual TFuture<TErrorOr<TTimestamp>> GenerateNewTimestamp() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TGuard<TSpinLock> guard(Spinlock);
        if (!NewTimestamp) {
            NewTimestamp = NewPromise<TErrorOr<TTimestamp>>();

            TTimestampServiceProxy proxy(Channel);
            proxy.SetDefaultTimeout(Config->RpcTimeout);
            
            auto req = proxy.GetTimestamp();
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

ITimestampProviderPtr CreateRemoteTimestampProvider(
    TRemoteTimestampProviderConfigPtr config,
    IChannelPtr channel)
{
    return New<TRemoteTimestampProvider>(config, channel);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT

