#include "stdafx.h"
#include "timestamp_provider.h"
#include "remote_timestamp_provider.h"
#include "timestamp_service_proxy.h"
#include "config.h"

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
    { }

    virtual TFuture<TErrorOr<TTimestamp>> GetTimestamp() override
    {
        TGuard<TSpinLock> guard(Spinlock);
        if (!Promise) {
            Promise = NewPromise<TErrorOr<TTimestamp>>();

            TTimestampServiceProxy proxy(Channel);
            proxy.SetDefaultTimeout(Config->RpcTimeout);
            
            auto req = proxy.GetTimestamp();
            req->Invoke().Subscribe(
                BIND(&TRemoteTimestampProvider::OnGetTimestampResponse, MakeStrong(this)));
        }
        return Promise;
    }


private:
    TRemoteTimestampProviderConfigPtr Config;
    IChannelPtr Channel;

    TSpinLock Spinlock;
    TPromise<TErrorOr<TTimestamp>> Promise;


    void OnGetTimestampResponse(TTimestampServiceProxy::TRspGetTimestampPtr rsp)
    {
        TGuard<TSpinLock> guard(Spinlock);
        if (rsp->IsOK()) {
            Promise.Set(rsp->timestamp());
        } else {
            Promise.Set(rsp->GetError());
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

