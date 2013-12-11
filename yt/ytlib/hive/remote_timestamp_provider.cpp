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

// TODO(babenko): this needs much improvement
class TRemoteTimestampProvider
    : public ITimestampProvider
{
public:
    TRemoteTimestampProvider(
        TRemoteTimestampProviderConfigPtr config,
        IChannelFactoryPtr channelFactory)
        : Config(config)
        , Channel(CreatePeerChannel(config, channelFactory, EPeerRole::Leader))
        , Proxy(Channel)
        , LatestTimestamp(NullTimestamp)
    { }

    virtual TFuture<TErrorOr<TTimestamp>> GenerateNewTimestamp() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto req = Proxy.GetTimestamp();
        return req->Invoke().Apply(
            BIND(&TRemoteTimestampProvider::OnGetTimestampResponse, MakeStrong(this)));
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


    TErrorOr<TTimestamp> OnGetTimestampResponse(TTimestampServiceProxy::TRspGetTimestampPtr rsp)
    {
        if (rsp->IsOK()) {
            TGuard<TSpinLock> guard(Spinlock);
            auto timestamp = TTimestamp(rsp->timestamp());
            LatestTimestamp = std::max(LatestTimestamp, timestamp);
            return TErrorOr<TTimestamp>(timestamp);
        } else {
            return TErrorOr<TTimestamp>(rsp->GetError());
        }
    }

};

ITimestampProviderPtr CreateRemoteTimestampProvider(
    TRemoteTimestampProviderConfigPtr config,
    IChannelFactoryPtr channelFactory)
{
    return New<TRemoteTimestampProvider>(
        config,
        channelFactory);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT

