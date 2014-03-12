#include "stdafx.h"
#include "timestamp_provider.h"
#include "remote_timestamp_provider.h"
#include "timestamp_service_proxy.h"
#include "config.h"

#include <core/concurrency/thread_affinity.h>

#include <ytlib/hydra/peer_channel.h>

namespace NYT {
namespace NTransactionClient {

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
        : Config_(config)
        , Channel_(CreatePeerChannel(config, channelFactory, EPeerRole::Leader))
        , Proxy(Channel_)
        , LatestTimestamp_(NullTimestamp)
    { }

    virtual TFuture<TErrorOr<TTimestamp>> GenerateTimestamps(int count) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto req = Proxy.GenerateTimestamps();
        req->set_count(count);
        return req->Invoke().Apply(BIND(
            &TRemoteTimestampProvider::OnGenerateTimestampsResponse,
            MakeStrong(this),
            count));
    }

    virtual TTimestamp GetLatestTimestamp() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return LatestTimestamp_;
    }


private:
    TRemoteTimestampProviderConfigPtr Config_;

    IChannelPtr Channel_;
    TTimestampServiceProxy Proxy;

    TSpinLock SpinLock_;
    TTimestamp LatestTimestamp_;


    TErrorOr<TTimestamp> OnGenerateTimestampsResponse(
        int count,
        TTimestampServiceProxy::TRspGenerateTimestampsPtr rsp)
    {
        if (rsp->IsOK()) {
            TGuard<TSpinLock> guard(SpinLock_);
            auto timestamp = TTimestamp(rsp->timestamp());
            LatestTimestamp_ = std::max(LatestTimestamp_, timestamp + count - 1);
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

} // namespace NTransactionClient
} // namespace NYT

