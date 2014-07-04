#include "stdafx.h"
#include "caching_channel_factory.h"
#include "channel_detail.h"
#include "channel.h"
#include "client.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

class TCachingChannelFactory
    : public IChannelFactory
{
public:
    explicit TCachingChannelFactory(IChannelFactoryPtr underlyingFactory)
        : UnderlyingFactory_(std::move(underlyingFactory))
    { }

    virtual IChannelPtr CreateChannel(const Stroka& address) override
    {
        TGuard<TSpinLock> firstAttemptGuard(SpinLock);

        auto it = ChannelMap.find(address);
        if (it == ChannelMap.end()) {
            firstAttemptGuard.Release();

            auto channel = UnderlyingFactory_->CreateChannel(address);

            TGuard<TSpinLock> secondAttemptGuard(SpinLock);
            it = ChannelMap.find(address);
            if (it == ChannelMap.end()) {
                it = ChannelMap.insert(std::make_pair(address, channel)).first;
            } else {
                channel->Terminate(TError(
                    NRpc::EErrorCode::TransportError,
                    "Channel terminated"));
            }
        }

        return New<TChannelWrapper>(it->second);
    }

private:
    IChannelFactoryPtr UnderlyingFactory_;

    TSpinLock SpinLock;
    yhash_map<Stroka, IChannelPtr> ChannelMap;

};

IChannelFactoryPtr CreateCachingChannelFactory(IChannelFactoryPtr underlyingFactory)
{
    return New<TCachingChannelFactory>(underlyingFactory);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
