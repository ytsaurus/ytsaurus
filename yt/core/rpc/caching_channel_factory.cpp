#include "caching_channel_factory.h"
#include "channel.h"
#include "channel_detail.h"
#include "client.h"

#include <yt/core/concurrency/rw_spinlock.h>

namespace NYT {
namespace NRpc {

using namespace NConcurrency;

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
        {
            TReaderGuard guard(SpinLock_);
            auto it = ChannelMap_.find(address);
            if (it != ChannelMap_.end()) {
                return it->second;
            }
        }

        auto channel = UnderlyingFactory_->CreateChannel(address);

        {
            TWriterGuard guard(SpinLock_);
            auto it = ChannelMap_.find(address);
            if (it == ChannelMap_.end()) {
                YCHECK(ChannelMap_.insert(std::make_pair(address, channel)).second);
                return channel;
            } else {
                channel->Terminate(TError(
                    NRpc::EErrorCode::TransportError,
                    "Channel terminated"));
                // XXX(babenko): stop wrapping with TChannelWrapper after merging into master
                return New<TChannelWrapper>(it->second);
            }
        }
    }

private:
    const IChannelFactoryPtr UnderlyingFactory_;

    TReaderWriterSpinLock SpinLock_;
    yhash_map<Stroka, IChannelPtr> ChannelMap_;

};

IChannelFactoryPtr CreateCachingChannelFactory(IChannelFactoryPtr underlyingFactory)
{
    YCHECK(underlyingFactory);

    return New<TCachingChannelFactory>(std::move(underlyingFactory));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
