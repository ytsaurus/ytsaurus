#include "service_ticket_channel_factory.h"

#include <yt/yt/client/api/rpc_proxy/credentials_injecting_channel.h>

namespace NYT::NApi::NNative {

using namespace NAuth;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TServiceTicketInjectingChannelFactory
    : public IChannelFactory
{
public:
    TServiceTicketInjectingChannelFactory(
        IChannelFactoryPtr underlyingFactory,
        IServiceTicketAuthPtr serviceTicketAuth)
        : UnderlyingFactory_(std::move(underlyingFactory))
        , ServiceTicketAuth_(std::move(serviceTicketAuth))
    { }

    IChannelPtr CreateChannel(const TString& address) override
    {
        auto channel = UnderlyingFactory_->CreateChannel(address);
        if (!ServiceTicketAuth_) {
            return channel;
        }
        return NRpcProxy::CreateServiceTicketInjectingChannel(
            std::move(channel),
            /*user*/ std::nullopt,
            ServiceTicketAuth_);
    }

private:
    IChannelFactoryPtr UnderlyingFactory_;
    IServiceTicketAuthPtr ServiceTicketAuth_;
};

////////////////////////////////////////////////////////////////////////////////

IChannelFactoryPtr CreateServiceTicketInjectingChannelFactory(
    IChannelFactoryPtr underlyingFactory,
    IServiceTicketAuthPtr serviceTicketAuth)
{
    return New<TServiceTicketInjectingChannelFactory>(
        std::move(underlyingFactory),
        std::move(serviceTicketAuth));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
