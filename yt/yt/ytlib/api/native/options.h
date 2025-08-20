#pragma once

#include "public.h"

#include <yt/yt/client/api/options.h>

namespace NYT::NApi::NNative {

///////////////////////////////////////////////////////////////////////////////

struct TClientOptions
    : public NYT::NApi::TClientOptions
{
private:
    using TBase = NYT::NApi::TClientOptions;

public:
    static TClientOptions FromToken(std::string token);
    static TClientOptions FromUserAndToken(std::string user, std::string token);
    static TClientOptions FromUser(std::string user, std::optional<std::string> userTag = {});
    static TClientOptions Root();
    static TClientOptions FromAuthenticationIdentity(const NRpc::TAuthenticationIdentity& identity);
    static TClientOptions FromServiceTicketAuth(NAuth::IServiceTicketAuthPtr ticketAuth);
    static TClientOptions FromUserTicket(std::string userTicket);

    TCallback<NRpc::IChannelPtr(NRpc::IChannelPtr)> ChannelWrapper;
    TCallback<NRpc::IChannelFactoryPtr(NRpc::IChannelFactoryPtr)> ChannelFactoryWrapper;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNative
