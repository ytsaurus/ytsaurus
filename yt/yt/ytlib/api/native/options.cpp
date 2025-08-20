#include "options.h"

namespace NYT::NApi::NNative {

///////////////////////////////////////////////////////////////////////////////

TClientOptions TClientOptions::FromToken(std::string token)
{
    TClientOptions result;
    static_cast<TBase&>(result) = TBase::FromToken(std::move(token));
    return result;
}

TClientOptions TClientOptions::FromUserAndToken(std::string user, std::string token)
{
    TClientOptions result;
    static_cast<TBase&>(result) = TBase::FromUserAndToken(std::move(user), std::move(token));
    return result;
}

TClientOptions TClientOptions::FromUser(std::string user, std::optional<std::string> userTag)
{
    TClientOptions result;
    static_cast<TBase&>(result) = TBase::FromUser(std::move(user), std::move(userTag));
    return result;
}

TClientOptions TClientOptions::Root()
{
    TClientOptions result;
    static_cast<TBase&>(result) = TBase::Root();
    return result;
}

TClientOptions TClientOptions::FromAuthenticationIdentity(const NRpc::TAuthenticationIdentity& identity)
{
    TClientOptions result;
    static_cast<TBase&>(result) = TBase::FromAuthenticationIdentity(identity);
    return result;
}

TClientOptions TClientOptions::FromServiceTicketAuth(NAuth::IServiceTicketAuthPtr ticketAuth)
{
    TClientOptions result;
    static_cast<TBase&>(result) = TBase::FromServiceTicketAuth(std::move(ticketAuth));
    return result;
}

TClientOptions TClientOptions::FromUserTicket(std::string userTicket)
{
    TClientOptions result;
    static_cast<TBase&>(result) = TBase::FromUserTicket(std::move(userTicket));
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
