#include "stdafx.h"
#include "rpc_helpers.h"

#include <ytlib/ytree/attribute_helpers.h>

#include <ytlib/rpc/service.h>

namespace NYT {
namespace NSecurityClient {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

void SetRpcAuthenticatedUser(NRpc::IClientRequestPtr request, const Stroka& user)
{
    request->Attributes().Set("authenticated_user", user);
}

TNullable<Stroka> FindRpcAuthenticatedUser(NRpc::IServiceContextPtr context)
{
    return context->RequestAttributes().Find<Stroka>("authenticated_user");
}

Stroka GetRpcAuthenticatedUser(NRpc::IServiceContextPtr context)
{
    auto user = FindRpcAuthenticatedUser(context);
    if (!user) {
        THROW_ERROR_EXCEPTION("Must specify authenticated user in request attributes");
    }
    return user.Get();
}

////////////////////////////////////////////////////////////////////////////////

class TAuthenticatedChannel
    : public IChannel
{
public:
    TAuthenticatedChannel(IChannelPtr underlyingChannel, const Stroka& user)
        : UnderlyingChannel(underlyingChannel)
        , User(user)
    { }

    virtual TNullable<TDuration> GetDefaultTimeout() const override
    {
        return UnderlyingChannel->GetDefaultTimeout();
    }

    virtual bool GetRetryEnabled() const override
    {
        return UnderlyingChannel->GetRetryEnabled();
    }

    virtual void Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        TNullable<TDuration> timeout) override
    {
        SetRpcAuthenticatedUser(request, User);
        UnderlyingChannel->Send(request, responseHandler, timeout);
    }

    virtual void Terminate(const TError& error) override
    {
        UnderlyingChannel->Terminate(error);
    }

private:
    IChannelPtr UnderlyingChannel;
    Stroka User;

};

IChannelPtr CreateAuthenticatedChannel(IChannelPtr underlyingChannel, const Stroka& user)
{
    YCHECK(underlyingChannel);

    return New<TAuthenticatedChannel>(underlyingChannel, user);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityClient
} // namespace NYT
