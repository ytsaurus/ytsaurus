#include "stdafx.h"
#include "helpers.h"
#include "service.h"

#include <core/ytree/attribute_helpers.h>

namespace NYT {
namespace NRpc {

using namespace NRpc::NProto;

////////////////////////////////////////////////////////////////////////////////

void SetAuthenticatedUser(TRequestHeader* header, const Stroka& user)
{
    header->SetExtension(TAuthenticatedExt::authenticated_user, user);
}

void SetAuthenticatedUser(IClientRequestPtr request, const Stroka& user)
{
    SetAuthenticatedUser(&request->Header(), user);
}

TNullable<Stroka> FindAuthenticatedUser(const TRequestHeader& header)
{
    return header.HasExtension(TAuthenticatedExt::authenticated_user)
           ? TNullable<Stroka>(header.GetExtension(TAuthenticatedExt::authenticated_user))
           : Null;
}

TNullable<Stroka> FindAuthenticatedUser(IServiceContextPtr context)
{
    return FindAuthenticatedUser(context->RequestHeader());
}

Stroka GetAuthenticatedUserOrThrow(IServiceContextPtr context)
{
    auto user = FindAuthenticatedUser(context);
    if (!user) {
        THROW_ERROR_EXCEPTION("Must specify authenticated user in request header");
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

    virtual void Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        TNullable<TDuration> timeout) override
    {
        SetAuthenticatedUser(request, User);
        UnderlyingChannel->Send(request, responseHandler, timeout);
    }

    virtual TFuture<void> Terminate(const TError& error) override
    {
        return UnderlyingChannel->Terminate(error);
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

} // namespace NRpc
} // namespace NYT
