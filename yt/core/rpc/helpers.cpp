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
        : UnderlyingChannel_(underlyingChannel)
        , User_(user)
    { }

    virtual TNullable<TDuration> GetDefaultTimeout() const override
    {
        return UnderlyingChannel_->GetDefaultTimeout();
    }

    virtual void SetDefaultTimeout(const TNullable<TDuration>& timeout) override
    {
        UnderlyingChannel_->SetDefaultTimeout(timeout);
    }

    virtual void Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        TNullable<TDuration> timeout,
        bool requestAck) override
    {
        SetAuthenticatedUser(request, User_);
        UnderlyingChannel_->Send(
            request,
            responseHandler,
            timeout,
            requestAck);
    }

    virtual TFuture<void> Terminate(const TError& error) override
    {
        return UnderlyingChannel_->Terminate(error);
    }

private:
    IChannelPtr UnderlyingChannel_;
    Stroka User_;

};

IChannelPtr CreateAuthenticatedChannel(IChannelPtr underlyingChannel, const Stroka& user)
{
    YCHECK(underlyingChannel);

    return New<TAuthenticatedChannel>(underlyingChannel, user);
}

////////////////////////////////////////////////////////////////////////////////

class TRealmChannel
    : public IChannel
{
public:
    TRealmChannel(IChannelPtr underlyingChannel, const TRealmId& realmId)
        : UnderlyingChannel_(underlyingChannel)
        , RealmId_(realmId)
    { }

    virtual TNullable<TDuration> GetDefaultTimeout() const override
    {
        return UnderlyingChannel_->GetDefaultTimeout();
    }

    virtual void SetDefaultTimeout(const TNullable<TDuration>& timeout) override
    {
        UnderlyingChannel_->SetDefaultTimeout(timeout);
    }

    virtual void Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        TNullable<TDuration> timeout,
        bool requestAck) override
    {
        ToProto(request->Header().mutable_realm_id(), RealmId_);
        UnderlyingChannel_->Send(
            request,
            responseHandler,
            timeout,
            requestAck);
    }

    virtual TFuture<void> Terminate(const TError& error) override
    {
        return UnderlyingChannel_->Terminate(error);
    }

private:
    IChannelPtr UnderlyingChannel_;
    TRealmId RealmId_;

};

IChannelPtr CreateRealmChannel(IChannelPtr underlyingChannel, const TRealmId& realmId)
{
    YCHECK(underlyingChannel);

    return New<TRealmChannel>(underlyingChannel, realmId);
}

////////////////////////////////////////////////////////////////////////////////

class TRealmChannelFactory
    : public IChannelFactory
{
public:
    TRealmChannelFactory(
        IChannelFactoryPtr underlyingFactory,
        const TRealmId& realmId)
        : UnderlyingFactory_(underlyingFactory)
        , RealmId_(realmId)
    { }

    virtual IChannelPtr CreateChannel(const Stroka& address) override
    {
        auto underlyingChannel = UnderlyingFactory_->CreateChannel(address);
        return CreateRealmChannel(underlyingChannel, RealmId_);
    }

private:
    IChannelFactoryPtr UnderlyingFactory_;
    TRealmId RealmId_;

};

IChannelFactoryPtr CreateRealmChannelFactory(
    IChannelFactoryPtr underlyingFactory,
    const TRealmId& realmId)
{
    YCHECK(underlyingFactory);

    return New<TRealmChannelFactory>(underlyingFactory, realmId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
