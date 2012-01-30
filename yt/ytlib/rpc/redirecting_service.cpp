#include "stdafx.h"
#include "redirecting_service.h"
#include "channel_cache.h"

namespace NYT {
namespace NRpc {

using namespace NBus;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = RpcLogger;

////////////////////////////////////////////////////////////////////////////////

TChannelCache ChannelCache;

////////////////////////////////////////////////////////////////////////////////

class TRedirecitingServiceBase::TRequest
    : public IClientRequest
{
public:
    TRequest(
        IMessage::TPtr message,
        const TRequestId& requestId,
        const Stroka& path,
        const Stroka& verb)
        : Message(message)
        , RequestId(requestId)
        , Path(path)
        , Verb(verb)
    { }

    IMessage::TPtr Serialize() const
    {
        return Message;
    }

    const TRequestId& GetRequestId() const
    {
        return RequestId;
    }

    const Stroka& GetPath() const
    {
        return Path;
    }
    const Stroka& GetVerb() const
    {
        return Verb;
    }

private:
    TRequestId RequestId;
    Stroka Path;
    Stroka Verb;
    IMessage::TPtr Message;
};

////////////////////////////////////////////////////////////////////////////////

class TRedirecitingServiceBase::TResponseHandler
    : public IClientResponseHandler
{
public:
    TResponseHandler(IServiceContext* context)
        : Context(context)
    { }

    void OnAcknowledgement()
    {
        LOG_DEBUG("Request acknowledged");
    }

    void OnResponse(NBus::IMessage* message)
    {
        // TODO: will be available soon
        // Context->Reply(message);
    }

    void OnError(const TError& error)
    {
        Context->Reply(error);
    }

private:
    IServiceContext::TPtr Context;
};

////////////////////////////////////////////////////////////////////////////////

TRedirecitingServiceBase::TRedirecitingServiceBase(
    const Stroka& serviceName,
    const Stroka& loggingCategory)
        : ServiceName(serviceName)
        , LoggingCategory(loggingCategory)
{ }

void TRedirecitingServiceBase::OnBeginRequest(IServiceContext* context)
{
    auto redirectParams = GetRedirectParams(context);
    IChannel::TPtr channel = ChannelCache.GetChannel(redirectParams.Address);

    auto request = New<TRequest>(
        context->GetRequestMessage(),
        context->GetRequestId(),
        context->GetPath(),
        context->GetVerb());

    auto responseHandler = New<TResponseHandler>(context);
    channel->Send(~request, ~responseHandler, redirectParams.Timeout);
}

void TRedirecitingServiceBase::OnEndRequest(IServiceContext* context)
{ }

Stroka TRedirecitingServiceBase::GetServiceName() const
{
    return ServiceName;
}

Stroka TRedirecitingServiceBase::GetLoggingCategory() const
{
    return LoggingCategory;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
