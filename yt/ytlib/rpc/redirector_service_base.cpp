#include "stdafx.h"
#include "redirector_service_base.h"
#include "channel_cache.h"

namespace NYT {
namespace NRpc {

using namespace NBus;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = RpcLogger;
static TChannelCache ChannelCache;

////////////////////////////////////////////////////////////////////////////////

class TRedirectorServiceBase::TRequest
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

class TRedirectorServiceBase::TResponseHandler
    : public IClientResponseHandler
{
public:
    TResponseHandler(IServiceContext* context)
        : Context(context)
    { }

    void OnAcknowledgement()
    { }

    void OnResponse(NBus::IMessage* message)
    {
        Context->Reply(message);
    }

    void OnError(const TError& error)
    {
        Context->Reply(error);
    }

private:
    IServiceContext::TPtr Context;

};

////////////////////////////////////////////////////////////////////////////////

TRedirectorServiceBase::TRedirectorServiceBase(
    const Stroka& serviceName,
    const Stroka& loggingCategory)
    : ServiceName(serviceName)
    , LoggingCategory(loggingCategory)
{ }

void TRedirectorServiceBase::OnBeginRequest(IServiceContext* context)
{
    TRedirectParams redirectParams;
    try {
        redirectParams = GetRedirectParams(context);
    }
    catch (const std::exception& ex) {
        context->Reply(TError(
            NRpc::EErrorCode::Unavailable,
            Sprintf("Redirection failed\n%s", ex.what())));
        return;
    }

    context->SetRequestInfo(Sprintf("Address: %s, Timeout: %d",
        ~redirectParams.Address,
        static_cast<int>(redirectParams.Timeout.MilliSeconds())));

    auto channel = ChannelCache.GetChannel(redirectParams.Address);

    auto request = New<TRequest>(
        context->GetRequestMessage(),
        context->GetRequestId(),
        context->GetPath(),
        context->GetVerb());

    auto responseHandler = New<TResponseHandler>(context);
    channel->Send(~request, ~responseHandler, redirectParams.Timeout);
}

void TRedirectorServiceBase::OnEndRequest(IServiceContext* context)
{
    UNUSED(context);
}

Stroka TRedirectorServiceBase::GetServiceName() const
{
    return ServiceName;
}

Stroka TRedirectorServiceBase::GetLoggingCategory() const
{
    return LoggingCategory;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
