#include "stdafx.h"
#include "redirector_service.h"
#include "channel_cache.h"

namespace NYT {
namespace NRpc {

using namespace NBus;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = RpcLogger;
static TChannelCache ChannelCache;

////////////////////////////////////////////////////////////////////////////////

class TRedirectorService::TRequest
    : public IClientRequest
{
public:
    TRequest(
        IMessage::TPtr message,
        bool oneWay,
        const TRequestId& requestId,
        const Stroka& path,
        const Stroka& verb)
        : Message(message)
        , OneWay(oneWay)
        , RequestId(requestId)
        , Path(path)
        , Verb(verb)
    { }

    virtual IMessage::TPtr Serialize() const
    {
        return Message;
    }

    virtual bool IsOneWay() const
    {
        return OneWay;
    }

    virtual const TRequestId& GetRequestId() const
    {
        return RequestId;
    }

    virtual const Stroka& GetPath() const
    {
        return Path;
    }

    virtual const Stroka& GetVerb() const
    {
        return Verb;
    }

    virtual NYTree::IAttributeDictionary& Attributes()
    {
        YUNREACHABLE();
    }

    virtual const NYTree::IAttributeDictionary& Attributes() const
    {
        YUNREACHABLE();
    }

private:
    IMessage::TPtr Message;
    bool OneWay;
    TRequestId RequestId;
    Stroka Path;
    Stroka Verb;
};

////////////////////////////////////////////////////////////////////////////////

class TRedirectorService::TResponseHandler
    : public IClientResponseHandler
{
public:
    TResponseHandler(IServiceContextPtr context)
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
    IServiceContextPtr Context;

};

////////////////////////////////////////////////////////////////////////////////

TRedirectorService::TRedirectorService(
    const Stroka& serviceName,
    const Stroka& loggingCategory)
    : ServiceName(serviceName)
    , LoggingCategory(loggingCategory)
{ }

void TRedirectorService::OnBeginRequest(IServiceContextPtr context)
{
    HandleRedirect(context).Subscribe(BIND([=] (TRedirectResult result)
        {
            if (!result.IsOK()) {
                context->Reply(TError(
                    NRpc::EErrorCode::Unavailable,
                    Sprintf("Redirection failed\n%s", ~result.GetMessage())));
                return;
            }

            const auto& params = result.Value();

            context->SetRequestInfo(Sprintf("Address: %s, Timeout: %s",
                ~params.Address,
                ~ToString(params.Timeout)));

            auto channel = ChannelCache.GetChannel(params.Address);

            auto request = New<TRequest>(
                context->GetRequestMessage(),
                context->IsOneWay(),
                context->GetRequestId(),
                context->GetPath(),
                context->GetVerb());

            auto responseHandler = New<TResponseHandler>(context);
            channel->Send(request, responseHandler, params.Timeout);
        }));
}

void TRedirectorService::OnEndRequest(IServiceContextPtr context)
{
    UNUSED(context);
}

Stroka TRedirectorService::GetServiceName() const
{
    return ServiceName;
}

Stroka TRedirectorService::GetLoggingCategory() const
{
    return LoggingCategory;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
