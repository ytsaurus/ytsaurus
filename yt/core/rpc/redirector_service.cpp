#include "stdafx.h"
#include "redirector_service.h"
#include "client.h"
#include "service.h"
#include "message.h"
#include "private.h"

#include <core/bus/bus.h>

#include <core/ytree/node.h>

namespace NYT {
namespace NRpc {

using namespace NBus;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = RpcServerLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

class TRedirectedRequest
    : public IClientRequest
{
public:
    TRedirectedRequest(
        const NProto::TRequestHeader& header,
        TSharedRefArray message)
        : Header_(header)
        , Message(message)
        , RequestId(FromProto<TRequestId>(Header_.request_id()))
    { }

    virtual TSharedRefArray Serialize() const override
    {
        return Message;
    }

    virtual bool IsOneWay() const override
    {
        return Header_.one_way();
    }

    virtual bool IsRequestHeavy() const override
    {
        return false;
    }

    virtual bool IsResponseHeavy() const override
    {
        return false;
    }

    virtual TRequestId GetRequestId() const override
    {
        return RequestId;
    }

    virtual const Stroka& GetPath() const override
    {
        return Header_.path();
    }

    virtual const Stroka& GetVerb() const override
    {
        return Header_.verb();
    }

    virtual TInstant GetStartTime() const override
    {
        YUNREACHABLE();
    }

    virtual void SetStartTime(TInstant /*value*/) override
    {
        YUNREACHABLE();
    }

    virtual const NProto::TRequestHeader& Header() const override
    {
        return Header_;
    }

    virtual NProto::TRequestHeader& Header() override
    {
        return Header_;
    }

    virtual const NYTree::IAttributeDictionary& Attributes() const override
    {
        YUNREACHABLE();
    }

    virtual NYTree::IAttributeDictionary* MutableAttributes() override
    {
        YUNREACHABLE();
    }

private:
    NProto::TRequestHeader Header_;
    TSharedRefArray Message;

    TRequestId RequestId;

};

////////////////////////////////////////////////////////////////////////////////

class TRedirectedResponseHandler
    : public IClientResponseHandler
{
public:
    TRedirectedResponseHandler(IClientRequestPtr request, IBusPtr replyBus)
        : Request(request)
        , ReplyBus(replyBus)
    { }

    virtual void OnAcknowledgement() override
    {
        LOG_DEBUG("Redirected request acknowledged (RequestId: %s)",
            ~ToString(Request->GetRequestId()));
    }

    virtual void OnResponse(TSharedRefArray message) override
    {
        LOG_DEBUG("Response for redirected request received (RequestId: %s)",
            ~ToString(Request->GetRequestId()));

        ReplyBus->Send(message);
    }

    virtual void OnError(const TError& error) override
    {
        LOG_DEBUG(error, "Redirected request failed (RequestId: %s)",
            ~ToString(Request->GetRequestId()));

        auto message = CreateErrorResponseMessage(Request->GetRequestId(), error);
        ReplyBus->Send(message);
    }

private:
    IClientRequestPtr Request;
    IBusPtr ReplyBus;

};

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TRedirectorService
    : public IService
{
public:
    TRedirectorService(
        const Stroka& serviceName,
        IChannelPtr sinkChannel)
        : ServiceName(serviceName)
        , SinkChannel(sinkChannel)
    { }

    virtual void OnRequest(
        const NProto::TRequestHeader& header,
        TSharedRefArray message,
        NBus::IBusPtr replyBus) override
    {
        auto request = New<TRedirectedRequest>(header, message);

        LOG_DEBUG("Redirecting request (RequestId: %s, Path: %s, Verb: %s)",
            ~ToString(request->GetRequestId()),
            ~request->GetPath(),
            ~request->GetVerb());

        auto responseHandler = New<TRedirectedResponseHandler>(request, replyBus);
        SinkChannel->Send(request, responseHandler, Null);
    }

    virtual Stroka GetServiceName() const override
    {
        return ServiceName;
    }

    virtual void Configure(NYTree::INodePtr config) override
    {
        UNUSED(config);
    }

private:
    Stroka ServiceName;
    Stroka LoggingCategory;
    IChannelPtr SinkChannel;

};

IServicePtr CreateRedirectorService(
    const Stroka& serviceName,
    IChannelPtr sinkChannel)
{
    YCHECK(sinkChannel);

    return New<TRedirectorService>(serviceName, sinkChannel);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
