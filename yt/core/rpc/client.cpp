#include "stdafx.h"
#include "client.h"
#include "private.h"
#include "message.h"
#include "dispatcher.h"

#include <iterator>

#include <core/misc/address.h>

namespace NYT {
namespace NRpc {

using namespace NBus;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = RpcClientLogger;

static auto ClientHostAnnotation = Stroka("client_host");
static auto RequestIdAnnotation = Stroka("request_id");

////////////////////////////////////////////////////////////////////////////////

TClientRequest::TClientRequest(
    IChannelPtr channel,
    const Stroka& service,
    const Stroka& method,
    bool oneWay)
    : RequestAck_(true)
    , RequestHeavy_(false)
    , ResponseHeavy_(false)
    , Channel(channel)
{
    YCHECK(channel);

    Header_.set_service(service);
    Header_.set_method(method);
    Header_.set_one_way(oneWay);
    Header_.set_request_start_time(TInstant::Now().MicroSeconds());
    ToProto(Header_.mutable_request_id(), TRequestId::Create());
}

TSharedRefArray TClientRequest::Serialize() const
{
    auto header = Header_;
    header.set_retry_start_time(TInstant::Now().MicroSeconds());

    auto bodyData = SerializeBody();

    return CreateRequestMessage(
        header,
        bodyData,
        Attachments_);
}

void TClientRequest::DoInvoke(IClientResponseHandlerPtr responseHandler)
{
    Channel->Send(
        this,
        responseHandler,
        Timeout_,
        RequestAck_);
}

const Stroka& TClientRequest::GetService() const
{
    return Header_.service();
}

const Stroka& TClientRequest::GetMethod() const
{
    return Header_.method();
}

bool TClientRequest::IsOneWay() const
{
    return Header_.one_way();
}

bool TClientRequest::IsRequestHeavy() const
{
    return RequestHeavy_;
}

bool TClientRequest::IsResponseHeavy() const
{
    return RequestHeavy_;
}

TRequestId TClientRequest::GetRequestId() const
{
    return FromProto<TRequestId>(Header_.request_id());
}

TInstant TClientRequest::GetStartTime() const
{
    return TInstant(Header_.request_start_time());
}

void TClientRequest::SetStartTime(TInstant value)
{
    Header_.set_request_start_time(value.MicroSeconds());
}

TClientContextPtr TClientRequest::CreateClientContext()
{
    auto traceContext = NTracing::CreateChildTraceContext();
    if (traceContext.IsEnabled()) {
        SetTraceContext(&Header(), traceContext);

        TRACE_ANNOTATION(
            traceContext,
            GetService(),
            GetMethod(),
            NTracing::ClientSendAnnotation);

        TRACE_ANNOTATION(
            traceContext,
            RequestIdAnnotation,
            GetRequestId());

        TRACE_ANNOTATION(
            traceContext,
            ClientHostAnnotation,
            TAddressResolver::Get()->GetLocalHostName());
    }

    return New<TClientContext>(
        GetRequestId(),
        traceContext,
        GetService(),
        GetMethod());
}

////////////////////////////////////////////////////////////////////////////////

TClientResponseBase::TClientResponseBase(TClientContextPtr clientContext)
    : StartTime_(TInstant::Now())
    , State(EState::Sent)
    , ClientContext(std::move(clientContext))
{ }

bool TClientResponseBase::IsOK() const
{
    return Error_.IsOK();
}

TClientResponseBase::operator TError()
{
    return Error_;
}

void TClientResponseBase::OnError(const TError& error)
{
    {
        TGuard<TSpinLock> guard(SpinLock);
        if (State == EState::Done) {
            // Ignore the error.
            // Most probably this is a late timeout.
            return;
        }
        State = EState::Done;
        Error_  = error;
    }

    NTracing::TTraceContextGuard guard(ClientContext->GetTraceContext());
    FireCompleted();
}

void TClientResponseBase::BeforeCompleted()
{
    NTracing::TraceEvent(
        ClientContext->GetTraceContext(),
        ClientContext->GetService(),
        ClientContext->GetMethod(),
        NTracing::ClientReceiveAnnotation);
}

////////////////////////////////////////////////////////////////////////////////

TClientResponse::TClientResponse(TClientContextPtr clientContext)
    : TClientResponseBase(std::move(clientContext))
{ }

TSharedRefArray TClientResponse::GetResponseMessage() const
{
    YASSERT(ResponseMessage);
    return ResponseMessage;
}

void TClientResponse::Deserialize(TSharedRefArray responseMessage)
{
    YASSERT(responseMessage);
    YASSERT(!ResponseMessage);

    ResponseMessage = std::move(responseMessage);

    YASSERT(ResponseMessage.Size() >= 2);

    DeserializeBody(ResponseMessage[1]);

    Attachments_.clear();
    Attachments_.insert(
        Attachments_.begin(),
        ResponseMessage.Begin() + 2,
        ResponseMessage.End());
}

void TClientResponse::OnAcknowledgement()
{
    TGuard<TSpinLock> guard(SpinLock);
    if (State == EState::Sent) {
        State = EState::Ack;
    }
}

void TClientResponse::OnResponse(TSharedRefArray message)
{
    {
        TGuard<TSpinLock> guard(SpinLock);
        YASSERT(State == EState::Sent || State == EState::Ack);
        State = EState::Done;
    }

    NTracing::TTraceContextGuard guard(ClientContext->GetTraceContext());
    Deserialize(message);
    FireCompleted();
}

////////////////////////////////////////////////////////////////////////////////

TOneWayClientResponse::TOneWayClientResponse(TClientContextPtr clientContext)
    : TClientResponseBase(std::move(clientContext))
    , Promise(NewPromise<TThisPtr>())
{ }

void TOneWayClientResponse::OnAcknowledgement()
{
    {
        TGuard<TSpinLock> guard(SpinLock);
        if (State == EState::Done) {
            // Ignore the ack.
            return;
        }
        State = EState::Done;
    }

    NTracing::TTraceContextGuard guard(ClientContext->GetTraceContext());
    FireCompleted();
}

void TOneWayClientResponse::OnResponse(TSharedRefArray /*message*/)
{
    YUNREACHABLE();
}

TFuture<TOneWayClientResponsePtr> TOneWayClientResponse::GetAsyncResult()
{
    return Promise;
}

void TOneWayClientResponse::FireCompleted()
{
    BeforeCompleted();
    Promise.Set(this);
    Promise.Reset();
}

////////////////////////////////////////////////////////////////////////////////

TProxyBase::TProxyBase(
    IChannelPtr channel,
    const Stroka& serviceName)
    : DefaultTimeout_(channel->GetDefaultTimeout())
    , DefaultRequestAck_(true)
    , ServiceName_(serviceName)
    , Channel_(channel)
{
    YASSERT(Channel_);
}

////////////////////////////////////////////////////////////////////////////////

TGenericProxy::TGenericProxy(
    IChannelPtr channel,
    const Stroka& serviceName)
    : TProxyBase(channel, serviceName)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
