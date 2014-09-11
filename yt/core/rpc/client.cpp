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

static const auto& Logger = RpcClientLogger;

static auto ClientHostAnnotation = Stroka("client_host");
static auto RequestIdAnnotation = Stroka("request_id");

////////////////////////////////////////////////////////////////////////////////

TClientRequest::TClientRequest(
    IChannelPtr channel,
    const Stroka& service,
    const Stroka& method,
    bool oneWay,
    int protocolVersion)
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
    Header_.set_protocol_version(protocolVersion);
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
    , State_(EState::Sent)
    , ClientContext_(std::move(clientContext))
{ }

bool TClientResponseBase::IsOK() const
{
    return Error_.IsOK();
}

TClientResponseBase::operator TError() const
{
    return Error_;
}

void TClientResponseBase::OnError(const TError& error)
{
    {
        TGuard<TSpinLock> guard(SpinLock_);
        if (State_ == EState::Done) {
            // Ignore the error.
            // Most probably this is a late timeout.
            return;
        }
        State_ = EState::Done;
        Error_  = error;
    }

    NTracing::TTraceContextGuard guard(ClientContext_->GetTraceContext());
    FireCompleted();
}

void TClientResponseBase::BeforeCompleted()
{
    NTracing::TraceEvent(
        ClientContext_->GetTraceContext(),
        ClientContext_->GetService(),
        ClientContext_->GetMethod(),
        NTracing::ClientReceiveAnnotation);
}

////////////////////////////////////////////////////////////////////////////////

TClientResponse::TClientResponse(TClientContextPtr clientContext)
    : TClientResponseBase(std::move(clientContext))
{ }

TSharedRefArray TClientResponse::GetResponseMessage() const
{
    YASSERT(ResponseMessage_);
    return ResponseMessage_;
}

void TClientResponse::Deserialize(TSharedRefArray responseMessage)
{
    YASSERT(responseMessage);
    YASSERT(!ResponseMessage_);

    ResponseMessage_ = std::move(responseMessage);

    YASSERT(ResponseMessage_.Size() >= 2);

    DeserializeBody(ResponseMessage_[1]);

    Attachments_.clear();
    Attachments_.insert(
        Attachments_.begin(),
        ResponseMessage_.Begin() + 2,
        ResponseMessage_.End());
}

void TClientResponse::OnAcknowledgement()
{
    TGuard<TSpinLock> guard(SpinLock_);
    if (State_ == EState::Sent) {
        State_ = EState::Ack;
    }
}

void TClientResponse::OnResponse(TSharedRefArray message)
{
    {
        TGuard<TSpinLock> guard(SpinLock_);
        YASSERT(State_ == EState::Sent || State_ == EState::Ack);
        State_ = EState::Done;
    }

    NTracing::TTraceContextGuard guard(ClientContext_->GetTraceContext());
    Deserialize(message);
    FireCompleted();
}

////////////////////////////////////////////////////////////////////////////////

TOneWayClientResponse::TOneWayClientResponse(TClientContextPtr clientContext)
    : TClientResponseBase(std::move(clientContext))
    , Promise_(NewPromise<TThisPtr>())
{ }

void TOneWayClientResponse::OnAcknowledgement()
{
    {
        TGuard<TSpinLock> guard(SpinLock_);
        if (State_ == EState::Done) {
            // Ignore the ack.
            return;
        }
        State_ = EState::Done;
    }

    NTracing::TTraceContextGuard guard(ClientContext_->GetTraceContext());
    FireCompleted();
}

void TOneWayClientResponse::OnResponse(TSharedRefArray /*message*/)
{
    YUNREACHABLE();
}

TFuture<TOneWayClientResponsePtr> TOneWayClientResponse::GetAsyncResult()
{
    return Promise_;
}

void TOneWayClientResponse::FireCompleted()
{
    BeforeCompleted();
    Promise_.Set(this);
    Promise_.Reset();
}

////////////////////////////////////////////////////////////////////////////////

TProxyBase::TProxyBase(
    IChannelPtr channel,
    const Stroka& serviceName,
    int protocolVersion)
    : DefaultTimeout_(channel->GetDefaultTimeout())
    , DefaultRequestAck_(true)
    , ServiceName_(serviceName)
    , Channel_(std::move(channel))
    , ProtocolVersion_(protocolVersion)
{
    YASSERT(Channel_);
}

////////////////////////////////////////////////////////////////////////////////

TGenericProxy::TGenericProxy(
    IChannelPtr channel,
    const Stroka& serviceName)
    : TProxyBase(channel, serviceName, GenericProtocolVersion)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
