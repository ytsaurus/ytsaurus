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

static const auto ClientHostAnnotation = Stroka("client_host");
static const auto RequestIdAnnotation = Stroka("request_id");

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
    , Channel_(channel)
{
    YASSERT(Channel_);

    Header_.set_service(service);
    Header_.set_method(method);
    Header_.set_one_way(oneWay);
    ToProto(Header_.mutable_request_id(), TRequestId::Create());
    Header_.set_protocol_version(protocolVersion);
}

TSharedRefArray TClientRequest::Serialize()
{
    auto now = TInstant::Now();

    // Set request start time on first serialization attempt.
    if (!Header_.has_request_start_time()) {
        Header_.set_request_start_time(now.MicroSeconds());
    }
    Header_.set_retry_start_time(now.MicroSeconds());

    if (Timeout_) {
        Header_.set_timeout(Timeout_->MicroSeconds());
    }

    if (!SerializedBody_) {
        SerializedBody_ = SerializeBody();
    }

    return CreateRequestMessage(
        Header_,
        SerializedBody_,
        Attachments_);
}

IClientRequestControlPtr TClientRequest::Send(IClientResponseHandlerPtr responseHandler)
{
    return Channel_->Send(
        this,
        std::move(responseHandler),
        Timeout_,
        RequestAck_);
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

TRealmId TClientRequest::GetRealmId() const
{
    return FromProto<TRealmId>(Header_.realm_id());
}

const Stroka& TClientRequest::GetService() const
{
    return Header_.service();
}

const Stroka& TClientRequest::GetMethod() const
{
    return Header_.method();
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
        TraceRequest(traceContext);
    }

    return New<TClientContext>(
        GetRequestId(),
        traceContext,
        GetService(),
        GetMethod());
}

void TClientRequest::TraceRequest(const NTracing::TTraceContext& traceContext)
{
    NTracing::TraceEvent(
        traceContext,
        GetService(),
        GetMethod(),
        NTracing::ClientSendAnnotation);

    NTracing::TraceEvent(
        traceContext,
        RequestIdAnnotation,
        GetRequestId());

    NTracing::TraceEvent(
        traceContext,
        ClientHostAnnotation,
        TAddressResolver::Get()->GetLocalHostName());
}

////////////////////////////////////////////////////////////////////////////////

TClientResponseBase::TClientResponseBase(TClientContextPtr clientContext)
    : StartTime_(TInstant::Now())
    , ClientContext_(std::move(clientContext))
{ }

void TClientResponseBase::HandleError(const TError& error)
{
    {
        TGuard<TSpinLock> guard(SpinLock_);
        if (State_ == EState::Done) {
            // Ignore the error.
            // Most probably this is a late timeout.
            return;
        }
        State_ = EState::Done;
    }

    Finish(error);
}

void TClientResponseBase::Finish(const TError& error)
{
    NTracing::TTraceContextGuard guard(ClientContext_->GetTraceContext());
    TraceResponse();
    SetPromise(error);
}

void TClientResponseBase::TraceResponse()
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

void TClientResponse::HandleAcknowledgement()
{
    TGuard<TSpinLock> guard(SpinLock_);
    if (State_ == EState::Sent) {
        State_ = EState::Ack;
    }
}

void TClientResponse::HandleResponse(TSharedRefArray message)
{
    {
        TGuard<TSpinLock> guard(SpinLock_);
        YASSERT(State_ == EState::Sent || State_ == EState::Ack);
        State_ = EState::Done;
    }

    Deserialize(std::move(message));
    Finish(TError());
}

////////////////////////////////////////////////////////////////////////////////

TOneWayClientResponse::TOneWayClientResponse(TClientContextPtr clientContext)
    : TClientResponseBase(std::move(clientContext))
{ }

void TOneWayClientResponse::HandleAcknowledgement()
{
    {
        TGuard<TSpinLock> guard(SpinLock_);
        if (State_ == EState::Done) {
            // Ignore the ack.
            return;
        }
        State_ = EState::Done;
    }

    Finish(TError());
}

void TOneWayClientResponse::HandleResponse(TSharedRefArray /*message*/)
{
    YUNREACHABLE();
}

auto TOneWayClientResponse::GetPromise() -> TPromise<TResult>
{
    return Promise_;
}

void TOneWayClientResponse::SetPromise(const TError& error)
{
    Promise_.Set(error);
    Promise_.Reset();
}

////////////////////////////////////////////////////////////////////////////////

TProxyBase::TProxyBase(
    IChannelPtr channel,
    const Stroka& serviceName,
    int protocolVersion)
    : DefaultTimeout_(channel->GetDefaultTimeout())
    , DefaultRequestAck_(true)
    , Channel_(std::move(channel))
    , ServiceName_(serviceName)
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
