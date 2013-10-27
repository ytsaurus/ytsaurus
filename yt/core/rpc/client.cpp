#include "stdafx.h"
#include "client.h"
#include "private.h"
#include "message.h"
#include "dispatcher.h"

#include <core/ytree/attribute_helpers.h>

#include <iterator>

namespace NYT {
namespace NRpc {

using namespace NBus;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = RpcClientLogger;

////////////////////////////////////////////////////////////////////////////////

TProxyBase::TProxyBase(IChannelPtr channel, const Stroka& serviceName)
    : DefaultTimeout_(channel->GetDefaultTimeout())
    , ServiceName(serviceName)
    , Channel(channel)
{
    YASSERT(channel);
}

////////////////////////////////////////////////////////////////////////////////

TClientRequest::TClientRequest(
    IChannelPtr channel,
    const Stroka& path,
    const Stroka& verb,
    bool oneWay)
    : RequestHeavy_(false)
    , ResponseHeavy_(false)
    , Channel(channel)
    , Attributes_(CreateEphemeralAttributes())
{
    YCHECK(channel);

    Header_.set_path(path);
    Header_.set_verb(verb);
    Header_.set_one_way(oneWay);
    Header_.set_request_start_time(TInstant::Now().MicroSeconds());
    ToProto(Header_.mutable_request_id(), TRequestId::Create());
}

TSharedRefArray TClientRequest::Serialize() const
{
    auto header = Header_;
    header.set_retry_start_time(TInstant::Now().MicroSeconds());
    ToProto(header.mutable_attributes(), *Attributes_);

    auto bodyData = SerializeBody();

    return CreateRequestMessage(
        header,
        bodyData,
        Attachments_);
}

void TClientRequest::DoInvoke(IClientResponseHandlerPtr responseHandler)
{
    Channel->Send(this, responseHandler, Timeout_);
}

const Stroka& TClientRequest::GetPath() const
{
    return Header_.path();
}

const Stroka& TClientRequest::GetVerb() const
{
    return Header_.verb();
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

const NYTree::IAttributeDictionary& TClientRequest::Attributes() const
{
    return *Attributes_;
}

NYTree::IAttributeDictionary* TClientRequest::MutableAttributes()
{
    return ~Attributes_;
}

////////////////////////////////////////////////////////////////////////////////

TClientResponseBase::TClientResponseBase(const TRequestId& requestId)
    : RequestId_(requestId)
    , StartTime_(TInstant::Now())
    , State(EState::Sent)
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

    FireCompleted();
}

////////////////////////////////////////////////////////////////////////////////

TClientResponse::TClientResponse(const TRequestId& requestId)
    : TClientResponseBase(requestId)
    , Attributes_(CreateEphemeralAttributes())
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

    NProto::TResponseHeader responseHeader;
    YCHECK(ParseResponseHeader(ResponseMessage, &responseHeader));

    if (responseHeader.has_attributes()) {
        Attributes_ = FromProto(responseHeader.attributes());
    }
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

    Deserialize(message);
    FireCompleted();
}

IAttributeDictionary& TClientResponse::Attributes()
{
    return *Attributes_;
}

const IAttributeDictionary& TClientResponse::Attributes() const
{
    return *Attributes_;
}

////////////////////////////////////////////////////////////////////////////////

TOneWayClientResponse::TOneWayClientResponse(const TRequestId& requestId)
    : TClientResponseBase(requestId)
    , Promise(NewPromise<TPtr>())
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
    Promise.Set(this);
    Promise.Reset();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
