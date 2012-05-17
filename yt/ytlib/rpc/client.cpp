#include "stdafx.h"
#include "client.h"
#include "message.h"

namespace NYT {
namespace NRpc {

using namespace NBus;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = RpcLogger;

////////////////////////////////////////////////////////////////////////////////

TProxyBase::TProxyBase(IChannelPtr channel, const Stroka& serviceName)
    : Channel(channel)
    , ServiceName(serviceName)
    , DefaultTimeout_(channel->GetDefaultTimeout())
{
    YASSERT(channel);
}

////////////////////////////////////////////////////////////////////////////////

TClientRequest::TClientRequest(
    IChannelPtr channel,
    const Stroka& path,
    const Stroka& verb,
    bool oneWay)
    : Path(path)
    , Verb(verb)
    , RequestId(TRequestId::Create())
    , OneWay_(oneWay)
    , Channel(channel)
    , Attributes_(CreateEphemeralAttributes())
{
    YASSERT(channel);
}

IMessage::TPtr TClientRequest::Serialize() const
{
    NProto::TRequestHeader header;
    *header.mutable_request_id() = RequestId.ToProto();
    header.set_path(Path);
    header.set_verb(Verb);
    header.set_one_way(OneWay_);
    ToProto(header.mutable_attributes(), *Attributes_);

    auto bodyData = SerializeBody();

    return CreateRequestMessage(
        header,
        MoveRV(bodyData),
        Attachments_);
}

void TClientRequest::DoInvoke(
    IClientResponseHandlerPtr responseHandler,
    TNullable<TDuration> timeout)
{
    Channel->Send(this, responseHandler, timeout);
}

const Stroka& TClientRequest::GetPath() const
{
    return Path;
}

const Stroka& TClientRequest::GetVerb() const
{
    return Verb;
}

const TRequestId& TClientRequest::GetRequestId() const
{
    return RequestId;
}

NYTree::IAttributeDictionary& TClientRequest::Attributes()
{
    return *Attributes_;
}

const NYTree::IAttributeDictionary& TClientRequest::Attributes() const
{
    return *Attributes_;
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

int TClientResponseBase::GetErrorCode() const
{
    return Error_.GetCode();
}

void TClientResponseBase::OnError(const TError& error)
{
    LOG_DEBUG("Request failed (RequestId: %s)\n%s",
        ~RequestId_.ToString(),
        ~error.ToString());

    {
        TGuard<TSpinLock> guard(&SpinLock);
        if (State == EState::Done) {
            // Ignore the error.
            // Most probably this is a late timeout.
            return;
        }
        State = EState::Done;
    }

    Error_  = error;
    FireCompleted();
}

////////////////////////////////////////////////////////////////////////////////

TClientResponse::TClientResponse(const TRequestId& requestId)
    : TClientResponseBase(requestId)
    , Attributes_(CreateEphemeralAttributes())
{ }

IMessage::TPtr TClientResponse::GetResponseMessage() const
{
    YASSERT(ResponseMessage);
    return ResponseMessage;
}

void TClientResponse::Deserialize(IMessage::TPtr responseMessage)
{
    YASSERT(responseMessage);
    YASSERT(!ResponseMessage);

    ResponseMessage = responseMessage;

    const auto& parts = responseMessage->GetParts();
    YASSERT(parts.ysize() >= 2);

    DeserializeBody(parts[1]);
    
    Attachments_.clear();
    std::copy(
        parts.begin() + 2,
        parts.end(),
        std::back_inserter(Attachments_));

    auto header = GetResponseHeader(~responseMessage);

    if (header.has_attributes()) {
        Attributes_ = FromProto(header.attributes());
    }
}

void TClientResponse::OnAcknowledgement()
{
    LOG_DEBUG("Request acknowledged (RequestId: %s)", ~RequestId_.ToString());

    TGuard<TSpinLock> guard(&SpinLock);
    if (State == EState::Sent) {
        State = EState::Ack;
    }
}

void TClientResponse::OnResponse(IMessage* message)
{
    LOG_DEBUG("Response received (RequestId: %s)", ~RequestId_.ToString());

    {
        TGuard<TSpinLock> guard(&SpinLock);
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
    LOG_DEBUG("Request acknowledged (RequestId: %s)", ~RequestId_.ToString());

    {
        TGuard<TSpinLock> guard(&SpinLock);
        if (State == EState::Done) {
            // Ignore the ack.
            return;
        }
        State = EState::Done;
    }

    FireCompleted();
}

void TOneWayClientResponse::OnResponse(IMessage* message)
{
    UNUSED(message);
    YUNREACHABLE();
}

TFuture<TOneWayClientResponse::TPtr> TOneWayClientResponse::GetAsyncResult()
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
