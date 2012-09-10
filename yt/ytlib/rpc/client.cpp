#include "stdafx.h"
#include "client.h"
#include "private.h"
#include "message.h"
#include "rpc_dispatcher.h"

#include <ytlib/ytree/attribute_helpers.h>

#include <iterator>

namespace NYT {
namespace NRpc {

using namespace NBus;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = RpcClientLogger;

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
    , OneWay(oneWay)
    , Heavy_(false)
    , Channel(channel)
    , Attributes_(CreateEphemeralAttributes())
{
    YASSERT(channel);
}

IMessagePtr TClientRequest::Serialize() const
{
    NProto::TRequestHeader header;
    *header.mutable_request_id() = RequestId.ToProto();
    header.set_path(Path);
    header.set_verb(Verb);
    header.set_one_way(OneWay);
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
    return Path;
}

const Stroka& TClientRequest::GetVerb() const
{
    return Verb;
}

bool TClientRequest::IsOneWay() const
{
    return OneWay;
}

bool TClientRequest::IsHeavy() const
{
    return Heavy_;
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

TClientResponseBase::operator TError()
{
    return Error_;
}

void TClientResponseBase::OnError(const TError& error)
{
    LOG_DEBUG("Request failed (RequestId: %s)\n%s",
        ~RequestId_.ToString(),
        ~ToString(error));

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

    auto this_ = MakeStrong(this);
    TRpcDispatcher::Get()->GetPoolInvoker()->Invoke(BIND([=] () {
        this_->FireCompleted();
    }));
}

////////////////////////////////////////////////////////////////////////////////

TClientResponse::TClientResponse(const TRequestId& requestId)
    : TClientResponseBase(requestId)
    , Attributes_(CreateEphemeralAttributes())
{ }

IMessagePtr TClientResponse::GetResponseMessage() const
{
    YASSERT(ResponseMessage);
    return ResponseMessage;
}

void TClientResponse::Deserialize(IMessagePtr responseMessage)
{
    YASSERT(responseMessage);
    YASSERT(!ResponseMessage);

    ResponseMessage = responseMessage;

    const auto& parts = responseMessage->GetParts();
    YASSERT(parts.size() >= 2);

    DeserializeBody(parts[1]);
    
    Attachments_.clear();
    Attachments_.insert(
        Attachments_.begin(),
        parts.begin() + 2,
        parts.end());

    NProto::TResponseHeader responseHeader;
    YCHECK(ParseResponseHeader(responseMessage, &responseHeader));

    if (responseHeader.has_attributes()) {
        Attributes_ = FromProto(responseHeader.attributes());
    }
}

void TClientResponse::OnAcknowledgement()
{
    LOG_DEBUG("Request acknowledged (RequestId: %s)", ~RequestId_.ToString());

    TGuard<TSpinLock> guard(SpinLock);
    if (State == EState::Sent) {
        State = EState::Ack;
    }
}

void TClientResponse::OnResponse(IMessagePtr message)
{
    LOG_DEBUG("Response received (RequestId: %s)", ~RequestId_.ToString());

    {
        TGuard<TSpinLock> guard(SpinLock);
        YASSERT(State == EState::Sent || State == EState::Ack);
        State = EState::Done;
    }

    auto this_ = MakeStrong(this);
    TRpcDispatcher::Get()->GetPoolInvoker()->Invoke(BIND([=] () {
        this_->Deserialize(message);
        this_->FireCompleted();
    }));
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
        TGuard<TSpinLock> guard(SpinLock);
        if (State == EState::Done) {
            // Ignore the ack.
            return;
        }
        State = EState::Done;
    }

    FireCompleted();
}

void TOneWayClientResponse::OnResponse(IMessagePtr message)
{
    UNUSED(message);
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
