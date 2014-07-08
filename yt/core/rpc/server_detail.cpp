#include "stdafx.h"
#include "server_detail.h"
#include "private.h"
#include "message.h"
#include "config.h"

namespace NYT {
namespace NRpc {

using namespace NConcurrency;
using namespace NBus;
using namespace NYTree;
using namespace NRpc::NProto;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = RpcServerLogger;

////////////////////////////////////////////////////////////////////////////////

TServiceContextBase::TServiceContextBase(
    std::unique_ptr<TRequestHeader> header,
    TSharedRefArray requestMessage)
    : RequestHeader_(std::move(header))
    , RequestMessage_(std::move(requestMessage))
{
    Initialize();
}

TServiceContextBase::TServiceContextBase(
    TSharedRefArray requestMessage)
    : RequestHeader_(new TRequestHeader())
    , RequestMessage_(std::move(requestMessage))
{
    YCHECK(ParseRequestHeader(RequestMessage_, RequestHeader_.get()));
    Initialize();
}

void TServiceContextBase::Initialize()
{
    RequestId_ = RequestHeader_->has_request_id()
        ? FromProto<TRequestId>(RequestHeader_->request_id())
        : NullRequestId;

    RealmId_ = RequestHeader_->has_realm_id()
        ? FromProto<TRealmId>(RequestHeader_->realm_id())
        : NullRealmId;

    Replied_ = false;

    YASSERT(RequestMessage_.Size() >= 2);
    RequestBody_ = RequestMessage_[1];
    RequestAttachments_ = std::vector<TSharedRef>(
        RequestMessage_.Begin() + 2,
        RequestMessage_.End());
}

void TServiceContextBase::Reply(const TError& error)
{
    YASSERT(!Replied_);

    Error_ = error;
    Replied_ = true;

    if (IsOneWay()) {
        // Cannot reply OK to a one-way request.
        YCHECK(!error.IsOK());
    } else {
        DoReply();
    }

    LogResponse(error);
}

void TServiceContextBase::Reply(TSharedRefArray responseMessage)
{
    YASSERT(!Replied_);
    YASSERT(!IsOneWay());
    YASSERT(responseMessage.Size() >= 1);

    TResponseHeader header;
    YCHECK(DeserializeFromProto(&header, responseMessage[0]));

    Error_ = FromProto<TError>(header.error());
    if (Error_.IsOK()) {
        YASSERT(responseMessage.Size() >= 2);
        ResponseBody_ = responseMessage[1];
        ResponseAttachments_ = std::vector<TSharedRef>(
            responseMessage.Begin() + 2,
            responseMessage.End());
    } else {
        ResponseBody_.Reset();
        ResponseAttachments_.clear();
    }

    Replied_ = true;
    DoReply();
    
    LogResponse(Error_);
}

TSharedRefArray TServiceContextBase::GetResponseMessage() const
{
    YCHECK(Replied_);

    if (!ResponseMessage_) {
        NProto::TResponseHeader header;
        ToProto(header.mutable_request_id(), RequestId_);
        ToProto(header.mutable_error(), Error_);

        ResponseMessage_ = Error_.IsOK()
            ? CreateResponseMessage(
                header,
                ResponseBody_,
                ResponseAttachments_)
            : CreateErrorResponseMessage(header);
    }

    return ResponseMessage_;
}

bool TServiceContextBase::IsOneWay() const
{
    return RequestHeader_->one_way();
}

bool TServiceContextBase::IsReplied() const
{
    return Replied_;
}

const TError& TServiceContextBase::GetError() const
{
    YASSERT(Replied_);

    return Error_;
}

TSharedRef TServiceContextBase::GetRequestBody() const
{
    return RequestBody_;
}

std::vector<TSharedRef>& TServiceContextBase::RequestAttachments()
{
    return RequestAttachments_;
}

TSharedRef TServiceContextBase::GetResponseBody()
{
    return ResponseBody_;
}

void TServiceContextBase::SetResponseBody(const TSharedRef& responseBody)
{
    YASSERT(!Replied_);
    YASSERT(!IsOneWay());

    ResponseBody_ = responseBody;
}

std::vector<TSharedRef>& TServiceContextBase::ResponseAttachments()
{
    YASSERT(!IsOneWay());

    return ResponseAttachments_;
}

TSharedRefArray TServiceContextBase::GetRequestMessage() const
{
    return RequestMessage_;
}

TRequestId TServiceContextBase::GetRequestId() const
{
    return RequestId_;
}

TNullable<TInstant> TServiceContextBase::GetRequestStartTime() const
{
    return
        RequestHeader_->has_request_start_time()
        ? TNullable<TInstant>(TInstant(RequestHeader_->request_start_time()))
        : Null;
}

TNullable<TInstant> TServiceContextBase::GetRetryStartTime() const
{
    return
        RequestHeader_->has_retry_start_time()
        ? TNullable<TInstant>(TInstant(RequestHeader_->retry_start_time()))
        : Null;
}

i64 TServiceContextBase::GetPriority() const
{
    return
        RequestHeader_->has_request_start_time()
        ? -RequestHeader_->request_start_time()
        : 0;
}

const Stroka& TServiceContextBase::GetService() const
{
    return RequestHeader_->service();
}

const Stroka& TServiceContextBase::GetMethod() const
{
    return RequestHeader_->method();
}

const TRealmId& TServiceContextBase::GetRealmId() const
{
    return RealmId_;
}

const TRequestHeader& TServiceContextBase::RequestHeader() const
{
    return *RequestHeader_;
}

TRequestHeader& TServiceContextBase::RequestHeader()
{
    return *RequestHeader_;
}

void TServiceContextBase::SetRequestInfo(const Stroka& info)
{
    RequestInfo_ = info;
    LogRequest();
}

void TServiceContextBase::SetResponseInfo(const Stroka& info)
{
    YASSERT(!Replied_);
    YASSERT(!IsOneWay());

    ResponseInfo_ = info;
}

void TServiceContextBase::AppendInfo(Stroka& lhs, const Stroka& rhs)
{
    if (!rhs.empty()) {
        if (!lhs.empty()) {
            lhs.append(", ");
        }
        lhs.append(rhs);
    }
}

////////////////////////////////////////////////////////////////////////////////

TServiceContextWrapper::TServiceContextWrapper(IServiceContextPtr underlyingContext)
    : UnderlyingContext(std::move(underlyingContext))
{ }

TSharedRefArray TServiceContextWrapper::GetRequestMessage() const
{
    return UnderlyingContext->GetRequestMessage();
}

TRequestId TServiceContextWrapper::GetRequestId() const
{
    return UnderlyingContext->GetRequestId();
}

TNullable<TInstant> TServiceContextWrapper::GetRequestStartTime() const
{
    return UnderlyingContext->GetRequestStartTime();
}

TNullable<TInstant> TServiceContextWrapper::GetRetryStartTime() const
{
    return UnderlyingContext->GetRetryStartTime();
}

i64 TServiceContextWrapper::GetPriority() const
{
    return UnderlyingContext->GetPriority();
}

const Stroka& TServiceContextWrapper::GetService() const
{
    return UnderlyingContext->GetService();
}

const Stroka& TServiceContextWrapper::GetMethod() const
{
    return UnderlyingContext->GetMethod();
}

const TRealmId& TServiceContextWrapper::GetRealmId() const 
{
    return UnderlyingContext->GetRealmId();
}

bool TServiceContextWrapper::IsOneWay() const
{
    return UnderlyingContext->IsOneWay();
}

bool TServiceContextWrapper::IsReplied() const
{
    return UnderlyingContext->IsReplied();
}

void TServiceContextWrapper::Reply(const TError& error)
{
    UnderlyingContext->Reply(error);
}

void TServiceContextWrapper::Reply(TSharedRefArray responseMessage)
{
    UnderlyingContext->Reply(responseMessage);
}

TSharedRefArray TServiceContextWrapper::GetResponseMessage() const
{
    return UnderlyingContext->GetResponseMessage();
}

const TError& TServiceContextWrapper::GetError() const
{
    return UnderlyingContext->GetError();
}

TSharedRef TServiceContextWrapper::GetRequestBody() const
{
    return UnderlyingContext->GetRequestBody();
}

TSharedRef TServiceContextWrapper::GetResponseBody()
{
    return UnderlyingContext->GetResponseBody();
}

void TServiceContextWrapper::SetResponseBody(const TSharedRef& responseBody)
{
    UnderlyingContext->SetResponseBody(responseBody);
}

std::vector<TSharedRef>& TServiceContextWrapper::RequestAttachments()
{
    return UnderlyingContext->RequestAttachments();
}

std::vector<TSharedRef>& TServiceContextWrapper::ResponseAttachments()
{
    return UnderlyingContext->ResponseAttachments();
}

const NProto::TRequestHeader& TServiceContextWrapper::RequestHeader() const 
{
    return UnderlyingContext->RequestHeader();
}

NProto::TRequestHeader& TServiceContextWrapper::RequestHeader()
{
    return UnderlyingContext->RequestHeader();
}

void TServiceContextWrapper::SetRequestInfo(const Stroka& info)
{
    UnderlyingContext->SetRequestInfo(info);
}

void TServiceContextWrapper::SetResponseInfo(const Stroka& info)
{
    UnderlyingContext->SetResponseInfo(info);
}

////////////////////////////////////////////////////////////////////////////////

TReplyInterceptorContext::TReplyInterceptorContext(
    IServiceContextPtr underlyingContext,
    TClosure onReply)
    : TServiceContextWrapper(std::move(underlyingContext))
    , OnReply(std::move(onReply))
{ }

void TReplyInterceptorContext::Reply(const TError& error)
{
    TServiceContextWrapper::Reply(error);
    OnReply.Run();
}

void TReplyInterceptorContext::Reply(TSharedRefArray responseMessage)
{
    TServiceContextWrapper::Reply(std::move(responseMessage));
    OnReply.Run();
}

////////////////////////////////////////////////////////////////////////////////

TServerBase::TServerBase()
    : Started_(false)
{ }

void TServerBase::RegisterService(IServicePtr service)
{
    YCHECK(service);

    auto serviceId = service->GetServiceId();

    {
        TWriterGuard guard(ServicesLock_);
        YCHECK(ServiceMap_.insert(std::make_pair(serviceId, service)).second);
    }

    LOG_INFO("RPC service registered (ServiceName: %s, RealmId: %s)",
        ~serviceId.ServiceName,
        ~ToString(serviceId.RealmId));
}

void TServerBase::UnregisterService(IServicePtr service)
{
    YCHECK(service);

    auto serviceId = service->GetServiceId();

    {
        TWriterGuard guard(ServicesLock_);
        YCHECK(ServiceMap_.erase(serviceId) == 1);
    }

    LOG_INFO("RPC service unregistered (ServiceName: %s, RealmId: %s)",
        ~serviceId.ServiceName,
        ~ToString(serviceId.RealmId));
}

NYT::NRpc::IServicePtr TServerBase::FindService(const TServiceId& serviceId)
{
    TReaderGuard guard(ServicesLock_);
    auto it = ServiceMap_.find(serviceId);
    return it == ServiceMap_.end() ? nullptr : it->second;
}

void TServerBase::Configure(TServerConfigPtr config)
{
    for (const auto& pair : config->Services) {
        const auto& serviceName = pair.first;
        const auto& serviceConfig = pair.second;
        auto services = FindServices(serviceName);
        if (services.empty()) {
            THROW_ERROR_EXCEPTION("Cannot find RPC service %s to configure",
                ~serviceName.Quote());
        }
        for (auto service : services) {
            service->Configure(serviceConfig);
        }
    }
}

void TServerBase::Start()
{
    YCHECK(!Started_);

    DoStart();

    LOG_INFO("RPC server started");
}

void TServerBase::Stop()
{
    if (!Started_)
        return;

    DoStop();

    LOG_INFO("RPC server stopped");
}

void TServerBase::DoStart()
{
    Started_ = true;
}

void TServerBase::DoStop()
{
    Started_ = false;
}

std::vector<IServicePtr> TServerBase::FindServices(const Stroka& serviceName)
{
    std::vector<IServicePtr> result;
    TReaderGuard guard(ServicesLock_);
    for (const auto& pair : ServiceMap_) {
        if (pair.first.ServiceName == serviceName) {
            result.push_back(pair.second);
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
